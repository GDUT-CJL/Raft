# Raft 流水线复制详解

## 目录

1. [概述](#概述)
2. [核心概念](#核心概念)
3. [架构设计](#架构设计)
4. [数据流详解](#数据流详解)
5. [调用链路](#调用链路)
6. [关键代码分析](#关键代码分析)
7. [性能优化](#性能优化)
8. [安全性保证](#安全性保证)

---

## 概述

### 什么是流水线复制？

流水线复制（Pipeline Replication）是一种优化 Raft 日志复制性能的技术。它允许 Leader 在不等待前一个 AppendEntries RPC 响应的情况下，立即发送下一个请求，从而隐藏网络延迟和 Follower 处理时间。

### 为什么需要流水线复制？

**传统串行复制的问题：**

```
Leader                    Follower
  │                          │
  │── AE1 ──────────────────→│
  │                          │ 处理 AE1（持久化 50ms）
  │←── Response1 ───────────│
  │                          │
  │── AE2 ──────────────────→│  ← 必须等待响应才能发送下一个
  │                          │ 处理 AE2（持久化 50ms）
  │←── Response2 ───────────│
  │                          │
  │── AE3 ──────────────────→│
  │                          │ 处理 AE3（持久化 50ms）
  │←── Response3 ───────────│

问题：
├── 发送频率受 Follower 处理时间限制
├── 如果 Follower 处理时间 > 80ms，发送频率 < 12.5/秒
├── 日志积压
└── 最终超时
```

**流水线复制的优势：**

```
Leader                    Follower
  │                          │
  │── AE1 ──────────────────→│
  │── AE2 ──────────────────→│  ← 不等待响应，立即发送下一个
  │── AE3 ──────────────────→│
  │                          │
  │                          │ goroutine 1: 处理 AE1（并行）
  │                          │ goroutine 2: 处理 AE2（并行）
  │                          │ goroutine 3: 处理 AE3（并行）
  │                          │
  │←── Response1 ───────────│
  │←── Response2 ───────────│
  │←── Response3 ───────────│

优势：
├── Leader 发送不受 Follower 处理时间影响
├── 日志积压少
├── 不会超时
└── 吞吐量提升 10-20 倍
```

---

## 核心概念

### 1. PipelineReplicator

流水线复制管理器，负责管理所有 peer 的流水线。

```go
type PipelineReplicator struct {
    rf        *Raft            // Raft 实例
    term      int              // 当前任期
    pipelines []*peerPipeline  // 每个 peer 的流水线
    stopCh    chan struct{}    // 停止信号
    running   int32            // 运行状态
}
```

### 2. peerPipeline

单个 peer 的流水线，管理 inflight 请求队列。

```go
type peerPipeline struct {
    peer     int                // peer ID
    rf       *Raft              // Raft 实例
    inflight []*pipelineEntry   // inflight 请求队列
    stopCh   chan struct{}      // 停止信号
    cond     *sync.Cond         // 条件变量
    running  int32              // 运行状态
}
```

### 3. pipelineEntry

流水线中的一个请求条目。

```go
type pipelineEntry struct {
    index     int                // 日志索引
    term      int                // 任期
    args      *AppendEntriesArgs // RPC 参数
    timestamp time.Time          // 创建时间
}
```

### 4. inflight 请求

已发送但未收到响应的请求，最多 16 个。

```go
const (
    maxInflight = 16  // 每个 peer 最多 16 个 inflight 请求
)
```

---

## 架构设计

### 整体架构

```
┌─────────────────────────────────────────────────────────────────────────┐
│  PipelineReplicator                                                      │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐                     │
│  │ peerPipeline│  │ peerPipeline│  │ peerPipeline│                     │
│  │   (Peer 0)  │  │   (Peer 1)  │  │   (Peer 2)  │                     │
│  └─────────────┘  └─────────────┘  └─────────────┘                     │
│        │                │                │                              │
│        │                │                │                              │
│        ↓                ↓                ↓                              │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐                     │
│  │  inflight   │  │  inflight   │  │  inflight   │                     │
│  │  [AE1,AE2..]│  │  [AE1,AE2..]│  │  [AE1,AE2..]│                     │
│  └─────────────┘  └─────────────┘  └─────────────┘                     │
│                                                                         │
│  replicationLoop (每 80ms)                                              │
│       │                                                                 │
│       └── trySendEntries()                                              │
│              ├── 检查 inflight 数量                                     │
│              ├── 创建 AppendEntriesArgs                                 │
│              └── 添加到 inflight 队列                                   │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

### 数据流架构

```
┌─────────────────────────────────────────────────────────────────────────┐
│  数据流                                                                  │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│  Client (200 并发)                                                      │
│       ↓                                                                 │
│  网络层批量处理（batch_size=100, timeout=10ms）                         │
│       ↓                                                                 │
│  合并为 10-20 个 Op/批                                                  │
│       ↓                                                                 │
│  rf.Start([op1, op2, ..., op20])                                       │
│       ↓                                                                 │
│  Leader 日志追加                                                        │
│       ↓                                                                 │
│  PipelineReplicator                                                     │
│       ↓                                                                 │
│  每 80ms 调用 trySendEntries()                                          │
│       ↓                                                                 │
│  为每个 peer 创建 AppendEntriesArgs                                     │
│       ↓                                                                 │
│  添加到 inflight 队列                                                   │
│       ↓                                                                 │
│  立即启动 goroutine 发送 RPC                                            │
│       ↓                                                                 │
│  Follower 接收并处理（并行）                                            │
│       ↓                                                                 │
│  返回响应                                                                │
│       ↓                                                                 │
│  Leader 更新 matchIndex 和 commitIndex                                  │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## 数据流详解

### 1. 启动流程

```
启动流程：
─────────────────────────────────────────────

1. becomeLeaderLocked()
   ├── 创建 PipelineReplicator
   └── 启动 PipelineReplicator

2. PipelineReplicator.Start()
   ├── 启动所有 peerPipeline
   └── 启动 replicationLoop goroutine

3. replicationLoop()
   ├── 每 80ms 调用 trySendEntries()
   └── 持续运行直到停止
```

**代码位置：**

```go
// raft.go: becomeLeaderLocked()
func (rf *Raft) becomeLeaderLocked() {
    // ... 其他逻辑 ...
    
    if rf.pipelineReplicator != nil {
        rf.pipelineReplicator.Stop()
    }
    rf.pipelineReplicator = NewPipelineReplicator(rf, rf.currentTerm)
    rf.pipelineReplicator.Start()
    LOG(rf.me, rf.currentTerm, DLeader, "Pipeline replicator started for term %d", rf.currentTerm)
}
```

### 2. 发送流程

```
发送流程：
─────────────────────────────────────────────

1. replicationLoop() (每 80ms)
   └── trySendEntries()

2. trySendEntries()
   ├── 获取锁
   ├── 检查是否仍是 Leader
   ├── 遍历所有 peer
   ├── 为每个 peer 检查 inflight 数量
   ├── 创建 AppendEntriesArgs
   └── 调用 pipeline.addInflight()

3. pipeline.addInflight()
   ├── 添加到 inflight 队列
   └── 立即启动 goroutine: go sendAndProcess()

4. sendAndProcess()
   ├── 发送 RPC: sendAppendEntries()
   ├── 等待响应
   ├── 处理响应
   │   ├── 更新 matchIndex
   │   ├── 更新 nextIndex
   │   └── 更新 commitIndex
   └── 从 inflight 队列移除
```

**代码位置：**

```go
// raft_pipeline.go: trySendEntries()
func (pr *PipelineReplicator) trySendEntries() {
    pr.rf.mu.Lock()
    defer pr.rf.mu.Unlock()

    if pr.rf.contextLostLocked(Leader, pr.term) {
        return
    }

    for peer, pipeline := range pr.pipelines {
        if pipeline == nil || !pipeline.canSend() {
            continue
        }

        // ... 创建 AppendEntriesArgs ...

        entry := &pipelineEntry{
            index:     prevIndex + len(entries),
            term:      pr.term,
            args:      args,
            timestamp: time.Now(),
        }

        pipeline.addInflight(entry)  // 添加到 inflight 队列
    }
}

// raft_pipeline.go: addInflight()
func (pp *peerPipeline) addInflight(entry *pipelineEntry) {
    pp.cond.L.Lock()
    defer pp.cond.L.Unlock()
    pp.inflight = append(pp.inflight, entry)
    pp.cond.Signal()

    go pp.sendAndProcess(entry, entry.term)  // 立即启动 goroutine
}
```

### 3. 响应处理流程

```
响应处理流程：
─────────────────────────────────────────────

1. sendAndProcess()
   ├── 发送 RPC
   ├── 等待响应
   └── 处理响应

2. 处理成功响应：
   ├── 获取锁
   ├── 检查任期
   ├── 从 inflight 队列移除
   ├── 更新 matchIndex[peer]
   ├── 更新 nextIndex[peer]
   ├── 计算多数派 matchIndex
   └── 更新 commitIndex（如果满足条件）

3. 处理失败响应：
   ├── 获取锁
   ├── 检查任期
   ├── 从 inflight 队列移除
   ├── 回退 nextIndex[peer]
   └── 等待下次重试
```

**代码位置：**

```go
// raft_pipeline.go: sendAndProcess()
func (pp *peerPipeline) sendAndProcess(entry *pipelineEntry, term int) {
    reply := &AppendEntriesReply{}
    ok := pp.rf.sendAppendEntries(pp.peer, entry.args, reply)

    pp.rf.mu.Lock()
    defer pp.rf.mu.Unlock()

    if pp.rf.contextLostLocked(Leader, term) {
        return
    }

    pp.removeInflight(entry.index)

    if !ok {
        LOG(pp.rf.me, pp.rf.currentTerm, DLog, "-> S%d, Pipeline send failed", pp.peer)
        return
    }

    if reply.Term > pp.rf.currentTerm {
        pp.rf.becomeFollowerLocked(reply.Term)
        return
    }

    if !reply.Success {
        // ... 回退 nextIndex ...
        return
    }

    pp.rf.matchIndex[pp.peer] = entry.args.PrevLogIndex + len(entry.args.Entries)
    pp.rf.nextIndex[pp.peer] = pp.rf.matchIndex[pp.peer] + 1

    majorityMatched := pp.rf.getMajorityIndexLocked()
    if majorityMatched > pp.rf.commitIndex && pp.rf.log.at(majorityMatched).Term == pp.rf.currentTerm {
        LOG(pp.rf.me, pp.rf.currentTerm, DApply, "Pipeline: leader update commit index %d -> %d", pp.rf.commitIndex, majorityMatched)
        pp.rf.commitIndex = majorityMatched
        pp.rf.applyCond.Signal()
    }
}
```

### 4. 停止流程

```
停止流程：
─────────────────────────────────────────────

1. becomeFollowerLocked()
   ├── 检查 pipelineReplicator 是否存在
   ├── 调用 pipelineReplicator.Stop()
   └── 设置为 nil

2. PipelineReplicator.Stop()
   ├── 设置 running = 0
   ├── 关闭 stopCh
   └── 停止所有 peerPipeline

3. peerPipeline.stop()
   ├── 设置 running = 0
   └── 广播条件变量
```

**代码位置：**

```go
// raft.go: becomeFollowerLocked()
func (rf *Raft) becomeFollowerLocked(term int) {
    // ... 其他逻辑 ...

    if rf.pipelineReplicator != nil {
        rf.pipelineReplicator.Stop()
        rf.pipelineReplicator = nil
    }
}

// raft.go: Kill()
func (rf *Raft) Kill() {
    atomic.StoreInt32(&rf.dead, 1)
    if rf.pipelineReplicator != nil {
        rf.pipelineReplicator.Stop()
    }
}
```

---

## 调用链路

### 完整调用链路

```
启动链路：
─────────────────────────────────────────────

main.go
  └── raft.Make()
        └── (Leader 选举)
              └── becomeLeaderLocked() [raft.go:294]
                    ├── NewPipelineReplicator() [raft_pipeline.go:147]
                    └── PipelineReplicator.Start() [raft_pipeline.go:165]
                          ├── peerPipeline.start() [raft_pipeline.go:44]
                          └── replicationLoop() [raft_pipeline.go:189]

发送链路：
─────────────────────────────────────────────

replicationLoop() [raft_pipeline.go:189]
  └── trySendEntries() [raft_pipeline.go:206]
        ├── canSend() [raft_pipeline.go:51]
        ├── 创建 AppendEntriesArgs
        └── addInflight() [raft_pipeline.go:59]
              ├── 添加到 inflight 队列
              └── go sendAndProcess() [raft_pipeline.go:84]

响应处理链路：
─────────────────────────────────────────────

sendAndProcess() [raft_pipeline.go:84]
  ├── sendAppendEntries() [raft_replication.go:186]
  ├── removeInflight() [raft_pipeline.go:67]
  ├── 更新 matchIndex
  ├── 更新 nextIndex
  ├── getMajorityIndexLocked() [raft_replication.go:236]
  └── 更新 commitIndex

停止链路：
─────────────────────────────────────────────

becomeFollowerLocked() [raft.go:243]
  └── PipelineReplicator.Stop() [raft_pipeline.go:179]
        └── peerPipeline.stop() [raft_pipeline.go:48]

Kill() [raft.go:363]
  └── PipelineReplicator.Stop() [raft_pipeline.go:179]
```

### 关键调用点

| 调用点 | 文件 | 行号 | 说明 |
|--------|------|------|------|
| becomeLeaderLocked | raft.go | 294 | Leader 启动流水线 |
| becomeFollowerLocked | raft.go | 262 | Follower 停止流水线 |
| Kill | raft.go | 363 | 节点停止流水线 |
| replicationLoop | raft_pipeline.go | 189 | 定时发送日志 |
| trySendEntries | raft_pipeline.go | 206 | 尝试发送日志 |
| addInflight | raft_pipeline.go | 59 | 添加到 inflight 队列 |
| sendAndProcess | raft_pipeline.go | 84 | 发送并处理响应 |
| removeInflight | raft_pipeline.go | 67 | 从 inflight 队列移除 |

---

## 关键代码分析

### 1. inflight 队列管理

```go
// 添加到 inflight 队列
func (pp *peerPipeline) addInflight(entry *pipelineEntry) {
    pp.cond.L.Lock()
    defer pp.cond.L.Unlock()
    pp.inflight = append(pp.inflight, entry)
    pp.cond.Signal()

    go pp.sendAndProcess(entry, entry.term)  // 立即启动 goroutine
}

// 从 inflight 队列移除
func (pp *peerPipeline) removeInflight(index int) {
    pp.cond.L.Lock()
    defer pp.cond.L.Unlock()
    for i, entry := range pp.inflight {
        if entry.index == index {
            pp.inflight = append(pp.inflight[:i], pp.inflight[i+1:]...)
            pp.cond.Signal()
            return
        }
    }
}

// 检查是否可以发送
func (pp *peerPipeline) canSend() bool {
    pp.cond.L.Lock()
    defer pp.cond.L.Unlock()
    return len(pp.inflight) < maxInflight
}
```

**关键点：**

- `addInflight()` 会立即启动 goroutine 处理请求，不等待前一个请求完成
- `removeInflight()` 在收到响应后移除对应的条目
- `canSend()` 检查 inflight 数量是否达到上限（16 个）

### 2. 并发控制

```go
// 发送并处理响应
func (pp *peerPipeline) sendAndProcess(entry *pipelineEntry, term int) {
    reply := &AppendEntriesReply{}
    ok := pp.rf.sendAppendEntries(pp.peer, entry.args, reply)

    pp.rf.mu.Lock()
    defer pp.rf.mu.Unlock()

    // ... 处理响应 ...
}
```

**关键点：**

- 每个请求在独立的 goroutine 中处理
- 发送 RPC 不需要锁（网络传输）
- 处理响应需要获取 Raft 锁
- 多个 goroutine 可以并行发送请求

### 3. commitIndex 更新

```go
// 更新 commitIndex
majorityMatched := pp.rf.getMajorityIndexLocked()
if majorityMatched > pp.rf.commitIndex && pp.rf.log.at(majorityMatched).Term == pp.rf.currentTerm {
    LOG(pp.rf.me, pp.rf.currentTerm, DApply, "Pipeline: leader update commit index %d -> %d", pp.rf.commitIndex, majorityMatched)
    pp.rf.commitIndex = majorityMatched
    pp.rf.applyCond.Signal()
}
```

**关键点：**

- 只有当前任期的日志才能提交
- 需要多数派确认
- 提交后唤醒状态机应用

---

## 性能优化

### 1. 并行发送

**之前（串行）：**

```
吞吐量 = 1000ms / 50ms = 20 请求/秒
```

**现在（并行）：**

```
吞吐量 = 1000ms / 50ms × 16 = 320 请求/秒
```

**性能提升：16 倍**

### 2. 隐藏延迟

```
┌─────────────────────────────────────────────────────────────────────────┐
│  延迟隐藏                                                                │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│  网络延迟：1-10ms                                                       │
│  Follower 处理：10-50ms                                                │
│                                                                         │
│  串行发送：                                                              │
│  ├── 发送 AE1 → 等待 50ms → 发送 AE2 → 等待 50ms → ...                │
│  └── 总延迟：50ms × N                                                  │
│                                                                         │
│  流水线发送：                                                            │
│  ├── 发送 AE1 → 发送 AE2 → 发送 AE3 → ...                             │
│  └── 总延迟：max(50ms) ≈ 50ms                                          │
│                                                                         │
│  效果：延迟不累积                                                        │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

### 3. 资源利用

```
┌─────────────────────────────────────────────────────────────────────────┐
│  资源利用                                                                │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│  CPU：                                                                   │
│  ├── 串行：单核利用率低                                                 │
│  └── 流水线：多核并行处理，利用率高                                     │
│                                                                         │
│  网络：                                                                  │
│  ├── 串行：网络空闲时间多                                               │
│  └── 流水线：网络持续传输，利用率高                                     │
│                                                                         │
│  内存：                                                                  │
│  ├── inflight 队列：最多 16 个请求                                     │
│  └── 内存占用：可控                                                     │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## 安全性保证

### 1. Raft 安全性

```
┌─────────────────────────────────────────────────────────────────────────┐
│  Raft 安全性保证                                                         │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│  1. 日志持久化                                                          │
│     ─────────────────────────────                                       │
│     要求：日志必须持久化才能被认为已提交                                │
│     流水线复制：✅ 满足（Follower 必须持久化成功才返回响应）            │
│                                                                         │
│  2. 多数派确认                                                          │
│     ─────────────────────────────                                       │
│     要求：日志必须被多数派确认才能提交                                  │
│     流水线复制：✅ 满足（Leader 需要收到多数派响应才更新 commitIndex）  │
│                                                                         │
│  3. 崩溃恢复                                                            │
│     ─────────────────────────────                                       │
│     要求：崩溃后重启，已提交的日志不能丢失                              │
│     流水线复制：✅ 满足（已提交的日志已持久化）                         │
│                                                                         │
│  4. 一致性                                                              │
│     ─────────────────────────────                                       │
│     要求：所有节点的状态机必须一致                                      │
│     流水线复制：✅ 满足（日志持久化后才应用）                           │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

### 2. 与异步持久化的区别

```
┌─────────────────────────────────────────────────────────────────────────┐
│  流水线复制 vs 异步持久化                                                │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│  异步持久化（不安全）：                                                  │
│  ─────────────────────────────                                          │
│  Follower 接收日志 → 写入内存 → 立即返回 Success                       │
│  问题：崩溃会丢失日志，违反 Raft 安全性                                 │
│                                                                         │
│  流水线复制（安全）：                                                    │
│  ─────────────────────────────                                          │
│  Follower 接收日志 → 持久化 → 返回 Success                             │
│  安全：持久化成功才返回响应，符合 Raft 安全性                           │
│                                                                         │
│  关键区别：                                                              │
│  ├── 流水线复制：Leader 不等待响应就发送下一个                          │
│  │   但 Follower 必须持久化成功才返回响应                               │
│  └── 异步持久化：Follower 不等待持久化就返回响应                        │
│      违反 Raft 安全性                                                   │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

### 3. 错误处理

```
错误处理：
─────────────────────────────────────────────

1. RPC 超时：
   ├── sendAppendEntries() 返回 false
   ├── 从 inflight 队列移除
   └── 等待下次重试

2. 任期变更：
   ├── 检查 reply.Term > rf.currentTerm
   ├── 转换为 Follower
   └── 停止流水线

3. 日志不匹配：
   ├── 回退 nextIndex[peer]
   └── 等待下次重试

4. inflight 队列满：
   ├── canSend() 返回 false
   └── 等待有请求完成
```

---

## 总结

### 核心优势

1. **性能提升**：吞吐量提升 10-20 倍
2. **延迟隐藏**：网络延迟和 Follower 处理时间被隐藏
3. **资源利用**：CPU 和网络利用率提高
4. **安全性保证**：符合 Raft 安全性要求

### 关键技术

1. **inflight 队列**：管理已发送但未收到响应的请求
2. **并行处理**：每个请求在独立的 goroutine 中处理
3. **并发控制**：最多 16 个 inflight 请求
4. **错误处理**：完善的错误处理和重试机制

### 适用场景

- 高并发写入场景
- 网络延迟较高的场景
- Follower 处理时间较长的场景
- 需要高吞吐量的场景

---

## 参考资料

- [Raft 论文](https://raft.github.io/raft.pdf)
- [etcd Raft 实现](https://github.com/etcd-io/etcd/tree/main/raft)
- [TiKV Raft 实现](https://github.com/tikv/tikv/tree/master/components/raftstore)
