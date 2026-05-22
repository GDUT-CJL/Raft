# LeaseRead + ReadIndex 算法详解

## 一、背景：为什么需要 LeaseRead？

### 1.1 Raft 的强一致性与读性能矛盾

Raft 是一个**强一致性**共识算法，所有写操作都必须经过日志复制、多数派提交后才能生效。对于读操作，如果也走同样的流程（即 "Read as Write"），性能会非常差：

- **写操作路径**：客户端 → Leader → 日志复制 → 多数派确认 → 提交 → 返回
- **传统读操作路径**：客户端 → Leader → 发起 ReadIndex 请求 → 等待心跳确认自己是 Leader → 等待日志应用到该 Index → 读状态机 → 返回

每次读操作都需要至少 **2 次 RTT**（Round-Trip Time），在高并发场景下成为性能瓶颈。

### 1.2 LeaseRead 的核心思想

**LeaseRead（租约读）** 的核心思想是：

> Leader 通过心跳机制维护一个"租约期"，在租约期内，Leader 可以确信自己仍然是合法的 Leader（没有发生网络分区导致其他节点选举出新 Leader）。因此，Leader 可以直接读取本地状态机，**无需再次确认自己的 Leader 身份**。

这样，读操作从 **2 次 RTT** 优化为 **0 次 RTT**（纯本地读），大幅提升读性能。

---

## 二、核心数据结构

### 2.1 LeaseState（租约状态）

```go
// src/raft/raft.go
type LeaseState struct {
    IsLeader        bool          // 是否是 Leader
    LeaseValid      bool          // 租约是否有效
    ReadIndex       int           // 当前可安全读取的日志索引
    LeaseExpiration time.Time     // 租约过期时间
}
```

### 2.2 Raft 节点中的 Lease 相关字段

```go
// src/raft/raft.go
type Raft struct {
    // ... 其他字段 ...
    
    lastHeartbeatTime time.Time     // 上次收到心跳的时间
    leaseExpiration   time.Time     // 租约过期时间（本地计算）
    leaseReadIndex    int           // 租约对应的可读索引
    leaseDuration     time.Duration // 租约时长（默认 400ms）
    enableLeaseRead   bool          // 是否启用 LeaseRead
    
    leaseState atomic.Value         // 原子存储的 LeaseState（线程安全）
}
```

### 2.3 关键设计：为什么用 atomic.Value？

`leaseState` 使用 `atomic.Value` 存储，而不是直接读写 `Raft` 结构体的字段，原因是：

1. **读操作不需要加锁**：`IsLeaseValid()` 和 `GetLeaseReadIndex()` 是纯读操作，使用 `atomic.Value` 可以避免锁竞争
2. **保证可见性**：`atomic.Value.Store()` 保证写入的 `LeaseState` 对所有 goroutine 立即可见
3. **不可变性**：每次更新都是创建新的 `LeaseState` 对象，避免并发读写冲突

---

## 三、LeaseRead 完整流程

### 3.1 时序图

```
┌─────────┐     ┌─────────┐     ┌─────────┐     ┌─────────┐
│ Client  │     │  Leader │     │ Follower│     │  Timer  │
└────┬────┘     └────┬────┘     └────┬────┘     └────┬────┘
     │               │               │               │
     │  1. GET key   │               │               │
     │──────────────>│               │               │
     │               │               │               │
     │               │ 2. checkLeaseRead()           │
     │               │  - IsLeader? ✓                │
     │               │  - IsLeaseValid? ✓            │
     │               │  - GetLeaseReadIndex = 100    │
     │               │               │               │
     │               │ 3. WaitForApplied(100)        │
     │               │  - lastApplied >= 100? ✓      │
     │               │               │               │
     │               │ 4. 直接读本地状态机            │
     │               │  - bridge.Array_Get(key)      │
     │               │               │               │
     │  5. 返回 value│               │               │
     │<──────────────│               │               │
     │               │               │               │
     │               │               │               │
     │               │ 6. 心跳/日志复制（后台）       │
     │               │──────────────>│               │
     │               │               │               │
     │               │ 7. 收到成功回复                │
     │               │<──────────────│               │
     │               │               │               │
     │               │ 8. updateLease()              │
     │               │  - leaseExpiration = now + 400ms
     │               │  - leaseReadIndex = commitIndex
     │               │               │               │
     │               │               │               │
```

### 3.2 详细流程

#### 阶段 1：Leader 选举成功，初始化 Lease

```go
// src/raft/raft.go:becomeLeaderLocked()
func (rf *Raft) becomeLeaderLocked() {
    rf.role = Leader
    // ... 初始化 nextIndex, matchIndex ...
    
    rf.updateLease()  // <-- 成为 Leader 时立即更新 Lease
    go rf.heartbeatTicker(rf.currentTerm)
    // ...
}
```

#### 阶段 2：心跳成功，更新 Lease

```go
// src/raft/raft_heartbeat.go:sendHeartbeat()
func (rf *Raft) sendHeartbeat(server int, term int) {
    // ... 发送 AppendEntries RPC ...
    
    reply := &AppendEntriesReply{}
    ok := rf.sendAppendEntries(server, args, reply)
    if !ok {
        return
    }

    rf.mu.Lock()
    if reply.Term > rf.currentTerm {
        rf.becomeFollowerLocked(reply.Term)
        rf.mu.Unlock()
        return
    }
    if reply.Success {
        rf.updateLease()  // <-- 心跳成功，更新 Lease
    }
    rf.mu.Unlock()
}
```

#### 阶段 3：updateLease() 更新租约

```go
// src/raft/raft.go:updateLease()
func (rf *Raft) updateLease() {
    now := time.Now()
    rf.lastHeartbeatTime = now
    rf.leaseExpiration = now.Add(rf.leaseDuration)  // 400ms 后过期
    rf.leaseReadIndex = rf.commitIndex               // 当前已提交的日志索引

    rf.leaseState.Store(&LeaseState{
        IsLeader:        true,
        LeaseValid:      true,
        ReadIndex:       rf.commitIndex,
        LeaseExpiration: rf.leaseExpiration,
    })
}
```

**关键点**：
- `leaseDuration = 400ms`：租约时长，必须小于选举超时时间（500ms-1500ms），确保在租约过期前不会选举出新 Leader
- `leaseReadIndex = commitIndex`：记录当前已提交的日志索引，读操作必须等待状态机应用到这个索引

#### 阶段 4：客户端读请求，执行 LeaseRead

```go
// src/mnet/protocol.go:checkLeaseRead()
func checkLeaseRead(rf *raft.Raft, safeWrite func([]byte)) bool {
    // Step 1: 检查是否是 Leader
    if !rf.IsLeader() {
        sendRESPResponse(safeWrite, "error", "ERR not leader")
        return false
    }

    // Step 2: 检查租约是否有效
    if !rf.IsLeaseValid() {
        sendRESPResponse(safeWrite, "error", "ERR lease expired")
        return false
    }

    // Step 3: 获取 ReadIndex
    readIndex := rf.GetLeaseReadIndex()
    
    // Step 4: 等待状态机应用到 ReadIndex
    if !rf.WaitForApplied(readIndex) {
        sendRESPResponse(safeWrite, "error", "ERR wait for apply timeout")
        return false
    }

    return true  // 可以安全读取本地状态机
}
```

#### 阶段 5：IsLeaseValid() 检查租约

```go
// src/raft/raft.go:IsLeaseValid()
func (rf *Raft) IsLeaseValid() bool {
    state := rf.getLeaseState()
    if state == nil {
        return false
    }
    return state.IsLeader && time.Now().Before(state.LeaseExpiration)
}
```

**关键点**：
- 使用 `atomic.Value.Load()` 读取，无需加锁
- 检查两个条件：是 Leader + 租约未过期

#### 阶段 6：WaitForApplied() 等待日志应用

```go
// src/raft/raft.go:WaitForApplied()
func (rf *Raft) WaitForApplied(index int) bool {
    timeout := time.After(100 * time.Millisecond)

    for {
        rf.mu.Lock()
        applied := rf.lastApplied
        rf.mu.Unlock()

        if applied >= index {
            return true  // 已应用到目标索引
        }

        select {
        case <-time.After(1 * time.Millisecond):
            continue  // 继续轮询
        case <-timeout:
            return false  // 超时
        }
    }
}
```

**关键点**：
- 轮询检查 `lastApplied >= readIndex`
- 超时时间 100ms，防止无限等待
- 每次检查释放锁，避免阻塞其他操作

---

## 四、ReadIndex 机制详解

### 4.1 什么是 ReadIndex？

ReadIndex 是 etcd 提出的线性读方案，核心思想：

> 读操作也需要像写操作一样，等待当前时刻之前所有已提交的日志都应用到状态机后，才能读取。这样可以保证读到的数据包含所有之前已确认的写操作。

### 4.2 ReadIndex 的执行流程

```
┌─────────┐     ┌─────────┐     ┌─────────┐
│ Client  │     │  Leader │     │ Follower│
└────┬────┘     └────┬────┘     └────┬────┘
     │               │               │
     │  1. GET key   │               │
     │──────────────>│               │
     │               │               │
     │               │ 2. 记录当前 commitIndex = N
     │               │               │
     │               │ 3. 发送心跳/空日志给多数派
     │               │──────────────>│
     │               │               │
     │               │ 4. 收到多数派确认
     │               │<──────────────│
     │               │               │
     │               │ 5. 确认自己是合法 Leader
     │               │               │
     │               │ 6. 等待 lastApplied >= N
     │               │               │
     │               │ 7. 读本地状态机
     │               │               │
     │  8. 返回 value│               │
     │<──────────────│               │
```

### 4.3 你的项目中 LeaseRead 与 ReadIndex 的结合

你的项目将 LeaseRead 和 ReadIndex 结合使用：

| 机制 | 作用 | 在代码中的体现 |
|------|------|--------------|
| **LeaseRead** | 避免每次读都确认 Leader 身份 | `IsLeaseValid()` 检查租约 |
| **ReadIndex** | 保证线性一致性，读到最新数据 | `GetLeaseReadIndex()` + `WaitForApplied()` |

**结合后的流程**：

1. **LeaseRead 阶段**：检查租约是否有效（快速，无网络开销）
2. **ReadIndex 阶段**：获取租约记录的可读索引，等待状态机应用（本地等待，无网络开销）
3. **本地读**：直接读取状态机（快速）

---

## 五、关键代码路径

### 5.1 写操作路径（对比）

```
Client → Protocol.ParseRESP → Commited/CommitedBatch → BatchManager.Submit
→ BatchManager.batchWorker → Raft.Start → 日志复制 → 多数派提交
→ applyTask → BatchApply → 存储引擎 → 通知客户端
```

### 5.2 读操作路径（LeaseRead）

```
Client → Protocol.ParseRESP → checkLeaseRead
→ [IsLeader?] → [IsLeaseValid?] → [GetLeaseReadIndex]
→ [WaitForApplied] → bridge.Array_Get/Hash_Get/...
→ 直接返回结果
```

**关键区别**：读操作不经过 Raft 日志复制，直接读本地状态机。

---

## 六、安全性分析

### 6.1 为什么 LeaseRead 是安全的？

LeaseRead 的安全性基于以下假设：

1. **时钟偏移有界**：所有节点的时钟偏移不超过 `leaseDuration`
2. **网络分区检测**：如果 Leader 与多数派断开连接，心跳失败，租约会过期
3. **选举超时 > 租约时长**：`electionTimeout (500ms-1500ms) > leaseDuration (400ms)`

**安全场景**：

| 场景 | 行为 | 结果 |
|------|------|------|
| Leader 正常，网络正常 | 心跳成功，Lease 持续更新 | 读操作成功 |
| Leader 与少数派断开 | 心跳仍与多数派成功 | Lease 更新，读操作成功 |
| Leader 与多数派断开 | 心跳失败，Lease 过期 | 读操作失败，返回错误 |
| 网络分区，新 Leader 选出 | 旧 Leader Lease 过期 | 旧 Leader 拒绝读，新 Leader 接管 |

### 6.2 时钟偏移问题

如果 Leader 的时钟比 Follower 快，可能导致：

- Leader 认为租约仍然有效
- Follower 已经超时开始选举
- 可能出现双 Leader，导致脏读

**解决方案**：
- 使用单调时钟（Monotonic Clock）而非 wall clock
- 设置合理的租约时长（远小于选举超时）
- 你的代码中使用 `time.Now()`，在生产环境中建议使用 `time.Since()` 配合单调时钟

### 6.3 与 Raft 论文的对比

| 方案 | 来源 | 读延迟 | 实现复杂度 | 适用场景 |
|------|------|--------|-----------|---------|
| **ReadIndex** | etcd | 1 RTT | 中 | 通用场景 |
| **LeaseRead** | etcd | 0 RTT | 高 | 时钟同步好的环境 |
| **FollowerRead** | TiKV | 0 RTT | 高 | 读多写少，就近读取 |
| **Read as Write** | Raft 论文 | 2 RTT | 低 | 教学/简单场景 |

你的项目实现了 **LeaseRead + ReadIndex 组合**，是生产级分布式系统（如 etcd、TiKV）采用的方案。

---

## 七、性能优化效果

### 7.1 延迟对比

| 读方案 | 网络 RTT | 本地计算 | 总延迟 |
|--------|---------|---------|--------|
| Read as Write | 2 RTT | 低 | ~20ms (假设 RTT=10ms) |
| ReadIndex | 1 RTT | 低 | ~10ms |
| **LeaseRead** | **0 RTT** | **低** | **~0.1ms** |

### 7.2 吞吐量对比

假设：
- 网络 RTT = 10ms
- 单节点 QPS 上限 = 1000（IO 限制）

| 读方案 | 理论 QPS | 实际 QPS（瓶颈） |
|--------|---------|----------------|
| Read as Write | 50 | 网络 RTT |
| ReadIndex | 100 | 网络 RTT |
| **LeaseRead** | **10000+** | **本地 IO** |

### 7.3 你的项目中的实际效果

在你的项目中，LeaseRead 使得：

- **GET/COUNT/EXIST** 等读操作直接走本地状态机
- **SET/DELETE** 等写操作仍走 Raft 日志复制
- 读写分离，大幅提升读性能

---

## 八、代码中的关键细节

### 8.1 Lease 过期后的回退

```go
// src/raft/raft.go:becomeFollowerLocked()
func (rf *Raft) becomeFollowerLocked(term int) {
    rf.role = Follower
    // ...
    rf.resetLease()  // 变为 Follower 时重置 Lease
    // ...
}

func (rf *Raft) resetLease() {
    rf.leaseExpiration = time.Time{}
    rf.leaseState.Store(&LeaseState{
        IsLeader:        false,
        LeaseValid:      false,
        ReadIndex:       0,
        LeaseExpiration: time.Time{},
    })
}
```

### 8.2 初始化时的 Lease 设置

```go
// src/raft/raft.go:Make()
func Make(peerAddrs []string, me int, applyCh chan ApplyMsg) *Raft {
    rf := &Raft{}
    // ...
    rf.leaseDuration = 400 * time.Millisecond
    rf.lastHeartbeatTime = time.Now()
    rf.leaseExpiration = rf.lastHeartbeatTime.Add(rf.leaseDuration)
    rf.enableLeaseRead = true
    // ...
}
```

### 8.3 读操作的具体实现

```go
// src/mnet/protocol.go
switch action {
case "GET":
    if ok := checkLeaseRead(rf, safeWrite); ok {
        value := bridge.Array_Get(parts[1], len(parts[1]))
        sendRESPResponse(safeWrite, "bulk", value)
    }

case "COUNT":
    if ok := checkLeaseRead(rf, safeWrite); ok {
        value := bridge.Array_Count()
        sendRESPResponse(safeWrite, "integer", strconv.Itoa(value))
    }

// HGET, RGET, BGET, ZGET, RCGET 等类似
}
```

---

## 九、总结

### 9.1 核心思想

LeaseRead + ReadIndex 的核心思想是：

> **用时间换空间**：Leader 通过定期心跳维护一个"时间租约"，在租约期内可以直接读取本地状态机，无需网络确认。

### 9.2 你的项目的实现亮点

1. **atomic.Value 存储 LeaseState**：无锁读取，高性能
2. **Lease 与心跳绑定**：心跳成功即更新 Lease，自然续约
3. **ReadIndex 保证线性一致性**：等待日志应用到指定索引后才读
4. **自动回退**：Lease 过期后自动拒绝读请求，保证安全

### 9.3 面试要点

如果被问到 LeaseRead，可以强调以下几点：

1. **为什么不用 Read as Write？** 性能差，2 次 RTT
2. **LeaseRead 的前提条件？** 时钟同步、租约时长 < 选举超时
3. **如果时钟不同步会怎样？** 可能双 Leader，需要 Monotonic Clock
4. **和 etcd/TiKV 的 LeaseRead 有什么区别？** 你的实现更简化，核心思想一致
5. **如果 WaitForApplied 超时怎么办？** 返回错误，客户端重试或转发到新 Leader

---

*文档生成时间：2026-05-18*
