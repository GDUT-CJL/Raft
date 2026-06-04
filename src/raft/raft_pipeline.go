package raft

import (
	"sync"
	"sync/atomic"
	"time"
)

const (
	maxInflight            = 4
	maxEntriesPerAppendRPC = 64
)

// 表示“一次发给follower的复制任务”
type pipelineEntry struct {
	index     int
	term      int
	args      *AppendEntriesArgs
	timestamp time.Time
}

// Leader针对某一个人follower的复制窗口控制器
type peerPipeline struct {
	peer        int
	rf          *Raft
	inflight    []*pipelineEntry // 里面存的是“已经发出去但还没处理完成”的请求，长度不超过maxInflight
	nextSendIdx int
	stopCh      chan struct{}
	cond        *sync.Cond
	running     int32
}

func newPeerPipeline(peer int, rf *Raft) *peerPipeline {
	pp := &peerPipeline{
		peer:     peer,
		rf:       rf,
		inflight: make([]*pipelineEntry, 0, maxInflight),
		stopCh:   make(chan struct{}),
		cond:     sync.NewCond(&sync.Mutex{}),
		running:  0,
	}
	return pp
}

func (pp *peerPipeline) start(term int) {
	if !atomic.CompareAndSwapInt32(&pp.running, 0, 1) {
		return
	}
}

func (pp *peerPipeline) stop() {
	atomic.StoreInt32(&pp.running, 0)
	pp.cond.Broadcast()
}

func (pp *peerPipeline) canSend() bool {
	pp.cond.L.Lock()
	defer pp.cond.L.Unlock()
	return len(pp.inflight) < maxInflight
}

func (pp *peerPipeline) addInflight(entry *pipelineEntry) {
	pp.cond.L.Lock()
	defer pp.cond.L.Unlock()
	pp.inflight = append(pp.inflight, entry)
	pp.cond.Signal()

	go pp.sendAndProcess(entry, entry.term)
}

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

func (pp *peerPipeline) getInflightCount() int {
	pp.cond.L.Lock()
	defer pp.cond.L.Unlock()
	return len(pp.inflight)
}

// 发送AppendEntriesRPC给follower
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
		raftstate := pp.rf.becomeFollowerLocked(reply.Term)
		if raftstate {
			pp.rf.mu.Unlock()
			pp.rf.persistMeta(pp.rf.currentTerm, pp.rf.votedFor)
			pp.rf.mu.Lock()
		}
		return
	}

	if !reply.Success {
		prevIndex := pp.rf.nextIndex[pp.peer]
		if reply.ConfilictTerm == InvalidTerm {
			pp.rf.nextIndex[pp.peer] = reply.ConfilictIndex
		} else {
			firstTermIndex := pp.rf.log.firstFor(reply.ConfilictTerm)
			if firstTermIndex != InvalidTerm {
				pp.rf.nextIndex[pp.peer] = firstTermIndex + 1
			} else {
				pp.rf.nextIndex[pp.peer] = reply.ConfilictIndex
			}
		}
		if pp.rf.nextIndex[pp.peer] > prevIndex {
			pp.rf.nextIndex[pp.peer] = prevIndex
		}
		LOG(pp.rf.me, pp.rf.currentTerm, DLog, "-> S%d, Pipeline mismatch, retry from %d", pp.peer, pp.rf.nextIndex[pp.peer])
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

// Leader针对所有follower的复制窗口控制器
type PipelineReplicator struct {
	rf        *Raft
	term      int
	pipelines []*peerPipeline
	stopCh    chan struct{}
	running   int32
}

// 初始化Leader针对所有follower的复制窗口控制器
func NewPipelineReplicator(rf *Raft, term int) *PipelineReplicator {
	pr := &PipelineReplicator{
		rf:        rf,
		term:      term,
		pipelines: make([]*peerPipeline, len(rf.peers)),
		stopCh:    make(chan struct{}),
		running:   0,
	}

	for i := range pr.pipelines {
		if i != rf.me {
			pr.pipelines[i] = newPeerPipeline(i, rf)
		}
	}

	return pr
}

// 启动Leader针对所有follower的复制窗口控制器
func (pr *PipelineReplicator) Start() {
	if !atomic.CompareAndSwapInt32(&pr.running, 0, 1) {
		return
	}

	for _, pipeline := range pr.pipelines {
		if pipeline != nil {
			pipeline.start(pr.term)
		}
	}

	go pr.replicationLoop()
}

func (pr *PipelineReplicator) Stop() {
	atomic.StoreInt32(&pr.running, 0)
	close(pr.stopCh)
	for _, pipeline := range pr.pipelines {
		if pipeline != nil {
			pipeline.stop()
		}
	}
}

// Leader针对所有follower的复制窗口控制器的复制循环
// 每个循环会尝试发送一个AppendEntriesRPC给所有follower
// 如果follower没有处理完成，会等待下一次循环
func (pr *PipelineReplicator) replicationLoop() {
	ticker := time.NewTicker(replicateInterval) // 80ms
	defer ticker.Stop()

	for {
		select {
		case <-pr.stopCh:
			return
		case <-ticker.C:
			if atomic.LoadInt32(&pr.running) == 0 {
				return
			}
			pr.trySendEntries()
		}
	}
}

/*
1. 检查 leader 身份是否还有效
2. 遍历所有 follower pipeline
3. 对“还能发”的 follower 生成一个 `AppendEntries` 任务
4. 锁外真正把任务塞进该 peer 的 inflight 队列
*/
func (pr *PipelineReplicator) trySendEntries() {
	pr.rf.mu.Lock()

	if pr.rf.contextLostLocked(Leader, pr.term) {
		pr.rf.mu.Unlock()
		return
	}

	type sendTask struct {
		peer     int
		args     *AppendEntriesArgs
		pipeline *peerPipeline
	}

	tasks := make([]sendTask, 0, len(pr.pipelines))

	for peer, pipeline := range pr.pipelines {
		// 判断这个 peer 还能不能发AppendEntriesRPC
		if pipeline == nil || !pipeline.canSend() {
			continue
		}

		prevIndex := pr.rf.nextIndex[peer] - 1
		// 决定发快照还是发日志
		if prevIndex < pr.rf.log.snapLastIdx {
			// 如果已经在发送快照，跳过，避免重复发送
			if pr.rf.snapSending[peer] {
				continue
			}
			pr.rf.snapSending[peer] = true
			// 必须在锁内拷贝快照数据，避免释放锁后 goroutine 访问被修改的数据
			snapshotCopy := make([]byte, len(pr.rf.log.snapshot))
			copy(snapshotCopy, pr.rf.log.snapshot)
			go pr.rf.InstallToPeer(peer, pr.term, &InstallSnapshotArgs{
				Term:              pr.rf.currentTerm,
				LeaderId:          pr.rf.me,
				LastIncludedIndex: pr.rf.log.snapLastIdx,
				LastIncludedTerm:  pr.rf.log.snapLastTerm,
				Snapshot:          snapshotCopy,
			})
			continue
		}

		entries := pr.rf.log.tail(prevIndex + 1)
		if len(entries) == 0 {
			continue
		}
		// 一次最多发送64条日志条
		if len(entries) > maxEntriesPerAppendRPC {
			entries = entries[:maxEntriesPerAppendRPC]
		}
		entriesCopy := make([]LogEntry, len(entries))
		copy(entriesCopy, entries)

		prevTerm := pr.rf.log.at(prevIndex).Term
		args := &AppendEntriesArgs{
			Term:         pr.term,
			LeaderId:     pr.rf.me,
			PrevLogIndex: prevIndex,
			PrevLogTerm:  prevTerm,
			Entries:      entriesCopy,
			LeaderCommit: pr.rf.commitIndex,
			LeaderIP:     pr.rf.LeaderIP,
		}

		tasks = append(tasks, sendTask{
			peer:     peer,
			args:     args,
			pipeline: pipeline,
		})
	}

	pr.rf.mu.Unlock()

	for _, task := range tasks {
		entry := &pipelineEntry{
			index:     task.args.PrevLogIndex + len(task.args.Entries),
			term:      pr.term,
			args:      task.args,
			timestamp: time.Now(),
		}
		// 把任务放到该follower的inflight队列
		task.pipeline.addInflight(entry)
	}
}

func (pr *PipelineReplicator) GetStats() map[int]int {
	stats := make(map[int]int)
	for i, pipeline := range pr.pipelines {
		if pipeline != nil {
			stats[i] = pipeline.getInflightCount()
		}
	}
	return stats
}
