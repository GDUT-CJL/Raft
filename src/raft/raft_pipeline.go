package raft

import (
	"sync"
	"sync/atomic"
	"time"
)

const (
	maxInflight = 16
)

type pipelineEntry struct {
	index     int
	term      int
	args      *AppendEntriesArgs
	timestamp time.Time
}

type peerPipeline struct {
	peer        int
	rf          *Raft
	inflight    []*pipelineEntry
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
	go pp.pipelineWorker(term)
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

func (pp *peerPipeline) pipelineWorker(term int) {
	for atomic.LoadInt32(&pp.running) == 1 {
		pp.cond.L.Lock()
		for len(pp.inflight) == 0 && atomic.LoadInt32(&pp.running) == 1 {
			pp.cond.Wait()
		}
		if atomic.LoadInt32(&pp.running) == 0 {
			pp.cond.L.Unlock()
			return
		}
		entry := pp.inflight[0]
		pp.cond.L.Unlock()

		pp.sendAndProcess(entry, term)
	}
}

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

type PipelineReplicator struct {
	rf       *Raft
	term     int
	pipelines []*peerPipeline
	stopCh   chan struct{}
	running  int32
}

func NewPipelineReplicator(rf *Raft, term int) *PipelineReplicator {
	pr := &PipelineReplicator{
		rf:       rf,
		term:     term,
		pipelines: make([]*peerPipeline, len(rf.peers)),
		stopCh:   make(chan struct{}),
		running:  0,
	}

	for i := range pr.pipelines {
		if i != rf.me {
			pr.pipelines[i] = newPeerPipeline(i, rf)
		}
	}

	return pr
}

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

func (pr *PipelineReplicator) replicationLoop() {
	ticker := time.NewTicker(replicateInterval)
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

		prevIndex := pr.rf.nextIndex[peer] - 1
		if prevIndex < pr.rf.log.snapLastIdx {
			go pr.rf.InstallToPeer(peer, pr.term, &InstallSnapshotArgs{
				Term:              pr.rf.currentTerm,
				LeaderId:          pr.rf.me,
				LastIncludedIndex: pr.rf.log.snapLastIdx,
				LastIncludedTerm:  pr.rf.log.snapLastTerm,
				Snapshot:          pr.rf.log.snapshot,
			})
			continue
		}

		entries := pr.rf.log.tail(prevIndex + 1)
		if len(entries) == 0 {
			continue
		}

		prevTerm := pr.rf.log.at(prevIndex).Term
		args := &AppendEntriesArgs{
			Term:         pr.term,
			LeaderId:     pr.rf.me,
			PrevLogIndex: prevIndex,
			PrevLogTerm:  prevTerm,
			Entries:      entries,
			LeaderCommit: pr.rf.commitIndex,
			LeaderIP:     pr.rf.LeaderIP,
		}

		entry := &pipelineEntry{
			index:     prevIndex + len(entries),
			term:      pr.term,
			args:      args,
			timestamp: time.Now(),
		}

		pipeline.addInflight(entry)
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
