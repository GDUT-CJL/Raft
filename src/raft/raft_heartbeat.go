package raft

import (
	"sync"
	"time"
)

type heartbeatSnapshot struct {
	term        int
	leaderId    int
	leaderIP    string
	commitIndex int
	peers       []peerHeartbeatData
}

type peerHeartbeatData struct {
	peer         int
	prevLogIndex int
	prevLogTerm  int
	snapLastIdx  int
	snapLastTerm int
	snapshot     []byte
	isSnapshot   bool
}

func (rf *Raft) sendHeartbeat(server int, term int) {
	if rf.me == server {
		return
	}

	rf.mu.Lock()
	if rf.contextLostLocked(Leader, term) {
		rf.mu.Unlock()
		return
	}
	prevIndex := rf.nextIndex[server] - 1
	if prevIndex < rf.log.snapLastIdx {
		args := &InstallSnapshotArgs{
			Term:              rf.currentTerm,
			LeaderId:          rf.me,
			LastIncludedIndex: rf.log.snapLastIdx,
			LastIncludedTerm:  rf.log.snapLastTerm,
			Snapshot:          rf.log.snapshot,
		}
		rf.mu.Unlock()
		go rf.InstallToPeer(server, term, args)
		return
	}
	prevTerm := rf.log.at(prevIndex).Term
	args := &AppendEntriesArgs{
		Term:         term,
		LeaderId:     rf.me,
		PrevLogIndex: prevIndex,
		PrevLogTerm:  prevTerm,
		Entries:      nil,
		LeaderCommit: rf.commitIndex,
		LeaderIP:     rf.LeaderIP,
	}
	rf.mu.Unlock()

	reply := &AppendEntriesReply{}
	ok := rf.sendAppendEntries(server, args, reply)
	if !ok {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if reply.Term > rf.currentTerm {
		rf.becomeFollowerLocked(reply.Term)
		return
	}
	if reply.Success {
		rf.updateLease()
	}
}

func (rf *Raft) takeHeartbeatSnapshot(term int) *heartbeatSnapshot {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.contextLostLocked(Leader, term) {
		return nil
	}

	snap := &heartbeatSnapshot{
		term:        rf.currentTerm,
		leaderId:    rf.me,
		leaderIP:    rf.LeaderIP,
		commitIndex: rf.commitIndex,
	}

	peerCount := len(rf.peers)
	snap.peers = make([]peerHeartbeatData, 0, peerCount)
	for peer := 0; peer < peerCount; peer++ {
		if peer == rf.me {
			continue
		}
		prevIndex := rf.nextIndex[peer] - 1
		if prevIndex < rf.log.snapLastIdx {
			snapshotCopy := make([]byte, len(rf.log.snapshot))
			copy(snapshotCopy, rf.log.snapshot)
			snap.peers = append(snap.peers, peerHeartbeatData{
				peer:         peer,
				snapLastIdx:  rf.log.snapLastIdx,
				snapLastTerm: rf.log.snapLastTerm,
				snapshot:     snapshotCopy,
				isSnapshot:   true,
			})
		} else {
			snap.peers = append(snap.peers, peerHeartbeatData{
				peer:         peer,
				prevLogIndex: prevIndex,
				prevLogTerm:  rf.log.at(prevIndex).Term,
				snapLastIdx:  rf.log.snapLastIdx,
				isSnapshot:   false,
			})
		}
	}
	return snap
}

func (rf *Raft) sendHeartbeatFromSnapshot(snap *heartbeatSnapshot, term int) {
	if snap == nil {
		return
	}

	var wg sync.WaitGroup
	for i := range snap.peers {
		wg.Add(1)
		go func(data peerHeartbeatData) {
			defer wg.Done()
			if data.isSnapshot {
				args := &InstallSnapshotArgs{
					Term:              snap.term,
					LeaderId:          snap.leaderId,
					LastIncludedIndex: data.snapLastIdx,
					LastIncludedTerm:  data.snapLastTerm,
					Snapshot:          data.snapshot,
				}
				rf.InstallToPeer(data.peer, term, args)
			} else {
				args := &AppendEntriesArgs{
					Term:         snap.term,
					LeaderId:     snap.leaderId,
					PrevLogIndex: data.prevLogIndex,
					PrevLogTerm:  data.prevLogTerm,
					Entries:      nil,
					LeaderCommit: snap.commitIndex,
					LeaderIP:     snap.leaderIP,
				}
				reply := &AppendEntriesReply{}
				ok := rf.sendAppendEntries(data.peer, args, reply)
				if !ok {
					return
				}
				rf.mu.Lock()
				defer rf.mu.Unlock()
				if reply.Term > rf.currentTerm {
					rf.becomeFollowerLocked(reply.Term)
					return
				}
				if reply.Success {
					rf.updateLease()
				}
			}
		}(snap.peers[i])
	}
	wg.Wait()
}

func (rf *Raft) heartbeatTicker(term int) {
	ticker := time.NewTicker(60 * time.Millisecond)
	defer ticker.Stop()

	for !rf.killed() {
		<-ticker.C

		snap := rf.takeHeartbeatSnapshot(term)
		if snap == nil {
			return
		}
		rf.sendHeartbeatFromSnapshot(snap, term)
	}
}

func (rf *Raft) unifiedReplicator(term int) {
	ticker := time.NewTicker(replicateInterval)
	defer ticker.Stop()

	for !rf.killed() {
		<-ticker.C

		rf.mu.Lock()
		if rf.contextLostLocked(Leader, term) {
			rf.mu.Unlock()
			return
		}
		rf.mu.Unlock()

		rf.startReplication(term)
	}
}
