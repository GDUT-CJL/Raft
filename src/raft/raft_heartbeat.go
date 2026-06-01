package raft

import (
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
	// 心跳永远只发空 AppendEntries，不发送快照
	// 快照由 replicator 独立发送，避免快照数据阻塞心跳路径
	prevIndex := rf.nextIndex[server] - 1
	var prevTerm int
	if prevIndex < rf.log.snapLastIdx {
		// peer 需要快照，但心跳不发送，用 snapLastIdx 作为 PrevLogIndex
		prevIndex = rf.log.snapLastIdx
		prevTerm = rf.log.snapLastTerm
	} else {
		prevTerm = rf.log.at(prevIndex).Term
	}
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
	if reply.Term > rf.currentTerm {
		raftstate := rf.becomeFollowerLocked(reply.Term)
		rf.mu.Unlock()
		if raftstate {
			rf.persistMeta(rf.currentTerm, rf.votedFor)
		}
		return
	}
	if reply.Success {
		rf.updateLease()
	}
	rf.mu.Unlock()
}

func (rf *Raft) takeHeartbeatSnapshot(term int) *heartbeatSnapshot {
	rf.mu.Lock()

	if rf.contextLostLocked(Leader, term) {
		rf.mu.Unlock()
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
		var prevTerm int
		if prevIndex < rf.log.snapLastIdx {
			// peer 需要快照，但心跳不发送快照，用 snapLastIdx 作为 PrevLogIndex
			prevIndex = rf.log.snapLastIdx
			prevTerm = rf.log.snapLastTerm
		} else {
			prevTerm = rf.log.at(prevIndex).Term
		}
		snap.peers = append(snap.peers, peerHeartbeatData{
			peer:         peer,
			prevLogIndex: prevIndex,
			prevLogTerm:  prevTerm,
			isSnapshot:   false,
		})
	}
	rf.mu.Unlock()

	return snap
}

func (rf *Raft) sendHeartbeatFromSnapshot(snap *heartbeatSnapshot, term int) {
	if snap == nil {
		return
	}

	for i := range snap.peers {
		go func(data peerHeartbeatData) {
			// 心跳只发 AppendEntries，不发送 InstallSnapshot
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
			if reply.Term > rf.currentTerm {
				raftstate := rf.becomeFollowerLocked(reply.Term)
				rf.mu.Unlock()
				if raftstate {
					rf.persistMeta(rf.currentTerm, rf.votedFor)
				}
				return
			}
			if reply.Success {
				rf.updateLease()
			}
			rf.mu.Unlock()
		}(snap.peers[i])
	}
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
