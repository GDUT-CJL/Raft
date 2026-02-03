package raft

import (
	"time"
)

// 专门发送心跳的函数
func (rf *Raft) sendHeartbeat(server int, term int) {
	if rf.me == server {
		return
	}

	rf.mu.Lock()
	prevIndex := rf.nextIndex[server] - 1
	if prevIndex < rf.log.snapLastIdx {
		// 需要发送快照的情况
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
		Entries:      nil, // 空条目表示心跳
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

	// 处理回复（任期检查等）
	if reply.Term > rf.currentTerm {
		rf.becomeFollowerLocked(reply.Term)
		return
	}
	if reply.Success {
		rf.updateLease()
	}
	// 更新日志匹配信息（如果有）
	if reply.Success && len(args.Entries) > 0 {
		rf.matchIndex[server] = args.PrevLogIndex + len(args.Entries)
		rf.nextIndex[server] = rf.matchIndex[server] + 1
	}
}

// 独立的心跳发送器
func (rf *Raft) heartbeatTicker(term int) {
	for !rf.killed() {
		rf.mu.Lock()
		if rf.contextLostLocked(Leader, term) {
			rf.mu.Unlock()
			return
		}

		// 遍历所有 peer，发送心跳（除了自己）
		for peer := 0; peer < len(rf.peers); peer++ {
			if peer != rf.me {
				go rf.sendHeartbeat(peer, term)
			}
		}

		rf.mu.Unlock()

		// 心跳间隔：比如每 100ms 发送一轮
		time.Sleep(100 * time.Millisecond)
	}
}
