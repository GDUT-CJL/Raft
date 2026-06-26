package raft

import "log"

// 将已经提交但还未应用的日志发送到 applyCh，供状态机执行。
func (rf *Raft) applyTicker() {
	for !rf.killed() {
		rf.mu.Lock()
		for rf.lastApplied >= rf.commitIndex && !rf.snapAppending {
			rf.applyCond.Wait()
		}

		if rf.snapAppending {
			snapshot := make([]byte, len(rf.log.snapshot))
			copy(snapshot, rf.log.snapshot)
			snapIndex := rf.log.snapLastIdx
			snapTerm := rf.log.snapLastTerm
			log.Printf("Raft[%d]: applyTicker sending snapshot to applyCh, bytes=%d snapIndex=%d snapTerm=%d",
				rf.me, len(snapshot), snapIndex, snapTerm)
			rf.snapAppending = false
			rf.mu.Unlock()

			rf.applyCh <- ApplyMsg{
				SnapshotValid: true,
				Snapshot:      snapshot,
				SnapshotIndex: snapIndex,
				SnapshotTerm:  snapTerm,
			}

			rf.mu.Lock()
			if snapIndex > rf.lastApplied {
				rf.lastApplied = snapIndex
			}
			if rf.commitIndex < snapIndex {
				rf.commitIndex = snapIndex
			}
			LOG(rf.me, rf.currentTerm, DApply, "Applied SnapShot [0, %d]", snapIndex)
			rf.mu.Unlock()
			continue
		}

		entries := make([]LogEntry, 0)
		snapAppendingApply := rf.snapAppending
		if !snapAppendingApply {
			if rf.lastApplied < rf.log.snapLastIdx {
				rf.lastApplied = rf.log.snapLastIdx
			}
			start := rf.lastApplied + 1
			end := rf.commitIndex
			if end >= rf.log.size() {
				end = rf.log.size() - 1
			}
			for i := start; i <= end; i++ {
				entries = append(entries, rf.log.at(i))
			}
		}
		rf.mu.Unlock()

		if !snapAppendingApply {
			for i, entry := range entries {
				rf.applyCh <- ApplyMsg{
					CommandValid: entry.CommandValid,
					Command:      entry.Command,
					CommandIndex: rf.lastApplied + 1 + i,
				}
			}
		} else {
			rf.applyCh <- ApplyMsg{
				SnapshotValid: true,
				Snapshot:      rf.log.snapshot,
				SnapshotIndex: rf.log.snapLastIdx,
				SnapshotTerm:  rf.log.snapLastTerm,
			}
		}

		rf.mu.Lock()
		if !snapAppendingApply {
			LOG(rf.me, rf.currentTerm, DApply, "Apply log for [%d, %d]", rf.lastApplied+1, rf.lastApplied+len(entries))
			rf.lastApplied += len(entries)
		} else {
			LOG(rf.me, rf.currentTerm, DApply, "Apply SnapShot for [0, %d]", rf.log.snapLastIdx)
			rf.lastApplied = rf.log.snapLastIdx
			if rf.commitIndex < rf.lastApplied {
				rf.commitIndex = rf.lastApplied
			}
			rf.snapAppending = false
		}
		rf.mu.Unlock()
	}
}
