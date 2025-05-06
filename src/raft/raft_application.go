package raft
//import "fmt"
// 将已提交但未应用的日志发送到 applyCh，供状态机执行
func (rf *Raft)applyTicker(){
	for !rf.killed(){
		rf.mu.Lock()
		// Wait() 阻塞，直到有新的已提交日志需要应用（由其他协程通过 rf.applyCond.Signal() 触发）
		rf.applyCond.Wait()
		//fmt.Println("开始应用日志")
		entries := make([]LogEntry, 0)
		snapAppendingApply := rf.snapAppending
		// 如果没有快照
		if !snapAppendingApply{
			// 从 rf.lastApplied + 1 到 rf.commitIndex 的日志范围（这些日志已提交但未应用）
			if rf.lastApplied < rf.log.snapLastIdx {
				rf.lastApplied = rf.log.snapLastIdx
			}
			// make sure that the rf.log have all the entries
			start := rf.lastApplied + 1
			end := rf.commitIndex
			if end >= rf.log.size() {
				end = rf.log.size() - 1
			}
			for i := start; i <= end; i++ {
				entries = append(entries, rf.log.at(i))
				//fmt.Println("扩展日志")
			}
		}
		rf.mu.Unlock()
		// 如果没有快照
		if !snapAppendingApply{
			// 将日志条目按顺序发送到 applyCh，由状态机（如 KV 存储）执行具体命令。
			//fmt.Println("将日志提交到applyCh")
			for i,entry := range entries{
				rf.applyCh <- ApplyMsg{
					CommandValid:entry.CommandValid,
					Command:entry.Commands,
					CommandIndex:rf.lastApplied + 1 + i, // 注意，rf.lastApplied + 1 + i 确保索引连续（例如，lastApplied=2，entries 有 2 条，则发送索引 3 和 4）
				}
			}
		}else{
			rf.applyCh <- ApplyMsg{
				SnapshotValid:true,
				Snapshot:rf.log.snapshot,
				SnapshotIndex:rf.log.snapLastIdx,
				SnapshotTerm:rf.log.snapLastTerm,
			}
		}
		rf.mu.Lock()
		if !snapAppendingApply{
			LOG(rf.me, rf.currentTerm, DApply, "Apply log for [%d, %d]", rf.lastApplied+1, rf.lastApplied+len(entries))
			//fmt.Printf("提交[%d, %d]",rf.lastApplied+1, rf.lastApplied+len(entries))
			rf.lastApplied += len(entries) // 更新lastApplied，标记这些日志已应用，避免重复处理。
		}else{
			LOG(rf.me, rf.currentTerm, DApply, "Apply SnapShot for [0, %d]",rf.log.snapLastIdx)
			rf.lastApplied = rf.log.snapLastIdx
			if rf.commitIndex < rf.lastApplied{
				rf.commitIndex = rf.lastApplied
			}
			rf.snapAppending = false
		}
		rf.mu.Unlock()
	}
}