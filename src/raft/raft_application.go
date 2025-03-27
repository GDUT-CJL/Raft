package raft

// 将已提交但未应用的日志发送到 applyCh，供状态机执行
func (rf *Raft)applyTicker(){
	for !rf.killed(){
		rf.mu.Lock()
		// Wait() 阻塞，直到有新的已提交日志需要应用（由其他协程通过 rf.applyCond.Signal() 触发）
		rf.applyCond.Wait()
		entries := make([]LogEntry, 0)
		// 从 rf.lastApplied + 1 到 rf.commitIndex 的日志范围（这些日志已提交但未应用）
		for i:= rf.lastApplied + 1; i <= rf.commitIndex; i++{	//这里是等于号 <= ！！！
			entries = append(entries,rf.log[i])
		}
		rf.mu.Unlock()
		// 将日志条目按顺序发送到 applyCh，由状态机（如 KV 存储）执行具体命令。
		for i,entry := range entries{
			rf.applyCh <- ApplyMsg{
				CommandValid:entry.CommandValid,
				Command:entry.Command,
				CommandIndex:rf.lastApplied + 1 + i, // 注意，rf.lastApplied + 1 + i 确保索引连续（例如，lastApplied=2，entries 有 2 条，则发送索引 3 和 4）
			}
		}
	
		rf.mu.Lock()
		LOG(rf.me, rf.currentTerm, DApply, "Apply log for [%d, %d]", rf.lastApplied+1, rf.lastApplied+len(entries))
		rf.lastApplied += len(entries) // 更新lastApplied，标记这些日志已应用，避免重复处理。
		rf.mu.Unlock()
	}
}