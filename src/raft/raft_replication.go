package raft

import (
	"time"
	"sort"
	"fmt"
)

type LogEntry struct{
	Term		int			// 日志对应server的任期号
	Command		interface{}	// 客户端的具体命令
	CommandValid bool	// 标记该消息是否为有效的日志命令（true=有效，false=无效，如快照消息）。
	CommandIndex int	// 日志的索引（即 Raft 日志中的位置），用于保证顺序性。
}

type AppendEntriesArgs struct{
	Term			int		// 任期
	LeaderId		int		// 对应的leader的Id

	PrevLogIndex	int		// 前一个日志号,追加新日志之前希望follower有的日志号
	PrevLogTerm		int		// 前一个日志的任期号，追加新日志之前希望follower有的任期号
	Entries			[]LogEntry //具体的日志信息

	LeaderCommit int	// leader已提交的日志号
}

type AppendEntriesReply struct{
	Term		int
	Success		bool

	// 为了更快的日志匹配，在回复的时候多加了以下两个字段
	ConfilictIndex int		// 冲突的索引号
	ConfilictTerm  int		// 冲突的任期号
}

// reply为传出参数
func (rf *Raft)AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply){
	fmt.Println("AppendEntries")
	rf.mu.Lock()
	defer rf.mu.Unlock()
	LOG(rf.me, rf.currentTerm, DDebug, "<- S%d, Receive log, Prev=[%d]T%d, Len()=%d", args.LeaderId, args.PrevLogIndex, args.PrevLogTerm, len(args.Entries))
	reply.Term = rf.currentTerm
	reply.Success = false

	if args.Term < rf.currentTerm{
		LOG(rf.me, rf.currentTerm, DLog2, "<- S%d, Reject log", args.LeaderId)
		return
	}

	if args.Term >= rf.currentTerm{
		rf.becomeFollowerLocked(args.Term)
	}

	// 重置选举超时时间，表示我不再争取作为leader，因为你已经是我的leader
	// 将这个重置放在前面，对 rf.log 进行任何操作之前调用。这确保了在处理完所有事情（例如日志匹配和追加日志条目）后才重置选举计时器
	defer rf.resetElectionTimerLocked()

	// 本地的日志太久没有与leader同步了
	// 检查args.PrevLogIndex是否超出了跟随者的日志长度。如果超出，说明跟随者的日志不够长，拒绝日志追加并记录日志。
	// 需要记录具体的任期和日志中信息进行回复
	if args.PrevLogIndex >= rf.log.size(){
		reply.ConfilictIndex = rf.log.size()
		reply.ConfilictTerm = InvalidIndex
		LOG(rf.me,rf.currentTerm,DLog2,"<- S%d,Reject log,Follower too short,len : %d <= Pre:%d",args.LeaderId,rf.log.size(),args.PrevLogIndex)
		return
	}
	// 代码执行到这里说明此时leader和follower的日志号已经一致
	// 检查任期号是否一致，只有日志号和任期号一致才可以返回成功
	if rf.log.at(args.PrevLogIndex).Term != args.PrevLogTerm{
		reply.ConfilictTerm = rf.log.at(args.PrevLogIndex).Term
		reply.ConfilictIndex = rf.log.firstFor(reply.ConfilictTerm) // 找到这个任期的第一个日志
		LOG(rf.me,rf.currentTerm,DLog2,"<- S%d,Reject log,Pre Log not match,[%d]: T%d != T%d",args.LeaderId,args.PrevLogTerm,rf.log.at(args.PrevLogIndex).Term)
		return
	}

	// 追加日志
	rf.log.appendFrom(args.PrevLogIndex,args.Entries)
	rf.persistLocked()
	reply.Success = true
	LOG(rf.me, rf.currentTerm, DLog2, "Follower append logs: (%d, %d]", args.PrevLogIndex, args.PrevLogIndex+len(args.Entries))
 
	// TODO：LeaderCommit
	// 如果leader已提交的日志号大于本地要提交的日志号
	if args.LeaderCommit > rf.commitIndex {
		LOG(rf.me, rf.currentTerm, DApply, "Follower update the commit index %d->%d", rf.commitIndex, args.LeaderCommit)
		rf.commitIndex = args.LeaderCommit// 将本地的要提交日志号更新
		if rf.commitIndex >= rf.log.size() {// 如果本地提交日志号比本身日志条目还大
			rf.commitIndex = rf.log.size() - 1 // 则将要提交的日志号更新为日志长度 - 1
		}
		rf.applyCond.Signal() // 发送信号准备提交
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}


func (rf *Raft) getMajorityIndexLocked() int{
	tmpIndexs := make([]int,len(rf.matchIndex))
	copy(tmpIndexs,rf.matchIndex)
	sort.Ints(sort.IntSlice(tmpIndexs)) // 排序
	//majorityIndex := (len(tmpIndexs) - 1) / 2 // 取中位数，代表一半以上已经复制了的日志
	majorityIndex := len(tmpIndexs) / 2  // 3节点时取索引1（第2个节点的值）
	LOG(rf.me, rf.currentTerm, DDebug, "Match index after sort: %v, majority[%d]=%d", tmpIndexs, majorityIndex, tmpIndexs[majorityIndex])
	return tmpIndexs[majorityIndex]	// 返回对应的日志号
}

// 启动日志复制和心跳
func (rf *Raft)startReplication(term int) bool{
	replicateToPeer:=func(peer int,args *AppendEntriesArgs){
		 reply := &AppendEntriesReply{}
		ok:=rf.sendAppendEntries(peer,args,reply)
		rf.mu.Lock()
        defer rf.mu.Unlock()
		if !ok{
			LOG(rf.me, rf.currentTerm, DLog, "-> S%d, Lost or crashed", peer)
			return
		}

		// 如果回复我的节点的任期比我的任期还大，那么我变为follower
		if reply.Term > rf.currentTerm{
			rf.becomeFollowerLocked(reply.Term)
			return
		}
		if rf.contextLostLocked(Leader,term){
			LOG(rf.me, rf.currentTerm, DLog, "-> S%d, Context Lost, T%d:Leader->T%d:%s", peer, term, rf.currentTerm, rf.role)
			return
		}

		// 如果回复失败，说明日志号不对，需要再向前一个任期进行检查
		// if !reply.Success{
		// 	// 记录一下当前最后一个日志号
		// 	idx := rf.nextIndex[peer] - 1
		// 	// 记录一下当前的最后一个任期号
		// 	term := rf.log[idx].Term
		// 	for idx > 0 && rf.log[idx].Term == term{
		// 		idx-- //日志号减1
		// 	}
		// 	// 将下一个日志号改为idx + 1，说明减少了一个任期准备下次再次进行对比
		// 	rf.nextIndex[peer] = idx + 1
		// 	LOG(rf.me,rf.currentTerm,DLog,"Log not matched in %d,Update next = %d",args.PrevLogIndex,rf.nextIndex[peer])
		// 	return
		// }

		// 如果回复失败，说明日志号不对，需要进行检查
		if !reply.Success{
			prevIndex := rf.nextIndex[peer]
			// 如果任期为空
			if reply.ConfilictTerm == InvalidTerm{
				// 说明follower的日志太短，直接将nextIndex赋值为ConfilictIndex退到Follower日志末尾
				rf.nextIndex[peer] = reply.ConfilictIndex
			}else{ // 否则，以 Leader 日志为准，跳过 ConfilictTerm 的所有日志
				firstTermIndex := rf.log.firstFor(reply.ConfilictTerm)
				if firstTermIndex != InvalidTerm{
					rf.nextIndex[peer] = firstTermIndex + 1
				}else{ //如果发现Leader日志中不存在ConfilictTerm的任何日志，则以Follower为准跳过ConflictTerm，即使用ConfilictIndex
					rf.nextIndex[peer] = reply.ConfilictIndex
				}
			}
			// 设置为prevNext和rf.nextIndex[peer]最小的，防止较快的RPC会覆盖掉原来的小的值
			if rf.nextIndex[peer] > prevIndex {
                rf.nextIndex[peer] = prevIndex
       		}
			nextPrevIndex := rf.nextIndex[peer] - 1
			nextPrevTerm := InvalidTerm
			if nextPrevIndex >= rf.log.snapLastIdx{
				nextPrevTerm = rf.log.at(nextPrevIndex).Term
			}
			LOG(rf.me,rf.currentTerm,DLog,"-> S%d,Not match at Prev = [%d]T%d,Try Next Prev = [%d]T%d",
				peer,args.PrevLogIndex,args.PrevLogTerm,nextPrevIndex,nextPrevTerm)

			LOG(rf.me,rf.currentTerm,DDebug,"-> %S%d,Leader log=%v",peer,rf.log.String())
			return
		}

		// 如果成功，那么将本地的matchIndex（成功匹配的日志索引号）进行更新
		rf.matchIndex[peer] = args.PrevLogIndex + len(args.Entries) // 重要！！！
		// 下一次进行发送的日志号也进行更新
		rf.nextIndex[peer] = rf.matchIndex[peer] + 1
		// TODO：更新commitIndex
		majorityMactched := rf.getMajorityIndexLocked()
		// 当日志复制过半且提交的日志对应的任期为当前的任期时候才可以提交
		if majorityMactched > rf.commitIndex && rf.log.at(majorityMactched).Term == rf.currentTerm{
			LOG(rf.me,rf.currentTerm,DApply,"leader update commit index %d -> %d",rf.commitIndex,majorityMactched)
			rf.commitIndex = majorityMactched	// 更新commitIndex的值，用于applyTicker遍历的时候使用
			rf.applyCond.Signal()	// 唤醒applyTicker中的睡眠，将已提交但未应用的日志发送到applyCh，供状态机执行
		}
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.contextLostLocked(Leader,term){
		LOG(rf.me,rf.currentTerm,DLog,"Lost Leader[%d] to %s[T%d]",term,rf.role,rf.currentTerm)
		return false
	}
	for peer:=0;peer < len(rf.peers);peer++{
		if peer == rf.me{//如果是自己
			rf.matchIndex[peer] = rf.log.size() -1
			rf.nextIndex[peer] = rf.log.size()
			continue
		}
		// 初始化传入的参数，也即是要发送给各个其他RPC节点的参数
		prevIndex := rf.nextIndex[peer] - 1
		if prevIndex < rf.log.snapLastIdx{
			args:=&InstallSnapshotArgs{
				Term:rf.currentTerm,
				LeaderId:rf.me,
				LastIncludedIndex:rf.log.snapLastIdx,
				LastIncludedTerm:rf.log.snapLastTerm,
				Snapshot:rf.log.snapshot,
			}
			go rf.InstallToPeer(peer,term,args)
			continue
		}
		prevTerm := rf.log.at(prevIndex).Term
		args:= &AppendEntriesArgs{
			Term:term,
			LeaderId:rf.me,
			PrevLogIndex:prevIndex,
			PrevLogTerm:prevTerm,
			Entries:rf.log.tail(prevIndex+1), // 从 rf.log 切片中截取从索引 prevIndex+1 开始到末尾的所有日志条目。左闭右开
			LeaderCommit:rf.commitIndex,
		}
		go replicateToPeer(peer,args)
	}
	return true
}

// 只有在某个term内循环，当这个任期结束则心跳结束
func (rf* Raft)replicationTicker(term int){
	for !rf.killed(){
		ok := rf.startReplication(term)
		if !ok{
			break
		}
		time.Sleep(replicateInterval)
	}
}
