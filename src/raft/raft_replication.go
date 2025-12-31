package raft

import (
	"context"
	//"encoding/json"
	//"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

type OperationType uint32
type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Key    string
	Klen   int
	Value  string
	Vlen   int
	OpType OperationType
	// 每次请求都会生成ClientId和SeqId
	// ClientId和SeqId确定唯一的一次请求避免重复请求
	ClientId int64
	SeqId    int64
}
type LogEntry struct {
	Term         int  // 日志对应server的任期号
	Command      []Op // 客户端的具体命令
	CommandValid bool // 标记该消息是否为有效的日志命令（true=有效，false=无效，如快照消息）。
	CommandIndex int  // 日志的索引（即 Raft 日志中的位置），用于保证顺序性。
}

type AppendEntriesArgs struct {
	Term     int // 任期
	LeaderId int // 对应的leader的Id

	PrevLogIndex int        // 前一个日志号,追加新日志之前希望follower有的日志号
	PrevLogTerm  int        // 前一个日志的任期号，追加新日志之前希望follower有的任期号
	Entries      []LogEntry //具体的日志信息

	LeaderCommit int // leader已提交的日志号
	LeaderIP     string
}

type AppendEntriesReply struct {
	Term    int
	Success bool

	// 为了更快的日志匹配，在回复的时候多加了以下两个字段
	ConfilictIndex int // 冲突的索引号
	ConfilictTerm  int // 冲突的任期号
}

var (
	gLogEntryPool = sync.Pool{
		New: func() interface{} {
			return &G_LogEntry{}
		},
	}

	gOpPool = sync.Pool{
		New: func() interface{} {
			return &G_Op{}
		},
	}
)

// reply为传出参数
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	//fmt.Printf("leaderid = %d 在任期 %d 发出AppendEntries\n",args.LeaderId,args.Term)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	LOG(rf.me, rf.currentTerm, DDebug, "<- S%d, Receive log, Prev=[%d]T%d, Len()=%d", args.LeaderId, args.PrevLogIndex, args.PrevLogTerm, len(args.Entries))
	reply.Term = rf.currentTerm
	reply.Success = false

	if args.Term < rf.currentTerm {
		LOG(rf.me, rf.currentTerm, DLog2, "<- S%d, Reject log", args.LeaderId)
		return
	}

	if args.Term >= rf.currentTerm {
		rf.becomeFollowerLocked(args.Term)
	}

	// 重置选举超时时间，表示我不再争取作为leader，因为你已经是我的leader
	// 将这个重置放在前面，对 rf.log 进行任何操作之前调用。这确保了在处理完所有事情（例如日志匹配和追加日志条目）后才重置选举计时器
	defer rf.resetElectionTimerLocked()

	// 本地的日志太久没有与leader同步了
	// 检查args.PrevLogIndex是否超出了跟随者的日志长度。如果超出，说明跟随者的日志不够长，拒绝日志追加并记录日志。
	// 需要记录具体的任期和日志中信息进行回复
	if args.PrevLogIndex >= rf.log.size() {
		reply.ConfilictIndex = rf.log.size()
		reply.ConfilictTerm = InvalidIndex
		LOG(rf.me, rf.currentTerm, DLog2, "<- S%d,Reject log,Follower too short,len : %d <= Pre:%d", args.LeaderId, rf.log.size(), args.PrevLogIndex)
		return
	}
	// 检查领导者的前一个日志索引 (PrevLogIndex) 是否小于跟随者的快照最后索引 (snapLastIdx)。
	// 这表示跟随者的日志已经被截断，而该索引对应的条目不再存在于跟随者的日志中。
	if args.PrevLogIndex < rf.log.snapLastIdx {
		reply.ConfilictTerm = rf.log.snapLastTerm
		reply.ConfilictIndex = rf.log.snapLastIdx
		LOG(rf.me, rf.currentTerm, DLog2, "<- S%d, Reject log, Follower log truncated in %d", args.LeaderId, rf.log.snapLastIdx)
		return
	}
	// 代码执行到这里说明此时leader和follower的日志号已经一致
	// 检查任期号是否一致，只有日志号和任期号一致才可以返回成功
	if rf.log.at(args.PrevLogIndex).Term != args.PrevLogTerm {
		reply.ConfilictTerm = rf.log.at(args.PrevLogIndex).Term
		reply.ConfilictIndex = rf.log.firstFor(reply.ConfilictTerm) // 在follower找到这个任期的第一个日志，Leader可以据此快速移动其匹配位置，缩短同步时间。
		LOG(rf.me, rf.currentTerm, DLog2, "<- S%d,Reject log,Pre Log not match,[%d]: T%d != T%d", args.LeaderId, args.PrevLogTerm, rf.log.at(args.PrevLogIndex).Term)
		return
	}
	// 记录leaderIP
	rf.LeaderIP = args.LeaderIP

	// 追加日志
	rf.log.appendFrom(args.PrevLogIndex, args.Entries)
	rf.persistLocked()
	reply.Success = true
	LOG(rf.me, rf.currentTerm, DLog2, "Follower append logs: (%d, %d]", args.PrevLogIndex, args.PrevLogIndex+len(args.Entries))

	// TODO：LeaderCommit
	// 如果leader已提交的日志号大于本地要提交的日志号
	if args.LeaderCommit > rf.commitIndex {
		LOG(rf.me, rf.currentTerm, DApply, "Follower update the commit index %d->%d", rf.commitIndex, args.LeaderCommit)
		rf.commitIndex = args.LeaderCommit   // 将本地的要提交日志号更新
		if rf.commitIndex >= rf.log.size() { // 如果本地提交日志号比本身日志条目还大
			rf.commitIndex = rf.log.size() - 1 // 则将要提交的日志号更新为日志长度 - 1
		}
		rf.applyCond.Signal() // 发送信号准备提交
	}
}

// 转换函数：内部 LogEntry → gRPC LogEntry（bytes版本）
func convertToGrpcLogEntries(entries []LogEntry) []*G_LogEntry {
	if len(entries) == 0 {
		return nil
	}

	grpcEntries := make([]*G_LogEntry, len(entries))

	for i, entry := range entries {
		// 从池中获取G_LogEntry
		grpcEntry := gLogEntryPool.Get().(*G_LogEntry)
		grpcEntry.Term = int32(entry.Term)
		grpcEntry.CommandValid = entry.CommandValid
		grpcEntry.CommandIndex = int32(entry.CommandIndex)

		// 预分配Command切片
		if cap(grpcEntry.Command) >= len(entry.Command) {
			grpcEntry.Command = grpcEntry.Command[:len(entry.Command)]
		} else {
			grpcEntry.Command = make([]*G_Op, len(entry.Command))
		}

		// 填充Command
		for j, op := range entry.Command {
			var gOp *G_Op
			if j < len(grpcEntry.Command) && grpcEntry.Command[j] != nil {
				gOp = grpcEntry.Command[j]
			} else {
				gOp = gOpPool.Get().(*G_Op)
				grpcEntry.Command[j] = gOp
			}

			gOp.ClientId = op.ClientId
			gOp.SeqId = op.SeqId
			gOp.OpType = uint32(op.OpType)

			// 将 string 转换为 []byte
			// 关键：创建新的 []byte 副本，避免共享底层数组
			if op.Key != "" {
				keyBytes := make([]byte, len(op.Key))
				copy(keyBytes, op.Key)
				gOp.Key = keyBytes
			} else {
				gOp.Key = nil
			}

			if op.Value != "" {
				valueBytes := make([]byte, len(op.Value))
				copy(valueBytes, op.Value)
				gOp.Value = valueBytes
			} else {
				gOp.Value = nil
			}

			gOp.Klen = int64(op.Klen)
			gOp.Vlen = int64(op.Vlen)
		}

		grpcEntries[i] = grpcEntry
	}

	return grpcEntries
}

// 对应的清理函数（需要修改）
func recycleGrpcLogEntries(entries []*G_LogEntry) {
	for _, entry := range entries {
		if entry == nil {
			continue
		}

		// 回收Command中的G_Op
		for _, gOp := range entry.Command {
			if gOp != nil {
				// 重置字段（对于 []byte，设置为 nil）
				gOp.Key = nil
				gOp.Value = nil
				gOpPool.Put(gOp)
			}
		}

		// 重置G_LogEntry
		entry.Command = entry.Command[:0]
		gLogEntryPool.Put(entry)
	}
}

// 包装sendAppendEntries以进行性能监控
func (rf *Raft) sendAppendEntriesWithMetrics(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	start := time.Now()
	defer func() {
		duration := time.Since(start)

		if len(args.Entries) > 0 {
			// 真正的日志复制
			atomic.AddInt64(&rf.perfStats.appendRpcCount, 1)
			atomic.AddInt64(&rf.perfStats.appendRpcTime, duration.Milliseconds())
			// 记录慢RPC
			// if duration > 50*time.Millisecond {
			// 	fmt.Printf("Slow AppendEntries to S%d: %v, entries=%d\n", server, duration, len(args.Entries))
			// }
		} else {
			// 心跳
			atomic.AddInt64(&rf.perfStats.heartbeatCount, 1)
			atomic.AddInt64(&rf.perfStats.heartbeatTime, duration.Milliseconds())
			// if duration > 10*time.Millisecond {
			// 	fmt.Printf("Slow heartbeat to S%d: %v\n", server, duration)
			// }
		}
	}()

	return rf.sendAppendEntries(server, args, reply)
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	if rf.me == server {
		return false
	}
	grpcEntries := convertToGrpcLogEntries(args.Entries)
	leaderIPBytes := []byte(args.LeaderIP)
	grpcArgs := &G_AppendEntriesArgs{
		Term:         int64(args.Term),
		LeaderId:     int64(args.LeaderId),
		PrevLogIndex: int64(args.PrevLogIndex),
		PrevLogTerm:  int64(args.PrevLogTerm),
		Entries:      grpcEntries,
		LeaderCommit: int64(args.LeaderCommit),
		LeaderIP:     leaderIPBytes,
	}

	timeout := 200 * time.Millisecond // 默认200ms
	// if len(args.Entries) == 0 {
	// 	timeout = 50 * time.Millisecond // 心跳超时50ms
	// } else if len(args.Entries) < 10 {
	// 	timeout = 50 * time.Millisecond // 小批量超时100ms
	// }
	// 设置超时上下文
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	// 回收gRPC对象
	grpcReply, err := rf.peers[server].Grpc_AppendEntries(ctx, grpcArgs)
	// 回收gRPC对象
	recycleGrpcLogEntries(grpcEntries)
	if err != nil {
		if len(args.Entries) > 0 {
			//fmt.Printf("Grpc_AppendEntries发送给 S%d 失败, err=%v\n", server, err)
		}
		return false
	}
	reply.Term = int(grpcReply.Term)
	reply.Success = grpcReply.Success
	reply.ConfilictIndex = int(grpcReply.ConflictIndex)
	reply.ConfilictTerm = int(grpcReply.ConflictTerm)

	return true
}

func (rf *Raft) getMajorityIndexLocked() int {
	tmpIndexs := make([]int, len(rf.matchIndex))
	copy(tmpIndexs, rf.matchIndex)
	sort.Ints(sort.IntSlice(tmpIndexs))       // 排序
	majorityIndex := (len(tmpIndexs) - 1) / 2 // 取中位数，代表一半以上已经复制了的日志
	LOG(rf.me, rf.currentTerm, DDebug, "Match index after sort: %v, majority[%d]=%d", tmpIndexs, majorityIndex, tmpIndexs[majorityIndex])
	return tmpIndexs[majorityIndex] // 返回对应的日志号
}

// 启动日志复制和心跳
func (rf *Raft) startReplication(term int) bool {
	replicateToPeer := func(peer int, args *AppendEntriesArgs) {
		reply := &AppendEntriesReply{}
		ok := rf.sendAppendEntriesWithMetrics(peer, args, reply)
		rf.mu.Lock()
		defer rf.mu.Unlock()
		if !ok {
			LOG(rf.me, rf.currentTerm, DLog, "-> S%d, Lost or crashed", peer)
			return
		}

		// 如果回复我的节点的任期比我的任期还大，那么我变为follower
		if reply.Term > rf.currentTerm {
			rf.becomeFollowerLocked(reply.Term)
			return
		}
		if rf.contextLostLocked(Leader, term) {
			LOG(rf.me, rf.currentTerm, DLog, "-> S%d, Context Lost, T%d:Leader->T%d:%s", peer, term, rf.currentTerm, rf.role)
			return
		}
		// 如果回复失败，说明日志号不对，需要进行检查
		if !reply.Success {
			prevIndex := rf.nextIndex[peer]
			// 如果任期为空
			if reply.ConfilictTerm == InvalidTerm {
				// 说明follower的日志太短，直接将nextIndex赋值为ConfilictIndex退到Follower日志末尾
				rf.nextIndex[peer] = reply.ConfilictIndex
			} else { // 否则，以 Leader 日志为准，跳过 ConfilictTerm 的所有日志
				firstTermIndex := rf.log.firstFor(reply.ConfilictTerm)
				if firstTermIndex != InvalidTerm {
					rf.nextIndex[peer] = firstTermIndex + 1
				} else { //如果发现Leader日志中不存在ConfilictTerm的任何日志，则以Follower为准跳过ConflictTerm，即使用ConfilictIndex
					rf.nextIndex[peer] = reply.ConfilictIndex
				}
			}
			// 设置为prevNext和rf.nextIndex[peer]最小的，防止较快的RPC会覆盖掉原来的小的值
			if rf.nextIndex[peer] > prevIndex {
				rf.nextIndex[peer] = prevIndex
			}
			nextPrevIndex := rf.nextIndex[peer] - 1
			nextPrevTerm := InvalidTerm
			if nextPrevIndex >= rf.log.snapLastIdx {
				nextPrevTerm = rf.log.at(nextPrevIndex).Term
			}
			LOG(rf.me, rf.currentTerm, DLog, "-> S%d,Not match at Prev = [%d]T%d,Try Next Prev = [%d]T%d",
				peer, args.PrevLogIndex, args.PrevLogTerm, nextPrevIndex, nextPrevTerm)

			LOG(rf.me, rf.currentTerm, DDebug, "-> %S%d,Leader log=%v", peer, rf.log.String())
			return
		}

		// 如果成功，那么将本地的matchIndex（成功匹配的日志索引号）进行更新
		rf.matchIndex[peer] = args.PrevLogIndex + len(args.Entries) // 重要！！！
		// 下一次进行发送的日志号也进行更新
		rf.nextIndex[peer] = rf.matchIndex[peer] + 1
		// TODO：更新commitIndex
		majorityMactched := rf.getMajorityIndexLocked()
		// 当日志复制过半且提交的日志对应的任期为当前的任期时候才可以提交
		if majorityMactched > rf.commitIndex && rf.log.at(majorityMactched).Term == rf.currentTerm {
			LOG(rf.me, rf.currentTerm, DApply, "leader update commit index %d -> %d", rf.commitIndex, majorityMactched)
			rf.commitIndex = majorityMactched // 更新commitIndex的值，用于applyTicker遍历的时候使用
			rf.applyCond.Signal()             // 唤醒applyTicker中的睡眠，将已提交但未应用的日志发送到applyCh，供状态机执行
		}
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.contextLostLocked(Leader, term) {
		LOG(rf.me, rf.currentTerm, DLog, "Lost Leader[%d] to %s[T%d]", term, rf.role, rf.currentTerm)
		return false
	}
	for peer := 0; peer < len(rf.peers); peer++ {
		if peer == rf.me { //如果是自己
			rf.matchIndex[peer] = rf.log.size() - 1
			rf.nextIndex[peer] = rf.log.size()
			continue
		}
		// 初始化传入的参数，也即是要发送给各个其他RPC节点的参数
		prevIndex := rf.nextIndex[peer] - 1
		if prevIndex < rf.log.snapLastIdx {
			args := &InstallSnapshotArgs{
				Term:              rf.currentTerm,
				LeaderId:          rf.me,
				LastIncludedIndex: rf.log.snapLastIdx,
				LastIncludedTerm:  rf.log.snapLastTerm,
				Snapshot:          rf.log.snapshot,
			}
			go rf.InstallToPeer(peer, term, args)
			continue
		}
		prevTerm := rf.log.at(prevIndex).Term
		args := &AppendEntriesArgs{
			Term:         term,
			LeaderId:     rf.me,
			PrevLogIndex: prevIndex,
			PrevLogTerm:  prevTerm,
			Entries:      rf.log.tail(prevIndex + 1), // 从 rf.log 切片中截取从索引 prevIndex+1 开始到末尾的所有日志条目。左闭右开
			LeaderCommit: rf.commitIndex,
			LeaderIP:     rf.LeaderIP,
		}
		go replicateToPeer(peer, args)
	}
	return true
}

// 只有在某个term内循环，当这个任期结束则心跳结束
func (rf *Raft) replicationTicker(term int) {
	for !rf.killed() {
		ok := rf.startReplication(term)
		if !ok {
			break
		}
		time.Sleep(replicateInterval)
	}
}
