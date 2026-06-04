package raft

import (
	"context"
	"sort"

	//"sync"
	"fmt"
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

// reply为传出参数
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	LOG(rf.me, rf.currentTerm, DDebug, "<- S%d, Receive log, Prev=[%d]T%d, Len()=%d", args.LeaderId, args.PrevLogIndex, args.PrevLogTerm, len(args.Entries))
	reply.Term = rf.currentTerm
	reply.Success = false
	termUpdated := false

	if args.Term < rf.currentTerm {
		LOG(rf.me, rf.currentTerm, DLog2, "<- S%d, Reject log", args.LeaderId)
		rf.mu.Unlock()
		return
	}

	if args.Term >= rf.currentTerm {
		shouldPersist := rf.becomeFollowerLocked(args.Term)
		if shouldPersist {
			termUpdated = true
			currentTerm := rf.currentTerm
			votedFor := rf.votedFor
			rf.mu.Unlock()
			rf.persistMeta(currentTerm, votedFor)
			rf.mu.Lock()
		}
	}

	rf.resetElectionTimerLocked()

	if args.PrevLogIndex >= rf.log.size() {
		reply.ConfilictIndex = rf.log.size()
		reply.ConfilictTerm = InvalidIndex
		LOG(rf.me, rf.currentTerm, DLog2, "<- S%d,Reject log,Follower too short,len : %d <= Pre:%d", args.LeaderId, rf.log.size(), args.PrevLogIndex)
		rf.mu.Unlock()
		return
	}
	if args.PrevLogIndex < rf.log.snapLastIdx {
		reply.ConfilictTerm = rf.log.snapLastTerm
		reply.ConfilictIndex = rf.log.snapLastIdx
		LOG(rf.me, rf.currentTerm, DLog2, "<- S%d, Reject log, Follower log truncated in %d", args.LeaderId, rf.log.snapLastIdx)
		rf.mu.Unlock()
		return
	}
	if rf.log.at(args.PrevLogIndex).Term != args.PrevLogTerm {
		reply.ConfilictTerm = rf.log.at(args.PrevLogIndex).Term
		reply.ConfilictIndex = rf.log.firstFor(reply.ConfilictTerm)
		LOG(rf.me, rf.currentTerm, DLog2, "<- S%d,Reject log,Pre Log not match,[%d]: T%d != T%d", args.LeaderId, args.PrevLogTerm, rf.log.at(args.PrevLogIndex).Term)
		rf.mu.Unlock()
		return
	}

	rf.LeaderIP = args.LeaderIP

	prevLogIndex := args.PrevLogIndex
	entries := make([]LogEntry, len(args.Entries))
	copy(entries, args.Entries)
	leaderCommit := args.LeaderCommit

	rf.log.appendFrom(prevLogIndex, entries)

	needSignal := false
	if leaderCommit > rf.commitIndex {
		LOG(rf.me, rf.currentTerm, DApply, "Follower update the commit index %d->%d", rf.commitIndex, leaderCommit)
		rf.commitIndex = leaderCommit
		if rf.commitIndex >= rf.log.size() {
			rf.commitIndex = rf.log.size() - 1
		}
		needSignal = true
	}

	LOG(rf.me, rf.currentTerm, DLog2, "Follower append logs: (%d, %d]", prevLogIndex, prevLogIndex+len(entries))

	currentTerm := rf.currentTerm
	votedFor := rf.votedFor
	rf.mu.Unlock()

	// 同步持久化 占用RPC的时间
	if termUpdated {
		rf.persistMetaAndLog(currentTerm, votedFor, prevLogIndex, entries)
	} else {
		rf.persistLogReplace(prevLogIndex, entries)
	}
	// 持久化成功后才回复reply.Success = true
	reply.Success = true
	LOG(rf.me, rf.currentTerm, DPersist, "Follower persist completed for logs: (%d, %d]", prevLogIndex, prevLogIndex+len(entries))
	if needSignal {
		rf.mu.Lock()
		rf.applyCond.Signal()
		rf.mu.Unlock()
	}
}

// 转换函数：内部 LogEntry → gRPC LogEntry（bytes版本）- 简化版本，不使用对象池
func convertToGrpcLogEntries(entries []LogEntry) []*G_LogEntry {
	if len(entries) == 0 {
		return nil
	}

	grpcEntries := make([]*G_LogEntry, len(entries))

	for i, entry := range entries {
		// 直接创建新的 G_LogEntry，不使用对象池
		grpcEntry := &G_LogEntry{
			Term:         int32(entry.Term),
			CommandValid: entry.CommandValid,
			CommandIndex: int32(entry.CommandIndex),
		}

		// 创建 Command 切片
		if len(entry.Command) > 0 {
			grpcEntry.Command = make([]*G_Op, len(entry.Command))

			for j, op := range entry.Command {
				// 直接创建新的 G_Op，不使用对象池
				gOp := &G_Op{
					ClientId: op.ClientId,
					SeqId:    op.SeqId,
					OpType:   uint32(op.OpType),
					Klen:     int64(op.Klen),
					Vlen:     int64(op.Vlen),
				}

				// 将 string 转换为 []byte
				if op.Key != "" {
					keyBytes := make([]byte, len(op.Key))
					copy(keyBytes, op.Key)
					gOp.Key = keyBytes
				}

				if op.Value != "" {
					valueBytes := make([]byte, len(op.Value))
					copy(valueBytes, op.Value)
					gOp.Value = valueBytes
				}

				grpcEntry.Command[j] = gOp
			}
		}

		grpcEntries[i] = grpcEntry
	}

	return grpcEntries
}

func appendEntriesTimeout(entries []LogEntry) time.Duration {
	timeout := 2 * time.Second
	if len(entries) == 0 {
		return timeout
	}

	timeout += time.Duration(len(entries)) * 120 * time.Millisecond
	if timeout > 30*time.Second {
		return 30 * time.Second
	}
	return timeout
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	if rf.me == server {
		return false
	}

	// 创建新的 gRPC 参数对象，不使用对象池
	grpcEntries := convertToGrpcLogEntries(args.Entries)

	// 注意：LeaderIP 可能为空，需要检查
	var leaderIPBytes []byte
	if args.LeaderIP != "" {
		leaderIPBytes = []byte(args.LeaderIP)
	}

	grpcArgs := &G_AppendEntriesArgs{
		Term:         int64(args.Term),
		LeaderId:     int64(args.LeaderId),
		PrevLogIndex: int64(args.PrevLogIndex),
		PrevLogTerm:  int64(args.PrevLogTerm),
		Entries:      grpcEntries,
		LeaderCommit: int64(args.LeaderCommit),
		LeaderIP:     leaderIPBytes,
	}

	timeout := appendEntriesTimeout(args.Entries)
	// 设置超时上下文
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	grpcReply, err := rf.peers[server].Grpc_AppendEntries(ctx, grpcArgs)

	// 注意：这里我们不再尝试回收对象，让GC自动处理

	if err != nil {
		// 仅在真实日志复制失败时打印错误，避免心跳噪音
		if len(args.Entries) > 0 {
			fmt.Printf("Grpc_AppendEntries发送给 S%d 失败, err=%v, entries=%d\n", server, err, len(args.Entries))
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

type replicationTask struct {
	peer         int
	args         *AppendEntriesArgs
	snapshotArgs *InstallSnapshotArgs
	isSnapshot   bool
}

// func (rf *Raft) startReplication(term int) bool {
// 	rf.mu.Lock()
// 	if rf.contextLostLocked(Leader, term) {
// 		LOG(rf.me, rf.currentTerm, DLog, "Lost Leader[%d] to %s[T%d]", term, rf.role, rf.currentTerm)
// 		rf.mu.Unlock()
// 		return false
// 	}

// 	tasks := make([]replicationTask, 0, len(rf.peers))
// 	for peer := 0; peer < len(rf.peers); peer++ {
// 		if peer == rf.me {
// 			rf.matchIndex[peer] = rf.log.size() - 1
// 			rf.nextIndex[peer] = rf.log.size()
// 			continue
// 		}

// 		prevIndex := rf.nextIndex[peer] - 1
// 		if prevIndex < rf.log.snapLastIdx {
// 			// 如果已经在发送快照，跳过，避免重复发送
// 			if rf.snapSending[peer] {
// 				continue
// 			}
// 			rf.snapSending[peer] = true
// 			// 必须在锁内拷贝快照数据，因为释放锁后 goroutine 可能访问到被修改的数据
// 			snapshotCopy := make([]byte, len(rf.log.snapshot))
// 			copy(snapshotCopy, rf.log.snapshot)
// 			args := &InstallSnapshotArgs{
// 				Term:              rf.currentTerm,
// 				LeaderId:          rf.me,
// 				LastIncludedIndex: rf.log.snapLastIdx,
// 				LastIncludedTerm:  rf.log.snapLastTerm,
// 				Snapshot:          snapshotCopy,
// 			}
// 			tasks = append(tasks, replicationTask{
// 				peer:         peer,
// 				snapshotArgs: args,
// 				isSnapshot:   true,
// 			})
// 			continue
// 		}

// 		prevTerm := rf.log.at(prevIndex).Term
// 		entries := rf.log.tail(prevIndex + 1)
// 		args := &AppendEntriesArgs{
// 			Term:         term,
// 			LeaderId:     rf.me,
// 			PrevLogIndex: prevIndex,
// 			PrevLogTerm:  prevTerm,
// 			Entries:      entries,
// 			LeaderCommit: rf.commitIndex,
// 			LeaderIP:     rf.LeaderIP,
// 		}
// 		tasks = append(tasks, replicationTask{
// 			peer:       peer,
// 			args:       args,
// 			isSnapshot: false,
// 		})
// 	}
// 	rf.mu.Unlock()

// 	for _, task := range tasks {
// 		if task.isSnapshot {
// 			go rf.InstallToPeer(task.peer, term, task.snapshotArgs)
// 		} else {
// 			go rf.processReplicationReply(task.peer, task.args, term)
// 		}
// 	}
// 	return true
// }

// func (rf *Raft) processReplicationReply(peer int, args *AppendEntriesArgs, term int) {
// 	reply := &AppendEntriesReply{}
// 	ok := rf.sendAppendEntries(peer, args, reply)
// 	if !ok {
// 		LOG(rf.me, rf.currentTerm, DLog, "-> S%d, Lost or crashed", peer)
// 		return
// 	}

// 	rf.mu.Lock()
// 	defer rf.mu.Unlock()

// 	if reply.Term > rf.currentTerm {
// 		raftstate := rf.becomeFollowerLocked(reply.Term)
// 		if raftstate != nil {
// 			rf.mu.Unlock()
// 			rf.persistDirect(raftstate)
// 			rf.mu.Lock()
// 		}
// 		return
// 	}
// 	if rf.contextLostLocked(Leader, term) {
// 		LOG(rf.me, rf.currentTerm, DLog, "-> S%d, Context Lost, T%d:Leader->T%d:%s", peer, term, rf.currentTerm, rf.role)
// 		return
// 	}

// 	if !reply.Success {
// 		prevIndex := rf.nextIndex[peer]
// 		if reply.ConfilictTerm == InvalidTerm {
// 			rf.nextIndex[peer] = reply.ConfilictIndex
// 		} else {
// 			firstTermIndex := rf.log.firstFor(reply.ConfilictTerm)
// 			if firstTermIndex != InvalidTerm {
// 				rf.nextIndex[peer] = firstTermIndex + 1
// 			} else {
// 				rf.nextIndex[peer] = reply.ConfilictIndex
// 			}
// 		}
// 		if rf.nextIndex[peer] > prevIndex {
// 			rf.nextIndex[peer] = prevIndex
// 		}
// 		nextPrevIndex := rf.nextIndex[peer] - 1
// 		nextPrevTerm := InvalidTerm
// 		if nextPrevIndex >= rf.log.snapLastIdx {
// 			nextPrevTerm = rf.log.at(nextPrevIndex).Term
// 		}
// 		LOG(rf.me, rf.currentTerm, DLog, "-> S%d,Not match at Prev = [%d]T%d,Try Next Prev = [%d]T%d",
// 			peer, args.PrevLogIndex, args.PrevLogTerm, nextPrevIndex, nextPrevTerm)
// 		return
// 	}

// 	rf.matchIndex[peer] = args.PrevLogIndex + len(args.Entries)
// 	rf.nextIndex[peer] = rf.matchIndex[peer] + 1
// 	majorityMactched := rf.getMajorityIndexLocked()
// 	if majorityMactched > rf.commitIndex && rf.log.at(majorityMactched).Term == rf.currentTerm {
// 		LOG(rf.me, rf.currentTerm, DApply, "leader update commit index %d -> %d", rf.commitIndex, majorityMactched)
// 		rf.commitIndex = majorityMactched
// 		rf.applyCond.Signal()
// 	}
// }
