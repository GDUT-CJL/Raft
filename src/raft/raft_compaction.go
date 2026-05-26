package raft

import (
	"context"
	"fmt"
	"time"
)

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
// 应用层调用该接口，传入index即从哪里开始做快照已经一个byte的切片
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (PartD).
	rf.mu.Lock()
	if index > rf.commitIndex {
		LOG(rf.me, rf.currentTerm, DSnap, "Couldn't snapshot before CommitIdx: %d>%d", index, rf.commitIndex)
		rf.mu.Unlock()
		return
	}
	if index <= rf.log.snapLastIdx {
		LOG(rf.me, rf.currentTerm, DSnap, "Already snapshot in %d<=%d", index, rf.log.snapLastIdx)
		rf.mu.Unlock()
		return
	}
	rf.log.doSnapshot(index, snapshot)
	// 在锁内只序列化 raftstate（不拷贝快照），释放锁后再做耗时的 WAL 压缩
	raftstate := rf.prepareCompactData()
	rf.mu.Unlock()

	// 在锁外执行 WAL 压缩（涉及 fsync、文件 IO，可能耗时数十毫秒）
	// 快照数据直接使用参数传入，无需额外拷贝
	rf.persistAndCompactAsync(raftstate, snapshot)
}

type InstallSnapshotArgs struct {
	Term     int
	LeaderId int

	LastIncludedIndex int
	LastIncludedTerm  int

	Snapshot []byte
}

func (args *InstallSnapshotArgs) String() string {
	return fmt.Sprintf("Leader-%d,T%d,Last:[%d]T%d", args.LeaderId, args.Term, args.LastIncludedIndex, args.LastIncludedTerm)
}

// 为了对齐任期，只需要一个字段
type InstallSnapshotReply struct {
	Term int
}

func (reply *InstallSnapshotReply) String() string {
	return fmt.Sprintf("T %d", reply.Term)
}

// follower
func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	LOG(rf.me, rf.currentTerm, DDebug, "<- S%d,RecvSnapShot,Args= %v", args.LeaderId, args.String())

	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		LOG(rf.me, rf.currentTerm, DSnap, "-> S%d,Reject voted,higher term,T%d>T%d", args.LeaderId, rf.currentTerm, args.Term)
		rf.mu.Unlock()
		return
	}

	if args.Term > rf.currentTerm {
		raftstate := rf.becomeFollowerLocked(args.Term)
		if raftstate != nil {
			rf.mu.Unlock()
			rf.persistDirect(raftstate)
			rf.mu.Lock()
		}
	}

	// 如果本地snapshot已经包含了leader的snapshot，那么就无需更新
	if rf.log.snapLastIdx >= args.LastIncludedIndex {
		LOG(rf.me, rf.currentTerm, DSnap, "<- S%d,Reject Snap,Already installed %d>%d", args.LeaderId, rf.log.snapLastIdx, args.LastIncludedIndex)
		rf.mu.Unlock()
		return
	}

	// 将快照从leader中同步并应用到各个peer的状态机中
	rf.log.installSnapshot(args.LastIncludedIndex, args.LastIncludedTerm, args.Snapshot)
	// 在锁内只序列化 raftstate（不拷贝快照），释放锁后再做耗时的 WAL 压缩
	raftstate := rf.prepareCompactData()
	// 保存快照引用用于锁外持久化（args.Snapshot 在 RPC 期间不会被修改）
	snapshotData := args.Snapshot
	rf.snapAppending = true
	rf.applyCond.Signal()
	rf.mu.Unlock()

	// 在锁外执行 WAL 压缩（涉及 fsync、文件 IO，可能耗时数十毫秒）
	rf.persistAndCompactAsync(raftstate, snapshotData)
}

// leader
func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	if rf.me == server {
		return false
	}
	grpcArgs := &G_InstallSnapshotArgs{
		Term:     int64(args.Term),
		LeaderId: int64(args.LeaderId),

		LastIncludedIndex: int64(args.LastIncludedIndex),
		LastIncludedTerm:  int64(args.LastIncludedTerm),

		Snapshot: args.Snapshot,
	}
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	grpcReply, err := rf.peers[server].Grpc_InstallSnapshot(ctx, grpcArgs)
	if err != nil {
		return false
	}
	reply.Term = int(grpcReply.Term)
	return true
}

func (rf *Raft) InstallToPeer(peer, term int, args *InstallSnapshotArgs) {
	reply := &InstallSnapshotReply{}
	// 调用sendRequestVote RPC请求进行索票
	ok := rf.sendInstallSnapshot(peer, args, reply)

	rf.mu.Lock()
	defer rf.mu.Unlock()
	// 无论成功失败，清除快照发送标记
	rf.snapSending[peer] = false

	if !ok { // 如果返回错误，说明索票直接失败，返回
		LOG(rf.me, rf.currentTerm, DDebug, "Ask vote from S%d,Lost or error", peer)
		return
	}

	// 任期对齐
	if reply.Term > rf.currentTerm { // 如果回复的票数比我当前想要成为leader的任期还大
		raftstate := rf.becomeFollowerLocked(reply.Term) // 则我自己就取消作为leader，变为follower且将自己任期改为reply.Term
		if raftstate != nil {
			rf.mu.Unlock()
			rf.persistDirect(raftstate)
			rf.mu.Lock()
		}
		return //返回
	}

	// 更新对应的matchIndex和nextIndex,且要保证单调
	if args.LastIncludedIndex > rf.matchIndex[peer] {
		rf.matchIndex[peer] = args.LastIncludedIndex
		rf.nextIndex[peer] = args.LastIncludedIndex + 1
	}

	// 这里不需要更新各个peer的commitIndex，因为快照的日志已经是提交了的
}
