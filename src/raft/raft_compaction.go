package raft

import (
	"fmt"
)
// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
// 应用层调用该接口，传入index即从哪里开始做快照已经一个byte的切片
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (PartD).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if index > rf.commitIndex {
		LOG(rf.me, rf.currentTerm, DSnap, "Couldn't snapshot before CommitIdx: %d>%d", index, rf.commitIndex)
		return
	}
	if index <= rf.log.snapLastIdx {
		LOG(rf.me, rf.currentTerm, DSnap, "Already snapshot in %d<=%d", index, rf.log.snapLastIdx)
		return
	}
	rf.log.doSnapshot(index,snapshot)
	rf.persistLocked()
}


type InstallSnapshotArgs struct{
	Term	 	int
	LeaderId	int

	LastIncludedIndex	int
	LastIncludedTerm	int

	Snapshot	[]byte 
}

func (args *InstallSnapshotArgs) String()string{
	return fmt.Sprintf("Leader-%d,T%d,Last:[%d]T%d",args.LeaderId,args.Term,args.LastIncludedIndex,args.LastIncludedTerm)
}

// 为了对齐任期，只需要一个字段
type InstallSnapshotReply struct{
	Term	 	int
}

func (reply *InstallSnapshotReply) String()string{
	return fmt.Sprintf("T %d",reply.Term)
}

// follower
func (rf *Raft)InstallSnapshot(args *InstallSnapshotArgs,reply *InstallSnapshotReply){
	rf.mu.Lock()
	defer rf.mu.Unlock()
	LOG(rf.me,rf.currentTerm,DDebug,"<- S%d,RecvSnapShot,Args= %v",args.LeaderId,args.String())

	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm{
		LOG(rf.me,rf.currentTerm,DSnap,"-> S%d,Reject voted,higher term,T%d>T%d",args.LeaderId,rf.currentTerm,args.Term)
		return
	}

	if args.Term > rf.currentTerm{
		rf.becomeFollowerLocked(args.Term)
	}

	// 如果本地snapshot已经包含了leader的snapshot，那么就无需更新
	if rf.log.snapLastIdx >= args.LastIncludedIndex{
		LOG(rf.me,rf.currentTerm,DSnap,"<- S%d,Reject Snap,Already installed %d>%d",args.LeaderId,rf.log.snapLastIdx,args.LastIncludedIndex)
		return
	}

	// 将快照从leader中同步并应用到各个peer的状态机中
	rf.log.installSnapshot(args.LastIncludedIndex,args.LastIncludedTerm,args.Snapshot)
	rf.persistLocked()
	rf.snapAppending = true
	rf.applyCond.Signal()
}

// leader
func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

func (rf *Raft)InstallToPeer(peer,term int,args *InstallSnapshotArgs){
	reply := &InstallSnapshotReply{}
	// 调用sendRequestVote RPC请求进行索票
	ok := rf.sendInstallSnapshot(peer,args,reply)
	
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if !ok{// 如果返回错误，说明索票直接失败，返回
		LOG(rf.me,rf.currentTerm,DDebug,"Ask vote from S%d,Lost or error",peer)
		return
	}

	// 任期对齐
	if reply.Term > rf.currentTerm{// 如果回复的票数比我当前想要成为leader的任期还大
		rf.becomeFollowerLocked(reply.Term)// 则我自己就取消作为leader，变为follower且将自己任期改为reply.Term
		return //返回
	}

	// 检查自己的上下文状态是否变化
	// if rf.contextLostLocked(Candidate,rf.currentTerm){
	// 	LOG(rf.me,rf.currentTerm,DVote,"Lost context, abort RequestVoteReply for S%d",peer)
	// 	return
	// }

	// 更新对应的matchIndex和nextIndex,且要保证单调
	if args.LastIncludedIndex > rf.matchIndex[peer]{
		rf.matchIndex[peer] = args.LastIncludedIndex
		rf.nextIndex[peer] = args.LastIncludedIndex + 1
	}

	// 这里不需要更新各个peer的commitIndex，因为快照的日志已经是提交了的
}