package raft

import (
	"context"
	"fmt"
	"time"
)

func installSnapshotTimeout(snapshotSize int) time.Duration {
	timeout := 5 * time.Second
	if snapshotSize <= 0 {
		return timeout
	}

	timeout += time.Duration(snapshotSize/(256*1024)) * time.Second
	if timeout > 2*time.Minute {
		return 2 * time.Minute
	}
	return timeout
}

func (rf *Raft) Snapshot(index int, snapshot []byte) {
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
	raftstate := rf.prepareCompactData()
	rf.mu.Unlock()

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

type InstallSnapshotReply struct {
	Term int
}

func (reply *InstallSnapshotReply) String() string {
	return fmt.Sprintf("T %d", reply.Term)
}

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
		shouldPersist := rf.becomeFollowerLocked(args.Term)
		if shouldPersist {
			currentTerm := rf.currentTerm
			votedFor := rf.votedFor
			rf.mu.Unlock()
			rf.persistMeta(currentTerm, votedFor)
			rf.mu.Lock()
		}
	}

	if rf.log.snapLastIdx >= args.LastIncludedIndex {
		LOG(rf.me, rf.currentTerm, DSnap, "<- S%d,Reject Snap,Already installed %d>%d", args.LeaderId, rf.log.snapLastIdx, args.LastIncludedIndex)
		rf.mu.Unlock()
		return
	}

	rf.log.installSnapshot(args.LastIncludedIndex, args.LastIncludedTerm, args.Snapshot)
	raftstate := rf.prepareCompactData()
	rf.snapAppending = true
	rf.applyCond.Signal()
	rf.mu.Unlock()

	rf.persistAndCompactAsync(raftstate, args.Snapshot)
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	if rf.me == server {
		return false
	}
	grpcArgs := &G_InstallSnapshotArgs{
		Term:     int64(args.Term),
		LeaderId: int64(args.LeaderId),

		LastIncludedIndex: int64(args.LastIncludedIndex),
		LastIncludedTerm:  int64(args.LastIncludedTerm),
		Snapshot:          args.Snapshot,
	}
	ctx, cancel := context.WithTimeout(context.Background(), installSnapshotTimeout(len(args.Snapshot)))
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
	ok := rf.sendInstallSnapshot(peer, args, reply)

	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.snapSending[peer] = false

	if !ok {
		LOG(rf.me, rf.currentTerm, DDebug, "Ask vote from S%d,Lost or error", peer)
		return
	}

	if reply.Term > rf.currentTerm {
		shouldPersist := rf.becomeFollowerLocked(reply.Term)
		if shouldPersist {
			currentTerm := rf.currentTerm
			votedFor := rf.votedFor
			rf.mu.Unlock()
			rf.persistMeta(currentTerm, votedFor)
			rf.mu.Lock()
		}
		return
	}

	if args.LastIncludedIndex > rf.matchIndex[peer] {
		rf.matchIndex[peer] = args.LastIncludedIndex
		rf.nextIndex[peer] = args.LastIncludedIndex + 1
	}
}
