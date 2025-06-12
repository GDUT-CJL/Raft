package raft

import (
	"context"
	"encoding/json"
)

type RaftServer struct {
	UnimplementedRaftGrpcServer
	raftNode *Raft
}

func NewServer(raftNode *Raft) *RaftServer {
	return &RaftServer{
		raftNode: raftNode,
	}
}

func (s *RaftServer) Grpc_RequestVote(ctx context.Context, req *G_RequestVoteArgs) (*G_RequestVoteReply, error) {
	// 构造raft层内置RequestVote的请求参数
	args := &RequestVoteArgs{
		Term:         int(req.Term),
		CandidateId:  int(req.CandidateId),
		LastLogIndex: int(req.LastLogIndex),
		LastLogTerm:  int(req.LastLogTerm),
	}
	reply := &RequestVoteReply{}
	// 调用raft内部封装好了的RequestVote
	s.raftNode.RequestVote(args, reply)
	return &G_RequestVoteReply{
		Term:        int32(reply.Term),
		VoteGranted: reply.VoteGranted,
	}, nil
}

// 转换函数：gRPC LogEntry → 内部 LogEntry
func convertFromGrpcLogEntries(grpcEntries []*G_LogEntry) []LogEntry {
	entries := make([]LogEntry, len(grpcEntries))
	for i, grpcEntry := range grpcEntries {
		// 反序列化 Command 后序再取出这条日志的时后还要序列化回来
		var cmd interface{}
		if len(grpcEntry.Command) > 0 {
			json.Unmarshal(grpcEntry.Command, &cmd)
		}
		entries[i] = LogEntry{
			Term:         int(grpcEntry.Term),
			CommandValid: grpcEntry.CommandValid,
			Command:      cmd,
		}
	}
	return entries
}
func (s *RaftServer) Grpc_AppendEntries(ctx context.Context, req *G_AppendEntriesArgs) (*G_AppendEntriesReply, error) {
	args := &AppendEntriesArgs{
		Term:     int(req.Term),
		LeaderId: int(req.LeaderId),

		PrevLogIndex: int(req.PrevLogIndex),
		PrevLogTerm:  int(req.PrevLogTerm),
		Entries:      convertFromGrpcLogEntries(req.Entries),

		LeaderCommit: int(req.LeaderCommit),
	}
	reply := &AppendEntriesReply{}
	s.raftNode.AppendEntries(args, reply)
	return &G_AppendEntriesReply{
		Term:    int64(reply.Term),
		Success: reply.Success,

		ConflictIndex: int64(reply.ConfilictIndex),
		ConflictTerm:  int64(reply.ConfilictTerm),
	}, nil
}

func (s *RaftServer) Grpc_InstallSnapshot(ctx context.Context, req *G_InstallSnapshotArgs) (*G_InstallSnapshotReply, error) {
	args := &InstallSnapshotArgs{
		Term:     int(req.Term),
		LeaderId: int(req.LeaderId),

		LastIncludedIndex: int(req.LastIncludedIndex),
		LastIncludedTerm:  int(req.LastIncludedTerm),

		Snapshot: req.Snapshot,
	}
	reply := &InstallSnapshotReply{}
	s.raftNode.InstallSnapshot(args, reply)
	return &G_InstallSnapshotReply{
		Term: int64(reply.Term),
	}, nil
}
