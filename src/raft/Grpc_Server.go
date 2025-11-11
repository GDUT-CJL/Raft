package raft

import (
	"bytes"
	"context"
	"course/labgob"
	"fmt"
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
		var cmd []interface{}
		if len(grpcEntry.Command) > 0 {
			// 使用 labgob 反序列化而不是 JSON
			r := bytes.NewReader(grpcEntry.Command)
			d := labgob.NewDecoder(r)
			if err := d.Decode(&cmd); err != nil {
				fmt.Printf("Failed to decode command: %v\n", err)
				continue
			}
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
		LeaderIP:     string(req.LeaderIP),
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
