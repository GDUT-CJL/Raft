package raft

import (
	"context"
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

// 转换函数：gRPC LogEntry → 内部 LogEntry（bytes版本）
func convertFromGrpcLogEntries(grpcEntries []*G_LogEntry) []LogEntry {
	if grpcEntries == nil {
		return nil
	}

	entries := make([]LogEntry, len(grpcEntries))
	for i, grpcEntry := range grpcEntries {
		var ops []Op

		// 检查 Command 是否为 nil
		if grpcEntry.Command != nil {
			for _, grpcOp := range grpcEntry.Command {
				// 将 []byte 转换为 string
				keyStr := ""
				if grpcOp.Key != nil {
					keyStr = string(grpcOp.Key)
				}

				valueStr := ""
				if grpcOp.Value != nil {
					valueStr = string(grpcOp.Value)
				}

				ops = append(ops, Op{
					ClientId: grpcOp.ClientId,
					SeqId:    grpcOp.SeqId,
					OpType:   OperationType(grpcOp.OpType),
					Key:      keyStr,
					Value:    valueStr,
					Klen:     int(grpcOp.Klen),
					Vlen:     int(grpcOp.Vlen),
				})
			}
		}

		entries[i] = LogEntry{
			Term:         int(grpcEntry.Term),
			CommandValid: grpcEntry.CommandValid,
			Command:      ops,
			CommandIndex: int(grpcEntry.CommandIndex),
		}
	}
	return entries
}

func (s *RaftServer) Grpc_AppendEntries(ctx context.Context, req *G_AppendEntriesArgs) (*G_AppendEntriesReply, error) {
	// 将 LeaderIP 从 []byte 转换为 string
	leaderIP := ""
	if req.LeaderIP != nil {
		leaderIP = string(req.LeaderIP)
	}

	args := &AppendEntriesArgs{
		Term:     int(req.Term),
		LeaderId: int(req.LeaderId),

		PrevLogIndex: int(req.PrevLogIndex),
		PrevLogTerm:  int(req.PrevLogTerm),
		Entries:      convertFromGrpcLogEntries(req.Entries),

		LeaderCommit: int(req.LeaderCommit),
		LeaderIP:     leaderIP,
	}

	reply := &AppendEntriesReply{}
	s.raftNode.AppendEntries(args, reply)

	return &G_AppendEntriesReply{
		Term:          int64(reply.Term),
		Success:       reply.Success,
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
