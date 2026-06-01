package raft

import (
	"context"
	"fmt"
	"math/rand"
	"net"
	"sync/atomic"
	"time"
)

func getLocalIP() (string, error) {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return "", err
	}
	for _, addr := range addrs {
		if ipnet, ok := addr.(*net.IPNet); ok {
			if !ipnet.IP.IsLoopback() && ipnet.IP.To4() != nil {
				return ipnet.IP.String() + ":8000", nil
			}
		}
	}
	return "", fmt.Errorf("no local IP address found")
}

func (rf *Raft) resetElectionTimerLocked() {
	rf.electionStart = time.Now()
	randRange := int64(MaxElectionTimeout - MinElectionTimeout)
	rf.electionTimout = MinElectionTimeout + time.Duration(rand.Int63()%randRange)
}

func (rf *Raft) isElectionTimeOut() bool {
	return time.Since(rf.electionStart) > rf.electionTimout
}

type RequestVoteArgs struct {
	Term        int
	CandidateId int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

func (rf *Raft) isMoreUpToDateLocked(candidateIndex, candidateTerm int) bool {
	l := rf.log.size()
	lastTerm, lastIndex := rf.log.at(l-1).Term, l-1
	LOG(rf.me, rf.currentTerm, DVote, "Compare last log, Me: [%d]T%d, Candidate: [%d]T%d", lastIndex, lastTerm, candidateIndex, candidateTerm)
	if lastTerm != candidateTerm {
		return lastTerm > candidateTerm
	}
	return lastIndex > candidateIndex
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	if args.Term < rf.currentTerm {
		LOG(rf.me, rf.currentTerm, DVote, "-> S%d,Reject voted,higher term,T%d>T%d", args.CandidateId, rf.currentTerm, args.Term)
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

	if rf.votedFor != -1 {
		LOG(rf.me, rf.currentTerm, DVote, "-> S%d,Reject voted,Already voted to S%d", args.CandidateId, rf.votedFor)
		rf.mu.Unlock()
		return
	}

	if rf.isMoreUpToDateLocked(args.LastLogIndex, args.LastLogTerm) {
		LOG(rf.me, rf.currentTerm, DLog, "-> S%d,Reject Log,S%d`s log less up-to-date", args.CandidateId)
		rf.mu.Unlock()
		return
	}

	rf.votedFor = args.CandidateId
	reply.VoteGranted = true
	rf.resetElectionTimerLocked()
	currentTerm := rf.currentTerm
	votedFor := rf.votedFor
	LOG(rf.me, rf.currentTerm, DVote, "-> S%d,Vote granted", args.CandidateId)
	rf.mu.Unlock()

	rf.persistMeta(currentTerm, votedFor)
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	if rf.me == server {
		return false
	}

	grpcArgs := &G_RequestVoteArgs{
		Term:         int32(args.Term),
		CandidateId:  int32(args.CandidateId),
		LastLogIndex: int32(args.LastLogIndex),
		LastLogTerm:  int32(args.LastLogTerm),
	}
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	grpcReply, err := rf.peers[server].Grpc_RequestVote(ctx, grpcArgs)
	if err != nil {
		return false
	}
	reply.Term = int(grpcReply.Term)
	reply.VoteGranted = grpcReply.VoteGranted
	return true
}

func (rf *Raft) startElection(term int) {
	rf.mu.Lock()
	if rf.contextLostLocked(Candidate, term) {
		LOG(rf.me, rf.currentTerm, DVote, "Lost Candidate to %s, abort RequestVote", rf.role)
		rf.mu.Unlock()
		return
	}

	l := rf.log.size()
	lastLogTerm := rf.log.at(l - 1).Term
	rf.mu.Unlock()

	var voteCount int32 = 1
	votesNeeded := int32(len(rf.peers)/2 + 1)

	args := &RequestVoteArgs{
		Term:         term,
		CandidateId:  rf.me,
		LastLogIndex: l - 1,
		LastLogTerm:  lastLogTerm,
	}

	for peer := 0; peer < len(rf.peers); peer++ {
		if peer == rf.me {
			continue
		}
		go rf.askVoteFromPeer(peer, args, term, &voteCount, votesNeeded)
	}
}

func (rf *Raft) askVoteFromPeer(peer int, args *RequestVoteArgs, term int, voteCount *int32, votesNeeded int32) {
	reply := &RequestVoteReply{}
	ok := rf.sendRequestVote(peer, args, reply)
	if !ok {
		LOG(rf.me, rf.currentTerm, DDebug, "Ask vote from S%d,Lost or error", peer)
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

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

	if rf.contextLostLocked(Candidate, term) {
		LOG(rf.me, rf.currentTerm, DVote, "Lost context, abort RequestVoteReply for S%d", peer)
		return
	}

	if reply.VoteGranted {
		newCount := atomic.AddInt32(voteCount, 1)
		if newCount == votesNeeded {
			rf.becomeLeaderLocked()
			ip, err := getLocalIP()
			if err != nil {
				fmt.Println("Error:", err)
				return
			}
			rf.LeaderIP = ip
			fmt.Printf(" Node %d become leader IP:%s\n", rf.me, rf.LeaderIP)
		}
	}
}

func (rf *Raft) electionticker() {
	for !rf.killed() {
		time.Sleep(50 * time.Millisecond)
		rf.mu.Lock()
		if time.Since(rf.restartTime) < 5*time.Second {
			rf.resetElectionTimerLocked()
			rf.mu.Unlock()
			continue
		}
		if rf.role != Leader && rf.isElectionTimeOut() {
			shouldPersist := rf.becomeCandidateLocked()
			term := rf.currentTerm
			votedFor := rf.votedFor
			go rf.startElection(rf.currentTerm)
			rf.mu.Unlock()
			if shouldPersist {
				rf.persistMeta(term, votedFor)
			}
		} else {
			rf.mu.Unlock()
		}
		ms := 100 + (rand.Int63() % 400)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}
