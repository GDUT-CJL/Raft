package raft

import (
	"context"
	"fmt"
	"math/rand"
	"net"
	"sync/atomic"
	"time"
)

// var LeaderId int //全局遍历记录LeaderId
func getLocalIP() (string, error) {
	// 获取所有网络接口
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return "", err
	}
	for _, addr := range addrs {
		// 检查是否为 IP 地址
		if ipnet, ok := addr.(*net.IPNet); ok {
			// 排除回环地址（如 127.0.0.1）和 IPv6 地址（可选）
			if !ipnet.IP.IsLoopback() && ipnet.IP.To4() != nil {
				//return ipnet.IP.String(), nil
				return ipnet.IP.String() + ":8000", nil
			}
		}
	}
	return "", fmt.Errorf("no local IP address found")
}

// 重置选举超时的时间
func (rf *Raft) resetElectionTimerLocked() {
	// 记录当前开始时的时间戳
	rf.electionStart = time.Now()
	// 随机出选举超时的时间断
	randRange := int64(MaxElectionTimeout - MinElectionTimeout)
	// 设置超时时间
	rf.electionTimout = MinElectionTimeout + time.Duration(rand.Int63()%randRange)
}

// 是否超时
func (rf *Raft) isElectionTimeOut() bool {
	// 从记录的rf.electionStart开始到现在的时间，是否大于规定的超时时间，如果是则反会true
	//fmt.Printf("TimeOut:%v,Now:%v\n", rf.electionTimout, time.Since(rf.electionStart))
	return time.Since(rf.electionStart) > rf.electionTimout
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (PartA, PartB).
	Term        int // 候选人任期号
	CandidateId int // 请求选票的候选人的 ID

	LastLogIndex int // 候选人的最后日志号
	LastLogTerm  int // 候选人最后日志的任期号
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (PartA).
	Term        int
	VoteGranted bool
}

// 对比两条日志哪个更新
/*	如果两份日志最后条目任期号不同，那么任期号大的日志更“新”；
	如果两份日志最后条目的任期号相同，那么日志较长（日志号更大）的那个更“新” */
func (rf *Raft) isMoreUpToDateLocked(candidateIndex, candidateTerm int) bool {
	l := rf.log.size()
	lastTerm, lastIndex := rf.log.at(l-1).Term, l-1
	LOG(rf.me, rf.currentTerm, DVote, "Compare last log, Me: [%d]T%d, Candidate: [%d]T%d", lastIndex, lastTerm, candidateIndex, candidateTerm)
	if lastTerm != candidateTerm {
		return lastTerm > candidateTerm
	}
	return lastIndex > candidateIndex
}

// example RequestVote RPC handler.
// RPC的回调函数，在sendRequestVote中会回调此函数
// args代表想要成为leader的那个节点，reply代表我的回应（即我是否会给他投票）
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// Your code here (PartA, PartB).
	reply.Term = rf.currentTerm
	reply.VoteGranted = false // 回复先默认为false，表示不同意
	// 如果想要成为leader的那个节点的任期比我的任期还小，那我不会给票，直接返回
	if args.Term < rf.currentTerm {
		LOG(rf.me, rf.currentTerm, DVote, "-> S%d,Reject voted,higher term,T%d>T%d", args.CandidateId, rf.currentTerm, args.Term)
		return
	}

	// 如果想要成为leader的那个节点比我的任期大，那我主动成为他的follower并更新我的任期为他的任期
	if args.Term > rf.currentTerm {
		rf.becomeFollowerLocked(args.Term) // 我变成follower并且任期改为args.Term
	}

	// 如果我已经投给其他节点了，“votedFor！=-1”表示我可能已经投给别人了，那我直接返回
	if rf.votedFor != -1 {
		LOG(rf.me, rf.currentTerm, DVote, "-> S%d,Reject voted,Already voted to S%d", args.CandidateId, rf.votedFor)
		return
	}

	// 检查候选者的日志是否更“新”
	if rf.isMoreUpToDateLocked(args.LastLogIndex, args.LastLogTerm) {
		LOG(rf.me, rf.currentTerm, DLog, "-> S%d,Reject Log,S%d`s log less up-to-date", args.CandidateId)
		return
	}

	// 如果我还没投票给任何其他节点，那我才投票给你
	rf.votedFor = args.CandidateId
	rf.persistLocked()
	reply.VoteGranted = true
	// 并且重置选举超时时间
	rf.resetElectionTimerLocked()
	LOG(rf.me, rf.currentTerm, DVote, "-> S%d,Vote granted", args.CandidateId)

}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	if rf.me == server {
		return false
	}

	// 内部raft参数转为grpc参数，便于节点通讯
	grpcArgs := &G_RequestVoteArgs{
		Term:         int32(args.Term),
		CandidateId:  int32(args.CandidateId),
		LastLogIndex: int32(args.LastLogIndex),
		LastLogTerm:  int32(args.LastLogTerm),
	}
	// 设置超时上下文
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
	fmt.Printf("节点 %d 开始选举,当前peers数量: %d\n", rf.me, len(rf.peers))

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
		rf.becomeFollowerLocked(reply.Term)
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

// 选举循环函数，这里只要有rf节点就一直触发循环
func (rf *Raft) electionticker() {
	// 循环
	for !rf.killed() {
		time.Sleep(50 * time.Millisecond) // 更频繁检查（50ms）

		rf.mu.Lock()
		// 重启保护期：前5秒不触发选举
		if time.Since(rf.restartTime) < 5*time.Second {
			rf.resetElectionTimerLocked()
			rf.mu.Unlock()
			continue
		}
		if rf.role != Leader && rf.isElectionTimeOut() { //如果当前节点不是leader并且选举已经超时
			rf.becomeCandidateLocked()          // 变为candidate状态
			go rf.startElection(rf.currentTerm) // 启动选举go程,任期为当前节点任期
		}
		rf.mu.Unlock()
		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 100 + (rand.Int63() % 400)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}
