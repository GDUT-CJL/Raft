package raft

import (
	"math/rand"
	"time"
)


// 重置选举超时的时间
func (rf *Raft) resetElectionTimerLocked(){
	// 记录当前开始时的时间戳
	rf.electionStart = time.Now()
	// 随机出选举超时的时间断
	randRange := int64(MaxElectionTimeout - MinElectionTimeout)
	// 设置超时时间
	rf.eletionTimout = MinElectionTimeout + time.Duration(rand.Int63()%randRange)
}

// 是否超时
func (rf *Raft) isElectionTimeOut() bool{
	// 从记录的rf.electionStart开始到现在的时间，是否大于规定的超时时间，如果是则反会true
	return time.Since(rf.electionStart) > rf.eletionTimout
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (PartA, PartB).
	Term			int		// 候选人任期号
	CandidateId		int		// 请求选票的候选人的 ID

	LastLogIndex		int	// 候选人的最后日志号
	LastLogTerm			int	// 候选人最后日志的任期号
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (PartA).
	Term		int
	VoteGranted	bool
}
// 对比两条日志哪个更新
/*	如果两份日志最后条目任期号不同，那么任期号大的日志更“新”；
	如果两份日志最后条目的任期号相同，那么日志较长（日志号更大）的那个更“新” */
func (rf *Raft) isMoreUpToDateLocked(candidateIndex, candidateTerm int) bool {
    l := len(rf.log)
    lastTerm, lastIndex := rf.log[l-1].Term, l-1
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
	if args.Term < rf.currentTerm{
		LOG(rf.me,rf.currentTerm,DVote,"-> S%d,Reject voted,higher term,T%d>T%d",args.CandidateId,rf.currentTerm,args.Term)
		return
	}

	// 如果想要成为leader的那个节点比我的任期大，那我主动成为他的follower并更新我的任期为他的任期
	if args.Term > rf.currentTerm{
		rf.becomeFollowerLocked(args.Term)// 我变成follower并且任期改为args.Term
	}

	// 如果我已经投给其他节点了，“votedFor！=-1”表示我可能已经投给别人了，那我直接返回
	if rf.votedFor != -1{
		LOG(rf.me,rf.currentTerm,DVote,"-> S%d,Reject voted,Already voted to S%d",args.CandidateId,rf.votedFor)
		return
	}

	// 检查候选者的日志是否更“新”
	if rf.isMoreUpToDateLocked(args.LastLogIndex,args.LastLogTerm){
		LOG(rf.me,rf.currentTerm,DLog,"-> S%d,Reject Log,S%d`s log less up-to-date",args.CandidateId)
		return
	}

	// 如果我还没投票给任何其他节点，那我才投票给你
	rf.votedFor = args.CandidateId
	rf.persistLocked()
	reply.VoteGranted = true
	// 并且重置选举超时时间
	rf.resetElectionTimerLocked()
	LOG(rf.me,rf.currentTerm,DVote,"-> S%d,Vote granted",args.CandidateId)

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
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// 启动选举go程函数
func (rf *Raft)startElection(term int){
	vote := 0
	askVoteFromPeer := func(peer int,args *RequestVoteArgs){
		reply := &RequestVoteReply{}
		// 调用sendRequestVote RPC请求进行索票
		ok := rf.sendRequestVote(peer,args,reply)
		
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
		if rf.contextLostLocked(Candidate,rf.currentTerm){
			LOG(rf.me,rf.currentTerm,DVote,"Lost context, abort RequestVoteReply for S%d",peer)
			return
		}
		// 如果回复我的票表示同意给我选票
		if reply.VoteGranted{
			vote++// 则票数自增1
			if vote > len(rf.peers)/2{//如果我的票数大于一半以上的同意票
				rf.becomeLeaderLocked()// 那么我成为leader
				// 发起心跳和日志同步，通知其他成员我已经是leader并开始复制我的日志
				go rf.replicationTicker(term)
			}
		}
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// 检查一致性，是否为Candidate状态，和任期是否有改变
	if rf.contextLostLocked(Candidate,term){
		LOG(rf.me,rf.currentTerm,DVote,"Lost Candidate to %s, abort RequestVote",rf.role)
		return
	}
	l := len(rf.log)
	for peer := 0; peer < len(rf.peers); peer++{// 对于每一个rpc节点遍历
		if peer == rf.me{//如果是自己的话，就选票加1
			vote++
			continue
		}
		// 否则发送RPC请求，询问是否给我当前节点选票
		args := &RequestVoteArgs{
			Term:rf.currentTerm,
			CandidateId:rf.me,
			LastLogIndex:l-1,
			LastLogTerm:rf.log[l-1].Term,
		}
		// 另起go程向其他RPC节点要票
		go askVoteFromPeer(peer,args)
	}
}
// 选举循环函数，这里只要有rf节点就一直触发循环
func (rf *Raft) electionticker() {
	// 循环
	for !rf.killed() {

		// Your code here (PartA)
		// Check if a leader election should be started.
		rf.mu.Lock()
		if rf.role != Leader && rf.isElectionTimeOut(){//如果当前节点不是leader并且选举已经超时
			rf.becomeCandidateLocked()// 变为candidate状态
			go rf.startElection(rf.currentTerm)// 启动选举go程,任期为当前节点任期
		}
		rf.mu.Unlock()
		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 50 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}