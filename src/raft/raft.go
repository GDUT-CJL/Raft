package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	//	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"course/labgob"
	"course/labrpc"
)
// 定义最短和最长的超时时间分别为 250ms和400ms
const(
	MinElectionTimeout time.Duration = 250 * time.Millisecond
	MaxElectionTimeout time.Duration = 400 * time.Millisecond

	replicateInterval time.Duration = 200 * time.Millisecond
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

// 定义三种状态
type Role string
const(
	Follower Role = "Follower"
	Candidate Role = "Candidate"
	Leader Role = "Leader"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part PartD you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For PartD:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (PartA, PartB, PartC).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	role 		Role			// 三种角色
	currentTerm int				// 当前任期
	votedFor 	int 			// 投票给谁

	electionStart time.Time		// 选举开始标志
	eletionTimout time.Duration	// 随机超时时间	

}

// 将调用此函数的节点转换状态成为follower，任期更改为传入参数term
func (rf *Raft)becomeFollowerLocked(term int){
	if rf.currentTerm > term{//如果当前任期大于term，说明我还不能成为follower，因为我的任期比较大
		LOG(rf.me,rf.currentTerm,DError,"Can not be a Follower,lower term:T%d",term)
		return
	}

	LOG(rf.me,rf.currentTerm,DVote,"%s -> Follower,For T%v->T%v",rf.role,rf.currentTerm,term)
	rf.role = Follower	// 否则我的任期不大于term，那我成为follower
	if rf.currentTerm < term{ // 如果是小于term
		rf.votedFor = -1 // 初始化我的票数为-1，表示我有选票还没投
	}
	rf.currentTerm = term // 将我当前的任期改为你的任期
}

// 将调用此函数的节点转换状态成为candidate
func (rf *Raft)becomeCandidateLocked(){
	if rf.role == Leader{	// 如果我已经是leader我就无法变为candidate
		LOG(rf.me,rf.currentTerm,DError ,"Leader can not be a Candidate")
		return
	}

	LOG(rf.me,rf.currentTerm,DVote,"%s->Candidate,For T%d",rf.role,rf.currentTerm+1)
	// 成为candidate，任期自增1，投票给自己
	rf.currentTerm++
	rf.role = Candidate
	rf.votedFor = rf.me

}

// 将调用此函数的节点转换状态成为leader
func (rf *Raft)becomeLeaderLocked(){
	if rf.role != Candidate{
		LOG(rf.me,rf.currentTerm,DError ,"Only Candidate can be a Leader")
		return 
	} 

	LOG(rf.me,rf.currentTerm,DVote,"%s->Leader,For T%d",rf.role,rf.currentTerm)
	rf.role = Leader//成为leader
}
// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	// var term int
	// var isleader bool
	// Your code here (PartA).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.role == Leader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (PartC).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (PartC).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (PartD).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (PartA, PartB).
	Term		int
	CandidateId	int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (PartA).
	Term		int
	VoteGranted	bool
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

	// 如果我还没投票给任何其他节点，那我才投票给你
	rf.votedFor = args.CandidateId
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

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (PartB).

	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}
// 状态一致性是实现高可用性和准确性的基础，只有在正确的一致性检查的条件下才可以继续执行代码
// 即在一个任期内，只要你的角色没有变化，就能放心地推进状态机。
func (rf* Raft)contextLostLocked(role Role,term int) bool{
	return !(rf.currentTerm == term && rf.role == role)
}

type AppendEntriesArgs struct{
	Term		int
	LeaderId	int
}

type AppendEntriesReply struct{
	Term		int
	Success		bool
}

func (rf *Raft)AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply){
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	reply.Success = false

	if args.Term < rf.currentTerm{
		LOG(rf.me, rf.currentTerm, DLog2, "<- S%d, Reject log", args.LeaderId)
		return
	}

	if args.Term >= rf.currentTerm{
		rf.becomeFollowerLocked(args.Term)
	}

	// 重置选举超时时间，表示我不再争取作为leader，因为你已经是我的leader
	rf.resetElectionTimerLocked()
	reply.Success = true
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}


// 启动日志复制和心跳
func (rf *Raft)startReplication(term int) bool{
	replicateToPeer:=func(peer int,args *AppendEntriesArgs){
		 reply := &AppendEntriesReply{}
		ok:=rf.sendAppendEntries(peer,args,reply)
		if !ok{
			LOG(rf.me, rf.currentTerm, DLog, "-> S%d, Lost or crashed", peer)
			return
		}

		// 如果回复我的节点的任期比我的任期还大，那么我变为follower
		if reply.Term > rf.currentTerm{
			rf.becomeFollowerLocked(reply.Term)
			return
		}
		

	}
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.contextLostLocked(Leader,term){
		LOG(rf.me,rf.currentTerm,DLog,"Lost Leader[%d] to %s[T%d]",term,rf.role,rf.currentTerm)
		return false
	}
	for peer:=0;peer < len(rf.peers);peer++{
		if peer == rf.me{
			continue
		}

		args:= &AppendEntriesArgs{
			Term:term,
			LeaderId:rf.me,
		}

		go replicateToPeer(peer,args)
	}
	return true
}

// 只有在某个term内循环，当这个任期结束则心跳结束
func (rf* Raft)replicationTicker(term int){
	for !rf.killed(){
		ok := rf.startReplication(term)
		if !ok{
			break
		}

		time.Sleep(replicateInterval)
	}
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
	for peer := 0; peer < len(rf.peers); peer++{// 对于每一个rpc节点遍历
		if peer == rf.me{//如果是自己的话，就选票加1
			vote++
			continue
		}
		// 否则发送RPC请求，询问是否给我当前节点选票
		args := &RequestVoteArgs{
			Term:rf.currentTerm,
			CandidateId:rf.me,
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

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (PartA, PartB, PartC).
	rf.role = Follower
	rf.currentTerm = 0
	rf.votedFor = -1
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.electionticker() // 每创建一个raft就可以一直循环触发选举操作

	return rf
}
