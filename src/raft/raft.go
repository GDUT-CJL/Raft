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
	"fmt"
	"log"
	"net"
	//"os"
	"bytes"
	"course/labgob"
	"sync"
	"sync/atomic"
	"time"
	// 替换为实际的 proto 包路径

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// 定义最短和最长的超时时间分别为 250ms和400ms
const (
	MinElectionTimeout time.Duration = 500 * time.Millisecond
	MaxElectionTimeout time.Duration = 1500 * time.Millisecond

	replicateInterval time.Duration = 30 * time.Millisecond
)

const (
	InvalidIndex int = 0 // 空的日志号
	InvalidTerm  int = 0 // 空的任期号
)

// 定义三种状态
type Role string

const (
	Follower  Role = "Follower"
	Candidate Role = "Candidate"
	Leader    Role = "Leader"
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
	CommandValid bool // 用于区分 普通日志命令 和 其他类型的消息（如快照）。
	Command      []Op
	CommandIndex int

	// For PartD:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.RWMutex       // Lock to protect shared access to this peer's state
	peers     []RaftGrpcClient   // gRPC 客户端列表
	conns     []*grpc.ClientConn // 保存 gRPC 连接
	persister *Persister         // Object to hold this peer's persisted state
	me        int                // this peer's index into peers[]
	dead      int32              // set by Kill()

	// Your data here (PartA, PartB, PartC).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	role        Role // 三种角色
	currentTerm int  // 当前任期
	votedFor    int  // 投票给谁

	electionStart  time.Time     // 选举开始标志
	electionTimout time.Duration // 随机超时时间

	log *RaftLog // 日志
	// only used when it is Leader,
	// log view for each peer

	// 本质上来说，下面这两个字段是各个 Peer 中日志进度在 Leader 中的一个视图（view）。
	// Leader 正是依据此视图来决定给各个 Peer 发送多少日志。也是依据此视图，Leader 可以计算全局的 `commitIndex`。
	nextIndex  []int // 发送到该服务器的下一个日志号（初始值为领导人最后的日志号+1）
	matchIndex []int // 记录了 Leader 已知的每个 follower 已经复制的最高日志索引（初始值为0）

	commitIndex   int           // 已知已提交的最高的日志条目的索引（初始值为0，单调递增）
	lastApplied   int           // 已经被应用到状态机的最高的日志条目的索引（初始值为0，单调递增）
	applyCond     *sync.Cond    // 唤醒提交日志索引更新
	applyCh       chan ApplyMsg //将 applyMsg 通过构造 Peer 时传进来的 channel 返回给应用层，即上层模块（如kv数据库）与当前的raft层的联系
	snapAppending bool

	LeaderIP string
	//restartProtected bool       // 重启保护标志
	restartTime time.Time // 重启时间
	perfStats   struct {
		mu             sync.RWMutex
		appendRpcCount int64     // 真正的日志复制次数
		appendRpcTime  int64     // 日志复制总耗时
		heartbeatCount int64     // 心跳次数
		heartbeatTime  int64     // 心跳总耗时
		lastResetTime  time.Time // 上次重置时间
	}
}

// 性能统计数据结构
type PerformanceStats struct {
	AppendRpcCount      int64   `json:"append_rpc_count"`
	AppendRpcAvgTime    float64 `json:"append_rpc_avg_time_ms"`
	HeartbeatCount      int64   `json:"heartbeat_count"`
	HeartbeatAvgTime    float64 `json:"heartbeat_avg_time_ms"`
	TotalRpcTime        int64   `json:"total_rpc_time_ms"`
	MeasurementDuration string  `json:"measurement_duration"`
}

// 获取性能统计
func (rf *Raft) GetPerformanceStats() PerformanceStats {
	appendCount := atomic.LoadInt64(&rf.perfStats.appendRpcCount)
	appendTime := atomic.LoadInt64(&rf.perfStats.appendRpcTime)
	heartbeatCount := atomic.LoadInt64(&rf.perfStats.heartbeatCount)
	heartbeatTime := atomic.LoadInt64(&rf.perfStats.heartbeatTime)
	var appendAvg float64
	var heartbeatAvg float64
	if appendCount > 0 {
		appendAvg = float64(appendTime) / float64(appendCount)
	}
	if heartbeatCount > 0 {
		heartbeatAvg = float64(heartbeatTime) / float64(heartbeatCount)

	}
	return PerformanceStats{
		AppendRpcCount:      appendCount,
		AppendRpcAvgTime:    appendAvg,
		HeartbeatCount:      heartbeatCount,
		HeartbeatAvgTime:    heartbeatAvg,
		TotalRpcTime:        appendTime,
		MeasurementDuration: time.Since(rf.perfStats.lastResetTime).String(),
	}
}

// 重置性能统计
func (rf *Raft) ResetPerformanceStats() {
	atomic.StoreInt64(&rf.perfStats.appendRpcCount, 0)
	atomic.StoreInt64(&rf.perfStats.appendRpcTime, 0)
	rf.perfStats.lastResetTime = time.Now()
}

// 获取详细的Raft状态信息
func (rf *Raft) GetDetailedState() map[string]interface{} {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	state := make(map[string]interface{})
	state["role"] = string(rf.role)
	state["currentTerm"] = rf.currentTerm
	state["votedFor"] = rf.votedFor
	state["commitIndex"] = rf.commitIndex
	state["lastApplied"] = rf.lastApplied
	state["logSize"] = rf.log.size()
	state["snapLastIdx"] = rf.log.snapLastIdx
	state["snapLastTerm"] = rf.log.snapLastTerm

	// 复制进度信息
	nextIndex := make(map[int]int)
	matchIndex := make(map[int]int)
	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			nextIndex[i] = rf.nextIndex[i]
			matchIndex[i] = rf.matchIndex[i]
		}
	}
	state["nextIndex"] = nextIndex
	state["matchIndex"] = matchIndex

	// 性能统计
	stats := rf.GetPerformanceStats()
	state["performance"] = stats

	return state
}

// 获取日志一致性状态
func (rf *Raft) GetLogConsistencyState() map[string]interface{} {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	state := make(map[string]interface{})
	state["logSize"] = rf.log.size()
	state["commitIndex"] = rf.commitIndex
	state["lastApplied"] = rf.lastApplied

	peerStates := make(map[int]map[string]int)
	for peer := 0; peer < len(rf.peers); peer++ {
		if peer != rf.me {
			peerStates[peer] = map[string]int{
				"nextIndex":  rf.nextIndex[peer],
				"matchIndex": rf.matchIndex[peer],
			}
		}
	}
	state["peerStates"] = peerStates

	return state
}
func (rf *Raft) GetPeerLen() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return len(rf.peers)
}
func (rf *Raft) GetRaftStateSize() int {
	// 方案1：直接查询文件（最准确）
	fileSize := rf.persister.RaftStateSize()

	// 如果文件不存在或大小为0，使用内存中的估算
	if fileSize == 0 {
		rf.mu.Lock()
		defer rf.mu.Unlock()

		// 估算当前状态大小
		w := new(bytes.Buffer)
		e := labgob.NewEncoder(w)
		e.Encode(rf.currentTerm)
		e.Encode(rf.votedFor)
		rf.log.persisted(e)
		return w.Len()
	}

	return fileSize
}

// 将调用此函数的节点转换状态成为follower，任期更改为传入参数term
func (rf *Raft) becomeFollowerLocked(term int) {
	if rf.currentTerm > term { //如果当前任期大于term，说明我还不能成为follower，因为我的任期比较大
		LOG(rf.me, rf.currentTerm, DError, "Can not be a Follower,lower term:T%d", term)
		return
	}

	LOG(rf.me, rf.currentTerm, DVote, "%s -> Follower,For T%v->T%v", rf.role, rf.currentTerm, term)
	rf.role = Follower // 否则我的任期不大于term，那我成为follower
	shouldPersist := rf.currentTerm != term
	if rf.currentTerm < term { // 如果是小于term
		rf.votedFor = -1 // 初始化我的票数为-1，表示我有选票还没投
	}
	rf.currentTerm = term // 将我当前的任期改为你的任期
	if shouldPersist {
		rf.persistLocked()
	}
	rf.resetElectionTimerLocked()
}

// 将调用此函数的节点转换状态成为candidate
func (rf *Raft) becomeCandidateLocked() {
	if rf.role == Leader { // 如果我已经是leader我就无法变为candidate
		LOG(rf.me, rf.currentTerm, DError, "Leader can not be a Candidate")
		return
	}

	LOG(rf.me, rf.currentTerm, DVote, "%s->Candidate,For T%d", rf.role, rf.currentTerm+1)
	// 成为candidate，任期自增1，投票给自己
	rf.currentTerm++
	// if rf.role == Follower {
	// 	rf.currentTerm++
	// }
	rf.role = Candidate
	rf.votedFor = rf.me
	rf.persistLocked()
	rf.resetElectionTimerLocked() // 重置选举超时时间
}

// 将调用此函数的节点转换状态成为leader
func (rf *Raft) becomeLeaderLocked() {
	if rf.role != Candidate {
		LOG(rf.me, rf.currentTerm, DError, "Only Candidate can be a Leader")
		return
	}
	// 成为leader后需初始化nextIndex和matchIndex
	LOG(rf.me, rf.currentTerm, DVote, "%s->Leader,For T%d", rf.role, rf.currentTerm)
	rf.role = Leader //成为leader
	for peer := 0; peer < len(rf.peers); peer++ {
		rf.nextIndex[peer] = rf.log.size() // 初始化为日志长度，有效索引其实是 0 - (len -1),所以下一个是len
		rf.matchIndex[peer] = 0            //先初始化为0
	}
	// 启动独立的心跳协程
	go rf.heartbeatTicker(rf.currentTerm)
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	// var term int
	// var isleader bool
	// Your code here (PartA).
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	return rf.currentTerm, rf.role == Leader
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
func (rf *Raft) Start(command []Op) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 只有leader节点才能够操作日志，无论是set或者get等其他操作都必须是leader操作
	if rf.role != Leader {
		return 0, 0, false
	}

	rf.log.append(LogEntry{
		CommandValid: true,
		Command:      command,
		Term:         rf.currentTerm,
	})

	// Your code here (PartB).
	LOG(rf.me, rf.currentTerm, DLeader, "Leader accept log [%d]T%d", rf.log.size()-1, rf.currentTerm)
	rf.persistLocked()
	return rf.log.size() - 1, rf.currentTerm, true
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
func (rf *Raft) contextLostLocked(role Role, term int) bool {
	return !(rf.currentTerm == term && rf.role == role)
}

func MakeRaft() *Raft {
	return &Raft{}
}

// func (rf *Raft) GetLeaderId() int {
// 	rf.mu.Lock()
// 	defer rf.mu.Unlock()
// 	return LeaderId
// }

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peerAddrs []string, me int, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}

	rf.peers = make([]RaftGrpcClient, len(peerAddrs))
	rf.conns = make([]*grpc.ClientConn, len(peerAddrs))
	rf.persister = MakePersister(me)
	rf.me = me
	// Your initialization code here (PartA, PartB, PartC).
	rf.role = Follower
	rf.currentTerm = 1
	rf.votedFor = -1

	// log初始化为空，类似于一个空的头节点，避免边界的检查
	//rf.log.append(LogEntry{Term:InvalidTerm})
	rf.log = NewLog(InvalidIndex, InvalidTerm, nil, nil)

	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))

	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.applyCh = applyCh
	rf.applyCond = sync.NewCond(&rf.mu)
	rf.snapAppending = false
	atomic.StoreInt64(&rf.perfStats.appendRpcCount, 0)
	atomic.StoreInt64(&rf.perfStats.appendRpcTime, 0)
	rf.perfStats.lastResetTime = time.Now()
	// initialize from state persisted before a crash
	rf.readPersist(rf.persister.ReadRaftState())
	go func() {
		// 监听自己对应的ip和端口
		lis, err := net.Listen("tcp", "0.0.0.0:8001")
		if err != nil {
			fmt.Println("failed here")
			log.Fatalf("failed to listen: %v", err)
		}

		// 注册到rpc网络中，成为rpc网络中的一个节点
		grpcServer := grpc.NewServer(
			grpc.MaxRecvMsgSize(10*1024*1024),
			grpc.MaxSendMsgSize(10*1024*1024),
			grpc.InitialWindowSize(2*1024*1024),
			grpc.InitialConnWindowSize(4*1024*1024),
			grpc.NumStreamWorkers(4),
		)
		RegisterRaftGrpcServer(
			grpcServer,
			NewServer(rf), // 传入当前 Raft 实例
		)

		LOG(rf.me, rf.currentTerm, DDebug, "gRPC server started on %s", peerAddrs[me])
		if err := grpcServer.Serve(lis); err != nil {
			log.Fatalf("failed to serve: %v", err)
		}

	}()

	// 创建 gRPC 客户端连接
	// 节点自己：用net.Listen在自己的地址上监听，形成RPC服务端，等待其他节点连接。
	// 其他节点：用grpc.Dial()连接到这个地址，作为RPC客户端，发出请求。
	for i, addr := range peerAddrs {
		if i == me {
			rf.peers[i] = nil // 不需要连接自己
			continue
		}
		fmt.Printf("romote rpc:%s\n", addr)
		// Dial连接其他节点
		conn, err := grpc.Dial(
			addr,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithDefaultCallOptions(
				grpc.MaxCallRecvMsgSize(10*1024*1024), // 10MB
			//grpc.MaxCallSendMsgSize(10*1024*1024),
			//grpc.UseCompressor("gzip"), // 启用压缩
			),
			grpc.WithInitialWindowSize(2*1024*1024),     // 2MB
			grpc.WithInitialConnWindowSize(4*1024*1024), // 4MB
		)
		if err != nil {
			log.Fatalf("did not connect: %v", err)
			continue // 继续而不是退出，允许节点部分连接失败
		}
		rf.conns[i] = conn
		rf.peers[i] = NewRaftGrpcClient(conn)
	}

	go rf.electionticker() // 每创建一个raft就可以一直循环触发选举操作
	go rf.applyTicker()    // 每创建一个raft就启动日志复制的操作，在里面有cond等待唤醒

	return rf
}
