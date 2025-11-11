package server

import (
	"bytes"
	"course/bridge"
	"course/labgob"
	"course/raft"
	"fmt"
	"sync"
	"sync/atomic"
)

func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	lastApplied int

	// key为clientID，value是LastOperationInfo（SeqId + Reply）
	// 客户端每次发送一个请求在server中都会给随机一个ClentId和SeqId
	clientSeqTable map[int64]LastOperationInfo // 客户端最后处理的序列号,处理重复请求
	notifyChans    map[int]chan *OpReply
	notifyMu       sync.RWMutex
	Counter        int64 // 原子计数器,用于测试
	// 添加快照相关的字段
	persister *raft.Persister
}

func (kv *KVServer) GetSetCounter() int64 {
	return atomic.LoadInt64(&kv.Counter)
}

func (kv *KVServer) GetRaft() *raft.Raft {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	return kv.rf
}

func (kv *KVServer) Lock() {
	kv.mu.Lock()
}

func (kv *KVServer) Unlock() {
	kv.mu.Unlock()
}

func StartKVServer(servers []string, me int, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(raft.Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate
	// 初始化持久化器
	kv.persister = raft.MakePersister(me)
	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, kv.applyCh)

	// You may need initialization code here.
	kv.dead = 0
	kv.lastApplied = 0

	kv.clientSeqTable = make(map[int64]LastOperationInfo)
	kv.notifyChans = make(map[int]chan *OpReply)

	go kv.applyTask()
	return kv
}

// 制作快照
func (kv *KVServer) makeSnapshot(index int) {
	// 获取C层状态机的快照
	stateMachineSnapshot := bridge.Storage_Snapshot()
	if stateMachineSnapshot == nil {
		fmt.Printf("KVServer[%d] failed to create storage snapshot\n", kv.me)
		return
	}

	// 编码去重表和状态机快照
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	if e.Encode(kv.clientSeqTable) != nil {
		fmt.Printf("KVServer[%d] encode clientSeqTable failed\n", kv.me)
		return
	}
	if e.Encode(stateMachineSnapshot) != nil {
		fmt.Printf("KVServer[%d] encode stateMachineSnapshot failed\n", kv.me)
		return
	}

	// 调用Raft层快照
	snapshot := w.Bytes()
	kv.rf.Snapshot(index, snapshot)
	fmt.Printf("KVServer[%d] made snapshot at index %d, size: %d\n", kv.me, index, len(snapshot))
}

// 从快照恢复
func (kv *KVServer) restoreFromSnapshot(snapshot []byte) {
	if len(snapshot) == 0 {
		return
	}

	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)

	var clientSeqTable map[int64]LastOperationInfo
	var stateMachineSnapshot []byte

	if d.Decode(&clientSeqTable) != nil {
		fmt.Printf("KVServer[%d] decode clientSeqTable failed\n", kv.me)
		return
	}
	if d.Decode(&stateMachineSnapshot) != nil {
		fmt.Printf("KVServer[%d] decode stateMachineSnapshot failed\n", kv.me)
		return
	}

	// 恢复C层状态机
	if !bridge.Storage_RestoreSnapshot(stateMachineSnapshot) {
		fmt.Printf("KVServer[%d] restore storage snapshot failed\n", kv.me)
		return
	}

	kv.clientSeqTable = clientSeqTable
	fmt.Printf("KVServer[%d] restored from snapshot\n", kv.me)
}

var mechineLock sync.RWMutex

func (kv *KVServer) applyTask() {
	for !kv.killed() {
		select {
		case message := <-kv.applyCh:
			if message.CommandValid {
				kv.mu.Lock()
				// 如果是已经处理过的消息则直接忽略
				if message.CommandIndex <= kv.lastApplied {
					kv.mu.Unlock()
					continue
				}
				kv.lastApplied = message.CommandIndex
				var opReplies []*OpReply
				var notifyChs []chan *OpReply
				// fmt.Printf("Type of command: %T\n", message.Command)
				for _, opInterface := range message.Command {
					var opReply *OpReply
					if kv.requestDuplicated(opInterface.ClientId, opInterface.SeqId) {
						opReply = kv.clientSeqTable[opInterface.ClientId].Reply
						fmt.Println("重复请求")
					} else {
						// 应用到状态机中
						mechineLock.Lock()
						opReply = kv.applyToStateMachine(opInterface)
						mechineLock.Unlock()
						// 更新clientSeqTable数据
						kv.clientSeqTable[opInterface.ClientId] = LastOperationInfo{
							SeqId: opInterface.SeqId,
							Reply: opReply,
						}
					}
					// 为了防止死锁更改，先把每个回复的结果存储起来
					opReplies = append(opReplies, opReply)
					notifyCh := kv.GetNotifyChannel(message.CommandIndex)
					//fmt.Printf("commit index = %d\n", message.CommandIndex)
					// 并且把每个通道也存储起来，最后一一的对应返回
					notifyChs = append(notifyChs, notifyCh)
				}
				// 判断是否需要制作快照
				if kv.maxraftstate != -1 && kv.rf.GetRaftStateSize() >= kv.maxraftstate {
					kv.makeSnapshot(message.CommandIndex)
				}
				kv.mu.Unlock() // [注意] 提前释放锁，防止在net层的死锁！！！
				// // 在锁外检查是否是 Leader 并发送结果
				_, isLeader := kv.rf.GetState()
				if isLeader {
					for i, notifyCh := range notifyChs {
						select {
						// 一一对应的返回
						case notifyCh <- opReplies[i]:
						default:
							// 防止阻塞，可选日志记录
						}
					}
				}
			} else if message.SnapshotValid {
				// 处理快照应用
				kv.mu.Lock()
				if message.SnapshotIndex > kv.lastApplied {
					kv.restoreFromSnapshot(message.Snapshot)
					kv.lastApplied = message.SnapshotIndex
				}
				kv.mu.Unlock()
			}
		}
	}
}

func (kv *KVServer) applyToStateMachine(op raft.Op) *OpReply {
	var value int
	var err string
	switch op.OpType {
	case Set:
		err = bridge.Array_Set(op.Key, op.Klen, op.Value, op.Vlen)
	case Delete:
		err = bridge.Array_Delete(op.Key, op.Klen)
	case Count:
		value = bridge.Array_Count()

	case HSet:
		err = bridge.Hash_Set(op.Key, op.Klen, op.Value, op.Vlen)
	case HDelete:
		err = bridge.Hash_Delete(op.Key, op.Klen)
	case HCount:
		value = bridge.Hash_Count()

	case RSet:
		err = bridge.RB_Set(op.Key, op.Klen, op.Value, op.Vlen)
	case RDelete:
		err = bridge.RB_Delete(op.Key, op.Vlen)
	case RCount:
		value = bridge.RB_Count()

	case BSet:
		err = bridge.BTree_Set(op.Key, op.Klen, op.Value, op.Vlen)
	case BDelete:
		err = bridge.BTree_Delete(op.Key, op.Klen)
	case BCount:
		value = bridge.BTree_Count()

	case ZSet:
		err = bridge.Skiplist_Set(op.Key, op.Klen, op.Value, op.Vlen)
	case ZDelete:
		err = bridge.Skiplist_Delete(op.Key, op.Klen)
	case ZCount:
		value = bridge.Skiplist_Count()

	case RCSet:
		err = bridge.RC_Set(op.Key, op.Value)
	case RCDelete:
		err = bridge.RC_Delete(op.Key)
	case RCCount:
		value = bridge.RC_Count()
	default:
		fmt.Println("Wrroied Command")
	}
	return &OpReply{Value: value, Err: err}
}

// 同时写入读取map即并发的读写map的时候会出现错误
func (kv *KVServer) GetNotifyChannel(index int) chan *OpReply {
	kv.notifyMu.Lock()
	defer kv.notifyMu.Unlock()
	if _, ok := kv.notifyChans[index]; !ok {
		kv.notifyChans[index] = make(chan *OpReply, 1)
	}
	return kv.notifyChans[index]
}

func (kv *KVServer) RemoveNotifyChannel(index int) {
	kv.notifyMu.Lock()
	defer kv.notifyMu.Unlock()
	delete(kv.notifyChans, index)
}

func (kv *KVServer) requestDuplicated(clientID, seqId int64) bool {
	info, ok := kv.clientSeqTable[clientID]
	return ok && seqId <= info.SeqId
}
