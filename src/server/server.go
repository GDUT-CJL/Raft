package server

import (
	"course/bridge"
	"course/labgob"
	"course/labrpc"
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
}

func (kv *KVServer) GetRaft() *raft.Raft {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	return kv.rf
}

func (kv *KVServer) GetApplyChannel() <-chan raft.ApplyMsg {
	return kv.applyCh
}

func (kv *KVServer) Lock() {
	kv.mu.Lock()
}

func (kv *KVServer) Unlock() {
	kv.mu.Unlock()
}

func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.dead = 0
	kv.lastApplied = 0

	kv.clientSeqTable = make(map[int64]LastOperationInfo)
	kv.notifyChans = make(map[int]chan *OpReply)

	go kv.applyTask()
	return kv
}

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
				fmt.Printf("Type of command: %T\n", message.Command)

				// 这行代码使用了类型断言（type assertion），它的主要作用是将 message.Command 转换为 Op 类型
				op := message.Command.(Op)
				var opReply *OpReply
				// 线性一致性要求，如果是重复的请求则只会执行一次被称为“请求的幂等性”
				// 避免重复请求的多次执行
				if kv.requestDuplicated(op.ClientId, op.SeqId) {
					opReply = kv.clientSeqTable[op.ClientId].Reply
				} else {
					// 更新clientSeqTable数据
					opReply = kv.applyToStateMachine(op)
					kv.clientSeqTable[op.ClientId] = LastOperationInfo{
						SeqId: op.SeqId,
						Reply: opReply,
					}
				}
				// 将结果发送回去
				if _, isLeader := kv.rf.GetState(); isLeader {
					notifyCh := kv.GetNotifyChannel(message.CommandIndex)
					notifyCh <- opReply
				}
				// 判断是否需要进行snapshot快照
				kv.mu.Unlock()
			}
		}
	}
}

func (kv *KVServer) applyToStateMachine(op Op) *OpReply {
	var value int
	var err string
	switch op.OpType {
	case Set:
		err = bridge.Array_Set(op.Key, op.Value)
	case Delete:
		err = bridge.Array_Delete(op.Key)
	case Count:
		value = bridge.Array_Count()

	case HSet:
		err = bridge.Hash_Set(op.Key, op.Value)
	case HDelete:
		err = bridge.Hash_Delete(op.Key)
	case HCount:
		value = bridge.Hash_Count()

	case RSet:
		err = bridge.RB_Set(op.Key, op.Value)
	case RDelete:
		err = bridge.RB_Delete(op.Key)
	case RCount:
		value = bridge.RB_Count()

	case BSet:
		err = bridge.BTree_Set(op.Key, op.Value)
	case BDelete:
		err = bridge.BTree_Delete(op.Key)
	case BCount:
		value = bridge.BTree_Count()

	case ZSet:
		err = bridge.Skiplist_Set(op.Key, op.Value)
	case ZDelete:
		err = bridge.Skiplist_Delete(op.Key)
	case ZCount:
		value = bridge.Skiplist_Count()
	default:
		fmt.Println("Wrroied Command")
	}
	return &OpReply{Value: value, Err: err}
}

func (kv *KVServer) GetNotifyChannel(index int) chan *OpReply {
	if _, ok := kv.notifyChans[index]; !ok {
		kv.notifyChans[index] = make(chan *OpReply, 1)
	}
	return kv.notifyChans[index]
}

func (kv *KVServer) RemoveNotifyChannel(index int) {
	delete(kv.notifyChans, index)
}

func (kv *KVServer) requestDuplicated(clientID, seqId int64) bool {
	info, ok := kv.clientSeqTable[clientID]
	return ok && seqId <= info.SeqId
}
