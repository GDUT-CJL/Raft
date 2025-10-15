package server

import (
	"course/bridge"
	"course/labgob"
	"course/raft"
	"encoding/json"
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

	Counter int64 // 原子计数器,用于测试
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
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

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
				//fmt.Printf("Type of command: %T\n", message.Command)
				for _, opInterface := range message.Command {
					var op Op
					switch cmd := opInterface.(type) {
					case Op:
						op = cmd
					case map[string]interface{}:
						// 如果是map类型，手动映射到Op,因为在grpc中我们使用json序列化
						// 且我们清楚的知道map中的字段对应Op的字段
						// 因此可以用json反序列化再转成Op，避免繁琐映射
						b, err := json.Marshal(cmd)
						if err == nil {
							json.Unmarshal(b, &op)
						} else {
							// 错误处理
							kv.mu.Unlock()
							continue
						}
					default:
						// 其他类型不处理
						kv.mu.Unlock()
						continue
					}
					var opReply *OpReply
					if kv.requestDuplicated(op.ClientId, op.SeqId) {
						opReply = kv.clientSeqTable[op.ClientId].Reply
						fmt.Println("重复请求")
					} else {
						// 应用到状态机中
						mechineLock.Lock()
						opReply = kv.applyToStateMachine(op)
						mechineLock.Unlock()
						// 更新clientSeqTable数据
						kv.clientSeqTable[op.ClientId] = LastOperationInfo{
							SeqId: op.SeqId,
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
			}
		}
	}
}

func (kv *KVServer) applyToStateMachine(op Op) *OpReply {
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
