package server

import (
	"bytes"
	"course/bridge"
	"course/labgob"
	"course/raft"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
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
	notifyChans    map[int]chan []*OpReply
	notifyMu       sync.RWMutex
	Counter        int64 // 原子计数器,用于测试
	// 添加快照相关的字段
	persister *raft.Persister

	stateMachineMu sync.RWMutex
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
	kv.notifyChans = make(map[int]chan []*OpReply)

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
	//fmt.Printf("KVServer[%d] made snapshot at index %d, size: %d\n", kv.me, index, len(snapshot))
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

func (kv *KVServer) applyTask() {
	for !kv.killed() {
		select {
		case message := <-kv.applyCh:
			if message.CommandValid {
				// 只在需要访问共享数据时加锁
				kv.mu.Lock()
				if message.CommandIndex <= kv.lastApplied {
					kv.mu.Unlock()
					continue
				}
				kv.lastApplied = message.CommandIndex

				// 批量收集需要处理的操作
				var operations []raft.Op
				var duplicateFlags []bool
				var cachedReplies []*OpReply

				for _, op := range message.Command {
					if kv.requestDuplicated(op.ClientId, op.SeqId) {
						duplicateFlags = append(duplicateFlags, true)
						cachedReplies = append(cachedReplies, kv.clientSeqTable[op.ClientId].Reply)
					} else {
						duplicateFlags = append(duplicateFlags, false)
						operations = append(operations, op)
					}
				}

				// 只在必要时检查快照
				snapshotNeeded := false
				if kv.maxraftstate != -1 && kv.rf.GetRaftStateSize() >= kv.maxraftstate {
					snapshotNeeded = true
				}

				kv.mu.Unlock() // 提前释放锁！

				// 应用状态机操作（使用状态机锁）
				var opReplies []*OpReply
				kv.stateMachineMu.Lock()
				for _, op := range operations {
					reply := kv.applyToStateMachine(op)
					opReplies = append(opReplies, reply)
				}
				kv.stateMachineMu.Unlock()

				// 合并结果
				var finalReplies []*OpReply
				duplicateIndex := 0
				operationIndex := 0

				for i := 0; i < len(duplicateFlags); i++ {
					if duplicateFlags[i] {
						finalReplies = append(finalReplies, cachedReplies[duplicateIndex])
						duplicateIndex++
					} else {
						finalReplies = append(finalReplies, opReplies[operationIndex])
						operationIndex++
					}
				}

				// 更新clientSeqTable（需要再次加锁，但时间很短）
				kv.mu.Lock()
				for i, op := range operations {
					kv.clientSeqTable[op.ClientId] = LastOperationInfo{
						SeqId: op.SeqId,
						Reply: opReplies[i],
					}
				}

				// 制作快照（如果必要）
				if snapshotNeeded {
					kv.makeSnapshot(message.CommandIndex)
				}

				kv.mu.Unlock()

				// 通知网络层 - 关键优化：先检查是否是Leader，再获取通道
				_, isLeader := kv.rf.GetState()
				if isLeader {
					//startTime := time.Now()
					notifyCh := kv.GetNotifyChannel(message.CommandIndex)
					select {
					case notifyCh <- finalReplies:
					default:
						// 通道满时，异步重试一次
						go func(ch chan []*OpReply, replies []*OpReply, idx int) {
							time.Sleep(time.Microsecond * 10)
							select {
							case ch <- replies:
							default:
								kv.RemoveNotifyChannel(idx)
							}
						}(notifyCh, finalReplies, message.CommandIndex)
					}
					//duration := time.Since(startTime)
					//fmt.Printf("Leader通知耗时: %v\n", duration)
				}
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
func (kv *KVServer) GetNotifyChannel(index int) chan []*OpReply {
	// 使用读锁快速检查
	kv.notifyMu.RLock()
	ch, exists := kv.notifyChans[index]
	kv.notifyMu.RUnlock()

	if exists {
		return ch
	}

	// 不存在时使用写锁创建
	kv.notifyMu.Lock()
	defer kv.notifyMu.Unlock()

	// 双重检查
	if ch, exists = kv.notifyChans[index]; exists {
		return ch
	}

	ch = make(chan []*OpReply, 1)
	kv.notifyChans[index] = ch
	return ch
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
