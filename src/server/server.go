package server

import (
	"bytes"
	"course/bridge"
	"course/labgob"
	"course/raft"
	"fmt"
	"runtime"
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
	snapshotCond   *sync.Cond     // 用于快照制作的同步
	snapshotQueue  []snapshotTask // 快照任务队列
}

type snapshotTask struct {
	index          int
	clientSeqTable map[int64]LastOperationInfo
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

	kv.applyCh = make(chan raft.ApplyMsg, 1000)
	kv.rf = raft.Make(servers, me, kv.applyCh)

	// You may need initialization code here.
	kv.dead = 0
	kv.lastApplied = 0

	kv.clientSeqTable = make(map[int64]LastOperationInfo)
	kv.notifyChans = make(map[int]chan []*OpReply)
	kv.snapshotCond = sync.NewCond(&kv.mu)
	kv.snapshotQueue = make([]snapshotTask, 0)

	// 启动快照制作协程
	go kv.snapshotWorker()

	go kv.applyTask()
	return kv
}

// 快照制作工作协程
func (kv *KVServer) snapshotWorker() {
	for !kv.killed() {
		var task snapshotTask

		kv.mu.Lock()
		// 等待快照任务
		for len(kv.snapshotQueue) == 0 && !kv.killed() {
			kv.snapshotCond.Wait()
		}
		if kv.killed() {
			kv.mu.Unlock()
			return
		}
		// 取出任务
		task = kv.snapshotQueue[0]
		kv.snapshotQueue = kv.snapshotQueue[1:]
		clientSeqTableCopy := make(map[int64]LastOperationInfo)
		for k, v := range task.clientSeqTable {
			clientSeqTableCopy[k] = v
		}
		kv.mu.Unlock()

		// 制作快照（不持有主锁）
		kv.makeSnapshotAsync(task.index, clientSeqTableCopy)
	}
}

// 异步制作快照
func (kv *KVServer) makeSnapshotAsync(index int, clientSeqTable map[int64]LastOperationInfo) {
	// 获取C层状态机的快照
	stateMachineSnapshot := bridge.Storage_Snapshot()
	if stateMachineSnapshot == nil {
		fmt.Printf("KVServer[%d] failed to create storage snapshot\n", kv.me)
		return
	}

	// 编码去重表和状态机快照
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	if e.Encode(clientSeqTable) != nil {
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

func (kv *KVServer) printResourceUsage() {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	fmt.Printf("KVServer[%d] Resource: Heap=%vMB, Goroutines=%d, GC=%v\n",
		kv.me,
		m.Alloc/1024/1024,
		runtime.NumGoroutine(),
		time.Duration(m.PauseTotalNs).String())
}

func (kv *KVServer) applyTask() {
	var lastPrintTime time.Time
	for !kv.killed() {
		select {
		case message := <-kv.applyCh:
			if message.CommandValid {
				// 只在需要访问共享数据时加锁
				if time.Since(lastPrintTime) > 5*time.Second {
					kv.printResourceUsage()
					lastPrintTime = time.Now()
				}

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

				// 检查是否需要快照
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
				// 先复制一份用于快照
				var clientSeqTableCopy map[int64]LastOperationInfo
				if snapshotNeeded {
					clientSeqTableCopy = make(map[int64]LastOperationInfo)
				}

				for i, op := range operations {
					info := LastOperationInfo{
						SeqId: op.SeqId,
						Reply: opReplies[i],
					}
					kv.clientSeqTable[op.ClientId] = info
					if snapshotNeeded {
						clientSeqTableCopy[op.ClientId] = info
					}
				}

				// 如果有快照需求，放入队列异步处理
				if snapshotNeeded {
					task := snapshotTask{
						index:          message.CommandIndex,
						clientSeqTable: clientSeqTableCopy,
					}
					kv.snapshotQueue = append(kv.snapshotQueue, task)
					kv.snapshotCond.Signal()
				}
				kv.mu.Unlock()

				// 通知网络层 - 关键优化：先检查是否是Leader，再获取通道
				_, isLeader := kv.rf.GetState()
				if isLeader {
					// 获取通知通道（不持有主锁）
					notifyCh := kv.GetNotifyChannel(message.CommandIndex)

					// 异步发送通知，避免阻塞
					go func(ch chan []*OpReply, replies []*OpReply, idx int) {
						select {
						case ch <- replies:
							// 发送成功，延迟清理
							time.Sleep(100 * time.Millisecond)
							kv.RemoveNotifyChannel(idx)
						case <-time.After(50 * time.Millisecond):
							// 超时，直接清理
							kv.RemoveNotifyChannel(idx)
						}
					}(notifyCh, finalReplies, message.CommandIndex)
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
