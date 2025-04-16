package server

import (
	"course/labgob"
	"course/labrpc"
	"course/raft"
	"sync"
	"sync/atomic"
	"course/bridge"
	//"fmt"
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
	lastApplied    int

	clientSeq   map[int64]int64 // 客户端最后处理的序列号

}

func (kv *KVServer) GetRaft() *raft.Raft {  
    kv.mu.Lock()  
    defer kv.mu.Unlock()  
    return kv.rf  
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

	kv.clientSeq = make(map[int64]int64)
	go kv.applyTask()
	return kv
}

func (kv *KVServer)applyTask(){
	for !kv.killed(){
		select{
		case message := <-kv.applyCh:
			if message.CommandValid{
				kv.mu.Lock()
				// 如果是已经处理过的消息则直接忽略
				if message.CommandIndex <= kv.lastApplied {
					kv.mu.Unlock()
					continue
				}
				kv.lastApplied = message.CommandIndex

				// 这行代码使用了类型断言（type assertion），它的主要作用是将 message.Command 转换为 Op 类型
				op := message.Command.(Op)
				kv.applyToStateMachine(op)
				kv.mu.Unlock()
			}
		}
	}
}

func (kv *KVServer) GetApplyChannel() <-chan raft.ApplyMsg {
	return kv.applyCh
}

func (kv *KVServer)applyToStateMachine(op Op){
	switch	op.OpType{
	case Set:
		bridge.Set(op.Key,op.Value)
	case Get:
		// 仅更新序列号，不修改数据
	}
	// 更新客户端序列号
	kv.clientSeq[op.ClientId] = op.SeqId
}