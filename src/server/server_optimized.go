package server

import (
	"bytes"
	"course/bridge"
	"course/labgob"
	"course/raft"
	"fmt"
	"time"
)

func (kv *KVServer) applyTaskOptimized() {
	var lastPrintTime time.Time

	for !kv.killed() {
		select {
		case message := <-kv.applyCh:
			if message.CommandValid {
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

				snapshotNeeded := false
				if kv.maxraftstate != -1 && kv.rf.GetRaftStateSize() >= kv.maxraftstate {
					snapshotNeeded = true
				}

				kv.mu.Unlock()

				var opReplies []*OpReply

				if len(operations) > 0 {
					batchOps := bridge.ConvertOpsToBatch(operations)

					kv.stateMachineMu.Lock()
					results := bridge.BatchApply(batchOps)
					kv.stateMachineMu.Unlock()

					for i, result := range results {
						reply := &OpReply{
							Value: result.Value,
							Err:   "OK",
						}
						if !result.Success {
							reply.Err = result.ErrMsg
							if reply.Err == "" {
								reply.Err = "FAILED"
							}
						}

						if i < len(operations) {
							if operations[i].OpType == Set ||
								operations[i].OpType == HCount ||
								operations[i].OpType == RCount ||
								operations[i].OpType == BCount ||
								operations[i].OpType == ZCount ||
								operations[i].OpType == RCCount {
								reply.Value = result.Value
							}
						}

						opReplies = append(opReplies, reply)
					}
				}

				var finalReplies []*OpReply
				duplicateIndex := 0
				operationIndex := 0

				for i := 0; i < len(duplicateFlags); i++ {
					if duplicateFlags[i] {
						finalReplies = append(finalReplies, cachedReplies[duplicateIndex])
						duplicateIndex++
					} else {
						if operationIndex < len(opReplies) {
							finalReplies = append(finalReplies, opReplies[operationIndex])
							operationIndex++
						}
					}
				}

				kv.mu.Lock()
				var clientSeqTableCopy map[int64]LastOperationInfo
				if snapshotNeeded {
					clientSeqTableCopy = make(map[int64]LastOperationInfo)
				}

				for i, op := range operations {
					if i < len(opReplies) {
						info := LastOperationInfo{
							SeqId: op.SeqId,
							Reply: opReplies[i],
						}
						kv.clientSeqTable[op.ClientId] = info
						if snapshotNeeded {
							clientSeqTableCopy[op.ClientId] = info
						}
					}
				}

				if snapshotNeeded {
					task := snapshotTask{
						index:          message.CommandIndex,
						clientSeqTable: clientSeqTableCopy,
					}
					kv.snapshotQueue = append(kv.snapshotQueue, task)
					kv.snapshotCond.Signal()
				}
				kv.mu.Unlock()

				_, isLeader := kv.rf.GetState()
				if isLeader {
					notifyCh := kv.GetNotifyChannel(message.CommandIndex)

					go func(ch chan []*OpReply, replies []*OpReply, idx int) {
						select {
						case ch <- replies:
							time.Sleep(100 * time.Millisecond)
							kv.RemoveNotifyChannel(idx)
						case <-time.After(50 * time.Millisecond):
							kv.RemoveNotifyChannel(idx)
						}
					}(notifyCh, finalReplies, message.CommandIndex)
				}
			}
		}
	}
}

func (kv *KVServer) applyToStateMachineBatch(ops []raft.Op) []*OpReply {
	replies := make([]*OpReply, len(ops))

	batchOps := bridge.ConvertOpsToBatch(ops)
	results := bridge.BatchApply(batchOps)

	for i, result := range results {
		replies[i] = &OpReply{
			Value: result.Value,
			Err:   "OK",
		}
		if !result.Success {
			replies[i].Err = result.ErrMsg
			if replies[i].Err == "" {
				replies[i].Err = "FAILED"
			}
		}
	}

	return replies
}

func (kv *KVServer) makeSnapshotAsyncOptimized(index int, clientSeqTable map[int64]LastOperationInfo) {
	stateMachineSnapshot := bridge.Storage_Snapshot()
	if stateMachineSnapshot == nil {
		fmt.Printf("KVServer[%d] failed to create storage snapshot\n", kv.me)
		return
	}

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

	snapshot := w.Bytes()
	kv.rf.Snapshot(index, snapshot)
}
