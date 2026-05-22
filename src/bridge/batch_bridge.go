package bridge

/*
#cgo CFLAGS: -I./
#cgo LDFLAGS: -L. -lstorage
#include "batch_ops.h"
#include <stdlib.h>
*/
import "C"
import (
	"course/raft"
	"sync"
	"unsafe"
)

type BatchOperation struct {
	OpType      int
	StorageType int
	Key         string
	Klen        int
	Value       string
	Vlen        int
}

type BatchResult struct {
	Success bool
	Value   int
	ErrMsg  string
}

const (
	StorageArray    = 0
	StorageHash     = 1
	StorageRBTree   = 2
	StorageBTree    = 3
	StorageSkiplist = 4
	StorageRocksDB  = 5
	StorageCount    = 6
)

var storageLocks [StorageCount]sync.Mutex

func BatchApply(ops []BatchOperation) []BatchResult {
	if len(ops) == 0 {
		return nil
	}

	groups := make(map[int][]int)
	for i, op := range ops {
		st := op.StorageType
		if st < 0 || st >= StorageCount {
			st = 0
		}
		groups[st] = append(groups[st], i)
	}

	results := make([]BatchResult, len(ops))

	var wg sync.WaitGroup
	for st, indices := range groups {
		wg.Add(1)
		go func(storageType int, idxList []int) {
			defer wg.Done()
			batchApplyByStorageType(storageType, ops, idxList, results)
		}(st, indices)
	}
	wg.Wait()

	return results
}

func batchApplyByStorageType(storageType int, ops []BatchOperation, indices []int, results []BatchResult) {
	storageLocks[storageType].Lock()
	defer storageLocks[storageType].Unlock()

	cOps := make([]C.BatchOperation, len(indices))
	cKeys := make([]*C.char, len(indices))
	cValues := make([]*C.char, len(indices))

	defer func() {
		for i := range cKeys {
			C.free(unsafe.Pointer(cKeys[i]))
		}
		for i := range cValues {
			C.free(unsafe.Pointer(cValues[i]))
		}
	}()

	for j, idx := range indices {
		op := ops[idx]
		cKeys[j] = C.CString(op.Key)
		cValues[j] = C.CString(op.Value)

		cOps[j] = C.BatchOperation{
			op_type:      C.BatchOpType(op.OpType),
			storage_type: C.int(op.StorageType),
			key:          cKeys[j],
			klen:         C.size_t(op.Klen),
			value:        cValues[j],
			vlen:         C.size_t(op.Vlen),
		}
	}

	cResults := make([]C.BatchResult, len(indices))

	ret := C.batch_apply(&cOps[0], C.int(len(indices)), &cResults[0])
	if ret != 0 {
		for _, idx := range indices {
			results[idx] = BatchResult{Success: false, ErrMsg: "batch_apply failed"}
		}
		return
	}

	for j, cResult := range cResults {
		idx := indices[j]
		results[idx] = BatchResult{
			Success: cResult.success != 0,
			Value:   int(cResult.value),
		}
		if cResult.err_msg != nil {
			results[idx].ErrMsg = C.GoString(cResult.err_msg)
		}
	}
}

func ConvertOpsToBatch(raftOps []raft.Op) []BatchOperation {
	batchOps := make([]BatchOperation, len(raftOps))

	for i, op := range raftOps {
		var opType, storageType int

		switch op.OpType {
		case 0: // Set
			opType = 0
			storageType = 0
		case 1: // Delete
			opType = 1
			storageType = 0
		case 2: // Count
			opType = 2
			storageType = 0
		case 3: // HSet
			opType = 0
			storageType = 1
		case 4: // HDelete
			opType = 1
			storageType = 1
		case 5: // HCount
			opType = 2
			storageType = 1
		case 6: // RSet
			opType = 0
			storageType = 2
		case 7: // RDelete
			opType = 1
			storageType = 2
		case 8: // RCount
			opType = 2
			storageType = 2
		case 9: // BSet
			opType = 0
			storageType = 3
		case 10: // BDelete
			opType = 1
			storageType = 3
		case 11: // BCount
			opType = 2
			storageType = 3
		case 12: // ZSet
			opType = 0
			storageType = 4
		case 13: // ZDelete
			opType = 1
			storageType = 4
		case 14: // ZCount
			opType = 2
			storageType = 4
		case 15: // RCSet
			opType = 0
			storageType = 5
		case 16: // RCDelete
			opType = 1
			storageType = 5
		case 17: // RCCount
			opType = 2
			storageType = 5
		}

		batchOps[i] = BatchOperation{
			OpType:      opType,
			StorageType: storageType,
			Key:         op.Key,
			Klen:        op.Klen,
			Value:       op.Value,
			Vlen:        op.Vlen,
		}
	}

	return batchOps
}
