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

var batchLock sync.Mutex

func BatchApply(ops []BatchOperation) []BatchResult {
	if len(ops) == 0 {
		return nil
	}

	batchLock.Lock()
	defer batchLock.Unlock()

	cOps := make([]C.BatchOperation, len(ops))
	cKeys := make([]*C.char, len(ops))
	cValues := make([]*C.char, len(ops))

	defer func() {
		for i := range cKeys {
			C.free(unsafe.Pointer(cKeys[i]))
		}
		for i := range cValues {
			C.free(unsafe.Pointer(cValues[i]))
		}
	}()

	for i, op := range ops {
		cKeys[i] = C.CString(op.Key)
		cValues[i] = C.CString(op.Value)

		cOps[i] = C.BatchOperation{
			op_type:      C.BatchOpType(op.OpType),
			storage_type: C.int(op.StorageType),
			key:          cKeys[i],
			klen:         C.size_t(op.Klen),
			value:        cValues[i],
			vlen:         C.size_t(op.Vlen),
		}
	}

	cResults := make([]C.BatchResult, len(ops))

	ret := C.batch_apply(&cOps[0], C.int(len(ops)), &cResults[0])
	if ret != 0 {
		return nil
	}

	results := make([]BatchResult, len(ops))
	for i, cResult := range cResults {
		results[i] = BatchResult{
			Success: cResult.success != 0,
			Value:   int(cResult.value),
		}
		if cResult.err_msg != nil {
			results[i].ErrMsg = C.GoString(cResult.err_msg)
		}
	}

	return results
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
