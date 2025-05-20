package bridge

/*
#cgo CFLAGS: -I./
#cgo LDFLAGS: -L. -lstorage
#include "storage.h"
#include <stdlib.h>
*/
import "C"
import (
	"fmt"
	"sync"
	"sync/atomic"
	"unsafe"
)

// 为每个数据结构定义独立的读写锁
// 这里加锁最好是在C层加锁，性能更高
// 为了方便这里在go层加锁
var (
	arrayLock    sync.RWMutex
	hashLock     sync.RWMutex
	rbTreeLock   sync.RWMutex
	bTreeLock    sync.RWMutex
	skiplistLock sync.RWMutex
)

func InitStorage() {
	C.init_btree(C.int(5))
	C.init_array()
	C.init_hashtable()
	C.init_rbtree()
	C.init_skipTable()
}

func DestoryStorage() {
	C.dest_array()
	C.dest_hashtable()
	C.dest_rbtree()
	C.dest_btree()
	C.dest_skiplist()
}

// ----------------------------Array------------------------------------- //
var arrayCounter int64 // 原子计数器
func Array_Set(key, value string) string {
	cKey := C.CString(key)
	cValue := C.CString(value)
	defer C.free(unsafe.Pointer(cKey))
	defer C.free(unsafe.Pointer(cValue))

	if ret := C.set(cKey, cValue); ret == 0 {
		atomic.AddInt64(&arrayCounter, 1)
		return "OK"
	}
	return "FAILED"
}

func Array_Get(key string) string {
	cKey := C.CString(key)
	defer C.free(unsafe.Pointer(cKey))
	cValue := C.get(cKey)
	return C.GoString(cValue)
}

func Array_Delete(key string) string {
	cKey := C.CString(key)
	defer C.free(unsafe.Pointer(cKey)) // 释放 C 字符串
	cRet := C.delete(cKey)
	if cRet == 0 {
		return "OK"
	}
	return "FALIED"
}

func Array_Count() int {
	return int(atomic.LoadInt64(&arrayCounter))
}

func Array_Exist(key string) int {
	cKey := C.CString(key)
	defer C.free(unsafe.Pointer(cKey)) // 释放 C 字符串
	ret := C.exist(cKey)
	return int(ret)
}

// ----------------------------Hash------------------------------------- //
func Hash_Set(key, value string) string {
	cKey := C.CString(key)
	cValue := C.CString(value)
	defer C.free(unsafe.Pointer(cKey))
	defer C.free(unsafe.Pointer(cValue))
	ret := C.hset(cKey, cValue)
	if ret == 0 {
		return "OK"
	}
	return "FALIED"
}

func Hash_Get(key string) string {
	cKey := C.CString(key)
	defer C.free(unsafe.Pointer(cKey))
	cValue := C.hget(cKey)
	return C.GoString(cValue)
}

func Hash_Delete(key string) string {
	cKey := C.CString(key)
	defer C.free(unsafe.Pointer(cKey))

	cRet := C.hdelete(cKey)
	if cRet == 0 {
		return "OK"
	}
	return "FAILED"
}

func Hash_Count() int {
	return int(C.hcount())
}

func Hash_Exist(key string) int {
	cKey := C.CString(key)
	defer C.free(unsafe.Pointer(cKey)) // 释放 C 字符串
	ret := C.hexist(cKey)
	return int(ret)
}

// ----------------------------RBTree------------------------------------- //
var rbCounter int64 // 原子计数器

func RB_Set(key, value string) string {
	rbTreeLock.RLock()
	defer rbTreeLock.RUnlock()
	cKey := C.CString(key)
	cValue := C.CString(value)
	defer C.free(unsafe.Pointer(cKey))
	defer C.free(unsafe.Pointer(cValue))
	if ret := C.rset(cKey, cValue); ret == 0 {
		atomic.AddInt64(&rbCounter, 1)
		return "OK"
	}
	return "FAILED"
}

func RB_Get(key string) string {
	rbTreeLock.RLock()
	defer rbTreeLock.RUnlock()

	cKey := C.CString(key)
	defer C.free(unsafe.Pointer(cKey))

	cValue := C.rget(cKey)
	if cValue == nil {
		return ""
	}
	return C.GoString(cValue)
}

func RB_Count() int {
	return int(atomic.LoadInt64(&rbCounter))
	//return int(C.rcount())
}

func RB_Delete(key string) string {
	cKey := C.CString(key)
	defer C.free(unsafe.Pointer(cKey)) // Don't forget to free the C string

	cRet := C.rdelete(cKey)
	if cRet == 0 {
		return "OK"
	}
	return "FAILED"
}

// func RB_Count() int {
// 	return int(C.rcount())
// }

func RB_Exist(key string) int {
	cKey := C.CString(key)
	defer C.free(unsafe.Pointer(cKey)) // 释放 C 字符串
	ret := C.rexist(cKey)
	return int(ret)
}

// ----------------------------BTree------------------------------------- //
func BTree_Set(key, value string) string {
	bTreeLock.Lock()
	defer bTreeLock.Unlock()
	cKey := C.CString(key)
	cValue := C.CString(value)
	if cKey == nil || cValue == nil {
		fmt.Println("invalid pointers")
	}
	defer C.free(unsafe.Pointer(cKey))
	defer C.free(unsafe.Pointer(cValue))
	ret := C.bset(cKey, cValue)
	if ret == 0 {
		return "OK"
	}
	return "FALIED"
}

func BTree_Get(key string) string {
	cKey := C.CString(key)
	defer C.free(unsafe.Pointer(cKey))
	cValue := C.bget(cKey)
	return C.GoString(cValue)
}

func BTree_Delete(key string) string {
	cKey := C.CString(key)
	defer C.free(unsafe.Pointer(cKey)) // Don't forget to free the C string

	cRet := C.bdelete(cKey)
	if cRet == 0 {
		return "OK"
	}
	return "FAILED"
}

func BTree_Count() int {
	return int(C.bcount())
}

func BTree_Exist(key string) int {
	cKey := C.CString(key)
	defer C.free(unsafe.Pointer(cKey)) // 释放 C 字符串
	ret := C.bexist(cKey)
	return int(ret)
}

// ---------------------------- SkipList ------------------------------------- //
var skCounter int64

func Skiplist_Set(key, value string) string {
	skiplistLock.Lock()
	defer skiplistLock.Unlock()
	cKey := C.CString(key)
	cValue := C.CString(value)
	defer C.free(unsafe.Pointer(cKey))
	defer C.free(unsafe.Pointer(cValue))
	if ret := C.zset(cKey, cValue); ret == 0 {
		atomic.AddInt64(&skCounter, 1)
		return "OK"
	}
	return "FALIED"
}

func Skiplist_Get(key string) string {
	cKey := C.CString(key)
	defer C.free(unsafe.Pointer(cKey))
	cValue := C.zget(cKey)
	return C.GoString(cValue)
}

func Skiplist_Delete(key string) string {
	cKey := C.CString(key)
	defer C.free(unsafe.Pointer(cKey)) // Don't forget to free the C string

	cRet := C.zdelete(cKey)
	if cRet == 0 {
		return "OK"
	}
	return "FAILED"
}

func Skiplist_Count() int {
	//return int(C.zcount())
	return int(atomic.LoadInt64(&skCounter))
}

func Skiplist_Exist(key string) int {
	cKey := C.CString(key)
	defer C.free(unsafe.Pointer(cKey)) // 释放 C 字符串
	ret := C.zexist(cKey)
	return int(ret)
}
