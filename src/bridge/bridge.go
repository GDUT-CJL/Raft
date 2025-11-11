package bridge

/*
#cgo CFLAGS: -I./
#cgo LDFLAGS: -L. -lstorage
#include "storage.h"
#include <stdlib.h>

// 导出C函数，供Go调用
extern void goLogCallback(char* message, int level);
*/
import "C"
import (
	"fmt"
	"log"
	"strings"
	"sync"
	"sync/atomic"
	"unsafe"
)

//export goLogCallback
func goLogCallback(message *C.char, level C.int) {
	// 这个函数在Go中实现，但可以被C调用
	msg := C.GoString(message)
	switch level {
	case 0: // DEBUG
		log.Printf("[STORAGE-DEBUG] %s", msg)
	case 1: // INFO
		log.Printf("[STORAGE-INFO] %s", msg)
	case 2: // WARN
		log.Printf("[STORAGE-WARN] %s", msg)
	case 3: // ERROR
		log.Printf("[STORAGE-ERROR] %s", msg)
	}
}

// 为每个数据结构定义独立的读写锁
// 这里加锁最好是在C层加锁，性能更高
// 为了方便这里在go层加锁
var (
	arrayLock    sync.RWMutex
	hashLock     sync.RWMutex
	rbTreeLock   sync.RWMutex
	bTreeLock    sync.RWMutex
	skiplistLock sync.RWMutex
	rocksdbLock  sync.RWMutex
)

// 创建存储引擎快照
func Storage_Snapshot() []byte {
	var cData *C.char
	var cSize C.size_t

	ret := C.storage_create_snapshot(&cData, &cSize)
	if ret != 0 || cData == nil {
		return nil
	}
	defer C.storage_free_snapshot(cData)

	// 将C数据转换为Go切片
	snapshot := C.GoBytes(unsafe.Pointer(cData), C.int(cSize))
	return snapshot
}

// 从快照恢复存储引擎
func Storage_RestoreSnapshot(snapshot []byte) bool {
	if len(snapshot) == 0 {
		return false
	}

	cData := C.CBytes(snapshot)
	defer C.free(cData)

	ret := C.storage_restore_snapshot((*C.char)(cData), C.size_t(len(snapshot)))
	return ret == 0
}

// BinaryToPrintable 将二进制数据转义为可打印字符串
func BinaryToPrintable(data []byte) string {
	var builder strings.Builder
	for _, b := range data {
		switch b {
		case 0:
			builder.WriteString("\\0") // 或 "\\x00"
		case '\n':
			builder.WriteString("\\n")
		case '\r':
			builder.WriteString("\\r")
		case '\t':
			builder.WriteString("\\t")
		case '\\':
			builder.WriteString("\\\\")
		case '"':
			builder.WriteString("\\\"")
		default:
			if b >= 32 && b <= 126 { // 可打印 ASCII
				builder.WriteByte(b)
			} else {
				// 不可打印字符用 \xXX 表示
				builder.WriteString(fmt.Sprintf("\\x%02x", b))
			}
		}
	}
	return builder.String()
}
func InitMemPool() {
	C.initPool()
}
func InitStorage() {
	// 将Go函数转换为C函数指针
	callback := (C.LogCallback)(unsafe.Pointer(C.goLogCallback))
	// 传递给C库
	C.set_storage_log_callback(callback)

	C.init_btree(C.int(5))
	C.init_array()
	C.init_hashtable()
	C.init_rbtree()
	C.init_skipTable()
	C.init_rocksdb()
}

func DestoryStorage() {
	C.dest_array()
	C.dest_hashtable()
	C.dest_rbtree()
	C.dest_btree()
	C.dest_skiplist()
}

// ----------------------------Array------------------------------------- //
func Array_Set(key string, klen int, value string, vlen int) string {
	cKey := C.CString(key)
	cValue := C.CString(value)
	cKlen := C.size_t(klen)
	cVlen := C.size_t(vlen)
	defer C.free(unsafe.Pointer(cKey))
	defer C.free(unsafe.Pointer(cValue))
	ret := C.set(cKey, cKlen, cValue, cVlen)
	if ret == 0 {
		return "OK"
	}
	return "FALIED"
}

func Array_Get(key string, klen int) string {
	cKey := C.CString(key)
	defer C.free(unsafe.Pointer(cKey))

	var cVlen C.size_t
	cValue := C.get(cKey, C.size_t(klen), &cVlen)

	if cValue == nil || cVlen == 0 {
		return "" // 或返回错误
	}
	// ✅ 使用 GoBytes 安全转换任意二进制数据
	goBytes := C.GoBytes(unsafe.Pointer(cValue), C.int(cVlen))
	return BinaryToPrintable(goBytes)
}

func Array_Delete(key string, klen int) string {
	cKey := C.CString(key)
	cKlen := C.size_t(klen)
	defer C.free(unsafe.Pointer(cKey)) // 释放 C 字符串
	cRet := C.delete(cKey, cKlen)
	if cRet == 0 {
		return "OK"
	}
	return "FALIED"
}

func Array_Count() int {
	return int(C.count())
}

func Array_Exist(key string, klen int) int {
	cKey := C.CString(key)
	cKlen := C.size_t(klen)
	defer C.free(unsafe.Pointer(cKey)) // 释放 C 字符串
	ret := C.exist(cKey, cKlen)
	return int(ret)
}

// ----------------------------Hash------------------------------------- //
func Hash_Set(key string, klen int, value string, vlen int) string {
	cKey := C.CString(key)
	cValue := C.CString(value)
	cKlen := C.size_t(klen)
	cVlen := C.size_t(vlen)
	defer C.free(unsafe.Pointer(cKey))
	defer C.free(unsafe.Pointer(cValue))
	ret := C.hset(cKey, cKlen, cValue, cVlen)
	if ret == 0 {
		return "OK"
	}
	return "FALIED"
}

func Hash_Get(key string, klen int) string {
	cKey := C.CString(key)
	defer C.free(unsafe.Pointer(cKey))
	var cVlen C.size_t
	cValue := C.hget(cKey, C.size_t(klen), &cVlen)
	if cValue == nil || cVlen == 0 {
		return "" // 或返回错误
	}
	goBytes := C.GoBytes(unsafe.Pointer(cValue), C.int(cVlen))
	return BinaryToPrintable(goBytes)
}

func Hash_Delete(key string, klen int) string {
	cKey := C.CString(key)
	cKlen := C.size_t(klen)
	defer C.free(unsafe.Pointer(cKey)) // Don't forget to free the C string
	cRet := C.hdelete(cKey, cKlen)
	if cRet == 0 {
		return "OK"
	}
	return "FAILED"
}

func Hash_Count() int {
	return int(C.hcount())
}

func Hash_Exist(key string, klen int) int {
	cKey := C.CString(key)
	cKlen := C.size_t(klen)
	defer C.free(unsafe.Pointer(cKey)) // 释放 C 字符串
	ret := C.hexist(cKey, cKlen)
	return int(ret)
}

// ----------------------------RBTree------------------------------------- //
func RB_Set(key string, klen int, value string, vlen int) string {
	cKey := C.CString(key)
	cValue := C.CString(value)
	cKlen := C.size_t(klen)
	cVlen := C.size_t(vlen)
	defer C.free(unsafe.Pointer(cKey))
	defer C.free(unsafe.Pointer(cValue))
	ret := C.rset(cKey, cKlen, cValue, cVlen)
	if ret == 0 {
		return "OK"
	}
	return "FALIED"
}

func RB_Get(key string, klen int) string {
	cKey := C.CString(key)
	defer C.free(unsafe.Pointer(cKey))
	var cVlen C.size_t
	cValue := C.rget(cKey, C.size_t(klen), &cVlen)
	if cValue == nil || cVlen == 0 {
		return "" // 或返回错误
	}
	goBytes := C.GoBytes(unsafe.Pointer(cValue), C.int(cVlen))
	return BinaryToPrintable(goBytes)
}

func RB_Delete(key string, klen int) string {
	cKey := C.CString(key)
	cKlen := C.size_t(klen)
	defer C.free(unsafe.Pointer(cKey)) // Don't forget to free the C string

	cRet := C.rdelete(cKey, cKlen)
	if cRet == 0 {
		return "OK"
	}
	return "FAILED"
}

func RB_Count() int {
	return int(C.rcount())
}

func RB_Exist(key string, klen int) int {
	cKey := C.CString(key)
	cKlen := C.size_t(klen)
	defer C.free(unsafe.Pointer(cKey)) // 释放 C 字符串
	ret := C.rexist(cKey, cKlen)
	return int(ret)
}

// ----------------------------BTree------------------------------------- //
func BTree_Set(key string, klen int, value string, vlen int) string {
	cKey := C.CString(key)
	cValue := C.CString(value)
	cKlen := C.size_t(klen)
	cVlen := C.size_t(vlen)
	defer C.free(unsafe.Pointer(cKey))
	defer C.free(unsafe.Pointer(cValue))
	ret := C.bset(cKey, cKlen, cValue, cVlen)
	if ret == 0 {
		return "OK"
	}
	return "FALIED"
}

func BTree_Get(key string, klen int) string {
	cKey := C.CString(key)
	defer C.free(unsafe.Pointer(cKey))
	var cVlen C.size_t
	cValue := C.bget(cKey, C.size_t(klen), &cVlen)
	if cValue == nil || cVlen == 0 {
		return "" // 或返回错误
	}
	// ✅ 使用 GoBytes 安全转换任意二进制数据
	goBytes := C.GoBytes(unsafe.Pointer(cValue), C.int(cVlen))
	return BinaryToPrintable(goBytes)
}

func BTree_Delete(key string, klen int) string {
	cKey := C.CString(key)
	cKlen := C.size_t(klen)
	defer C.free(unsafe.Pointer(cKey)) // Don't forget to free the C string
	cRet := C.bdelete(cKey, cKlen)
	if cRet == 0 {
		return "OK"
	}
	return "FAILED"
}

func BTree_Count() int {
	return int(C.bcount())
}

func BTree_Exist(key string, klen int) int {
	cKey := C.CString(key)
	cKlen := C.size_t(klen)
	defer C.free(unsafe.Pointer(cKey)) // 释放 C 字符串
	ret := C.bexist(cKey, cKlen)
	return int(ret)
}

// ---------------------------- SkipList ------------------------------------- //
func Skiplist_Set(key string, klen int, value string, vlen int) string {
	cKey := C.CString(key)
	cValue := C.CString(value)
	cKlen := C.size_t(klen)
	cVlen := C.size_t(vlen)
	defer C.free(unsafe.Pointer(cKey))
	defer C.free(unsafe.Pointer(cValue))
	ret := C.zset(cKey, cKlen, cValue, cVlen)
	if ret == 0 {
		return "OK"
	}
	return "FALIED"
}

func Skiplist_Get(key string, klen int) string {
	cKey := C.CString(key)
	defer C.free(unsafe.Pointer(cKey))
	var cVlen C.size_t
	cValue := C.zget(cKey, C.size_t(klen), &cVlen)
	if cValue == nil || cVlen == 0 {
		return "" // 或返回错误
	}
	goBytes := C.GoBytes(unsafe.Pointer(cValue), C.int(cVlen))
	return BinaryToPrintable(goBytes)
}

func Skiplist_Delete(key string, klen int) string {
	cKey := C.CString(key)
	cKlen := C.size_t(klen)
	defer C.free(unsafe.Pointer(cKey)) // Don't forget to free the C string
	cRet := C.zdelete(cKey, cKlen)
	if cRet == 0 {
		return "OK"
	}
	return "FAILED"
}

func Skiplist_Count() int {
	return int(C.zcount())
}

func Skiplist_Exist(key string, klen int) int {
	cKey := C.CString(key)
	cKlen := C.size_t(klen)
	defer C.free(unsafe.Pointer(cKey)) // 释放 C 字符串
	ret := C.zexist(cKey, cKlen)
	return int(ret)
}

// ---------------------------- Rocksdb ------------------------------------- //
var rocksdbCounter int64 // 原子计数器
func RC_Set(key, value string) string {
	cKey := C.CString(key)
	cValue := C.CString(value)
	defer C.kvs_free(unsafe.Pointer(cKey))
	defer C.kvs_free(unsafe.Pointer(cValue))
	ret := C.rc_set(cKey, cValue)
	if ret == 0 {
		atomic.AddInt64(&rocksdbCounter, 1)
		return "OK"
	}
	return "FALIED"
}

func RC_Get(key string) string {
	cKey := C.CString(key)
	defer C.kvs_free(unsafe.Pointer(cKey))
	cValue := C.rc_get(cKey)
	return C.GoString(cValue)
}

func RC_Delete(key string) string {
	cKey := C.CString(key)
	defer C.kvs_free(unsafe.Pointer(cKey))

	cRet := C.rc_delete(cKey)
	if cRet == 0 {
		atomic.AddInt64(&rocksdbCounter, -1)
		return "OK"
	}
	return "FAILED"
}

func RC_Count() int {
	return int(atomic.LoadInt64(&rocksdbCounter))
}
