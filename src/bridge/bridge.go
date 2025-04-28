package bridge
/*
#cgo CFLAGS: -I./
#cgo LDFLAGS: -L. -lstorage
#include "storage.h"
#include <stdlib.h>
*/
import "C"
import "unsafe"
// import "fmt"
func InitStorage() {
    C.init_array()
    C.init_hashtable()
    C.init_rbtree()
    C.init_btree(C.int(5))
    C.init_skipTable()
}

func DestoryStorage(){
    C.dest_array()
    C.dest_hashtable()
    C.dest_rbtree()
    C.dest_btree()
    C.dest_skiplist()
}
// ----------------------------Array------------------------------------- // 
func Array_Set(key, value string) string {
    cKey := C.CString(key)
    cValue := C.CString(value)
    defer C.free(unsafe.Pointer(cKey))
    defer C.free(unsafe.Pointer(cValue))
    ret := C.set(cKey, cValue)
    if ret == 0{
        return "OK"
    }
    return "FALIED"
}

func Array_Get(key string) string {
    cKey := C.CString(key)
    defer C.free(unsafe.Pointer(cKey))
    cValue := C.get(cKey)
	// if cValue == nil{
	// 	return "NO EXITS"
	// }
    return C.GoString(cValue)
}

func Array_Delete(key string) string {
    cKey := C.CString(key)
    defer C.free(unsafe.Pointer(cKey))  // 释放 C 字符串  
    cRet := C.delete(cKey)
    if cRet == 0{
        return "OK" 
    }
    return "FALIED"
}

func Array_Count() int {
    return int(C.count())
}

func Array_Exist(key string) int{
    cKey := C.CString(key)
    defer C.free(unsafe.Pointer(cKey))  // 释放 C 字符串 
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
    if ret == 0{
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
    defer C.free(unsafe.Pointer(cKey))  // Don't forget to free the C string
    
    cRet := C.hdelete(cKey)
    if cRet == 0 {
        return "OK" 
    }
    return "FAILED"
}

func Hash_Count() int {
    return int(C.hcount())
}

func Hash_Exist(key string) int{
    cKey := C.CString(key)
    defer C.free(unsafe.Pointer(cKey))  // 释放 C 字符串  
    ret := C.hexist(cKey)
    return int(ret)
}

// ----------------------------RBTree------------------------------------- // 
func RB_Set(key, value string) string {
    cKey := C.CString(key)
    cValue := C.CString(value)
    defer C.free(unsafe.Pointer(cKey))
    defer C.free(unsafe.Pointer(cValue))
    ret := C.rset(cKey, cValue)
    if ret == 0{
        return "OK"
    }
    return "FALIED"
}

func RB_Get(key string) string {
    cKey := C.CString(key)
    defer C.free(unsafe.Pointer(cKey))
    cValue := C.rget(cKey)
    return C.GoString(cValue)
}

func RB_Delete(key string) string {
    cKey := C.CString(key)
    defer C.free(unsafe.Pointer(cKey))  // Don't forget to free the C string
    
    cRet := C.rdelete(cKey)
    if cRet == 0 {
        return "OK" 
    }
    return "FAILED"
}

func RB_Count() int {
    return int(C.rcount())
}

func RB_Exist(key string) int{
    cKey := C.CString(key)
    defer C.free(unsafe.Pointer(cKey))  // 释放 C 字符串  
    ret := C.rexist(cKey)
    return int(ret)
}

// ----------------------------BTree------------------------------------- // 
func BTree_Set(key, value string) string {
    cKey := C.CString(key)
    cValue := C.CString(value)
    defer C.free(unsafe.Pointer(cKey))
    defer C.free(unsafe.Pointer(cValue))
    ret := C.bset(cKey, cValue)
    if ret == 0{
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
    defer C.free(unsafe.Pointer(cKey))  // Don't forget to free the C string
    
    cRet := C.bdelete(cKey)
    if cRet == 0 {
        return "OK" 
    }
    return "FAILED"
}

func BTree_Count() int {
    return int(C.bcount())
}

func BTree_Exist(key string) int{
    cKey := C.CString(key)
    defer C.free(unsafe.Pointer(cKey))  // 释放 C 字符串  
    ret := C.bexist(cKey)
    return int(ret)
}


// ---------------------------- SkipList ------------------------------------- // 
func Skiplist_Set(key, value string) string {
    cKey := C.CString(key)
    cValue := C.CString(value)
    defer C.free(unsafe.Pointer(cKey))
    defer C.free(unsafe.Pointer(cValue))
    ret := C.zset(cKey, cValue)
    if ret == 0{
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
    defer C.free(unsafe.Pointer(cKey))  // Don't forget to free the C string
    
    cRet := C.zdelete(cKey)
    if cRet == 0 {
        return "OK" 
    }
    return "FAILED"
}

func Skiplist_Count() int {
    return int(C.zcount())
}

func Skiplist_Exist(key string) int{
    cKey := C.CString(key)
    defer C.free(unsafe.Pointer(cKey))  // 释放 C 字符串  
    ret := C.zexist(cKey)
    return int(ret)
}
