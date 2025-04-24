package bridge
/*
#cgo CFLAGS: -I./
#cgo LDFLAGS: -L. -lstorage
#include "storage.h"
#include <stdlib.h>
*/
import "C"
import "unsafe"

func InitStorage() {
    C.init_array()
}

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
    ret := C.exist(cKey)
    return int(ret)
}