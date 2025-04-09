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

func Set(key, value string) {
    cKey := C.CString(key)
    cValue := C.CString(value)
    defer C.free(unsafe.Pointer(cKey))
    defer C.free(unsafe.Pointer(cValue))
    C.set(cKey, cValue)
}

func Get(key string) string {
    cKey := C.CString(key)
    defer C.free(unsafe.Pointer(cKey))
    cValue := C.get(cKey)
	if cValue == nil{
		return "NO EXITS"
	}
    return C.GoString(cValue)
}