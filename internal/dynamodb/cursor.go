package dynamodb

import (
	"github.com/francoispqt/gojay"
	"reflect"
	"unsafe"
)

var curOffset uintptr

func init() {
	cur, ok := reflect.TypeOf(gojay.Decoder{}).FieldByName("cursor")
	if !ok {
		panic("failed to get Decoder.cursor field")
	}
	curOffset = cur.Offset
}

func cursor(dec *gojay.Decoder) int {
	return *(*int)(unsafe.Pointer(uintptr(unsafe.Pointer(dec)) + curOffset))
}
