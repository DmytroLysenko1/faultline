package stream

import (
	"math"
	"unsafe"
)

func seriesIndex(i int) int32 {
	if i < 0 || i > math.MaxInt32 {
		panic("stream: series index out of the int32 range")
	}
	return int32(i)
}

func sizeOf[T any]() int64 {
	var value T
	size := unsafe.Sizeof(value)
	if size > math.MaxInt64 {
		panic("stream: type size out of the int64 range")
	}
	return int64(size)
}
