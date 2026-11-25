package checkpoint

import (
	framed "github.com/DmytroLysenko1/faultline/pkg/checkpoint"
)

func WorstHeapBytes() int64 {
	return spoolBuffer + framed.WriterHeapBytes() + chunkSize
}

func ChunkBytes() int64 {
	return chunkSize
}
