package checkpoint

import (
	"errors"
	"hash/crc32"
)

var ErrCorrupt = errors.New("corrupt checkpoint")

type Tag uint8

const (
	magic          = "FLCP"
	versionBytes   = 4
	tagBytes       = 1
	lengthBytes    = 4
	crcBytes       = 4
	frameBytes     = tagBytes + lengthBytes
	MaxStringBytes = 4 << 10
	maxChunkBytes  = 64 << 10
	flushAtBytes   = maxChunkBytes - MaxStringBytes - 2*maxVarintBytes
	maxVarintBytes = 10
	falseByte      = 0
	trueByte       = 1
	nanosPerSecond = 1_000_000_000
	zeroTimeUnix   = -62135596800
)

func checksum(frame, payload []byte) uint32 {
	table := crc32.MakeTable(crc32.Castagnoli)
	return crc32.Update(crc32.Update(0, table, frame), table, payload)
}
