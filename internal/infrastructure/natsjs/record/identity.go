package record

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"io"
	"math"

	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/ids"
)

const (
	fnvOffset64 uint64 = 14695981039346656037
	fnvPrime64  uint64 = 1099511628211
	separator   byte   = 0
)

func MsgID(tenant string, partition uint16, body []byte) string {
	hasher := sha256.New()
	_, _ = io.WriteString(hasher, tenant)
	_, _ = hasher.Write(binary.LittleEndian.AppendUint16([]byte{separator}, partition))
	_, _ = hasher.Write([]byte{separator})
	_, _ = hasher.Write(body)
	return hex.EncodeToString(hasher.Sum(nil)[:MsgIDLength/2])
}

func PartitionOf(traceID ids.TraceID, partitions int) uint16 {
	if partitions <= 1 {
		return 0
	}
	hash := fnvOffset64
	for _, value := range traceID {
		hash ^= uint64(value)
		hash *= fnvPrime64
	}
	index := hash % uint64(partitions)
	if index > math.MaxUint16 {
		return 0
	}
	return uint16(index)
}
