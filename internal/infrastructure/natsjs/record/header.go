package record

import (
	"encoding/binary"
	"fmt"
	"math"
	"time"
)

func ReadHeader(data []byte, partition uint16, maxFootprint int64) (Header, error) {
	if err := checkEnvelope(data); err != nil {
		return Header{}, err
	}
	if err := checkKnown(data); err != nil {
		return Header{}, err
	}
	header, err := parseHeader(data)
	if err != nil {
		return Header{}, err
	}
	if err := checkHeader(&header, len(data), partition, maxFootprint); err != nil {
		return Header{}, err
	}
	return header, nil
}

func checkEnvelope(data []byte) error {
	if !sizeFits(len(data)) {
		return fmt.Errorf("%w: %d bytes do not fit between the header and the guard", ErrCorrupt, len(data))
	}
	if data[0] != magicFirst || data[1] != magicSecond {
		return fmt.Errorf("%w: bad magic", ErrCorrupt)
	}
	if binary.LittleEndian.Uint32(data[offsetCRC:]) != checksum(data) {
		return fmt.Errorf("%w: checksum mismatch", ErrCorrupt)
	}
	return nil
}

func sizeFits(size int) bool {
	return size >= HeaderBytes && size <= MaxRecordBytes
}

func checkKnown(data []byte) error {
	switch {
	case data[offsetVersion] != Version:
		return fmt.Errorf("%w: version %d", ErrUnknownVersion, data[offsetVersion])
	case Kind(data[offsetKind]) > KindHeartbeat:
		return fmt.Errorf("%w: kind %d", ErrUnknownVersion, data[offsetKind])
	case data[offsetFlags] != 0:
		return fmt.Errorf("%w: header flags %08b", ErrUnknownVersion, data[offsetFlags])
	case binary.LittleEndian.Uint32(data[offsetReserved:]) != 0:
		return fmt.Errorf("%w: reserved header bytes are set", ErrUnknownVersion)
	}
	return nil
}

func parseHeader(data []byte) (Header, error) {
	tenantEnd := HeaderBytes + int(data[offsetTenantLen])
	if tenantEnd > len(data) {
		return Header{}, fmt.Errorf("%w: tenant runs past the record", ErrCorrupt)
	}
	tenant := string(data[HeaderBytes:tenantEnd])
	if err := validateTenant(tenant); err != nil {
		return Header{}, fmt.Errorf("%w: %w", ErrCorrupt, err)
	}
	return Header{
		Version:      data[offsetVersion],
		Kind:         Kind(data[offsetKind]),
		Tenant:       tenant,
		Partition:    binary.LittleEndian.Uint16(data[offsetPartition:]),
		Weight:       int64(binary.LittleEndian.Uint32(data[offsetWeight:])),
		WrittenAt:    time.Unix(0, signed(binary.LittleEndian.Uint64(data[offsetWrittenAt:]))),
		RulesVersion: binary.LittleEndian.Uint32(data[offsetRules:]),
	}, nil
}

func checkHeader(header *Header, size int, partition uint16, maxFootprint int64) error {
	switch {
	case header.Partition != partition:
		return fmt.Errorf("%w: record of partition %d in partition %d", ErrCorrupt, header.Partition, partition)
	case header.Weight > maxFootprint:
		return fmt.Errorf("%w: weight %d exceeds the largest batch footprint %d", ErrCorrupt, header.Weight, maxFootprint)
	}
	return checkShape(header, size-HeaderBytes-len(header.Tenant))
}

func checkShape(header *Header, bodyBytes int) error {
	if header.Kind == KindHeartbeat {
		return checkHeartbeat(header.Weight, bodyBytes)
	}
	if header.Weight == 0 {
		return fmt.Errorf("%w: batch without weight", ErrCorrupt)
	}
	return nil
}

func checkHeartbeat(weight int64, bodyBytes int) error {
	if weight != 0 || bodyBytes != 0 {
		return fmt.Errorf("%w: heartbeat with weight %d and %d body bytes", ErrCorrupt, weight, bodyBytes)
	}
	return nil
}

func signed(value uint64) int64 {
	if value > math.MaxInt64 {
		return 0
	}
	return int64(value)
}
