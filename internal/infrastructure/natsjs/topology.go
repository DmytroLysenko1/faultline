package natsjs

import (
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

var (
	ErrInvalidTopology  = errors.New("invalid journal topology")
	ErrStreamDrift      = errors.New("journal stream configuration drifted")
	ErrPayloadLimit     = errors.New("nats max_payload is below the journal record limit")
	ErrUnknownPartition = errors.New("partition outside the journal topology")
	ErrNotHolder        = errors.New("not the holder of the topology lease")
)

const (
	HeaderBudget    = 4 << 10
	MaxMessageBytes = 8 << 20
	MaxPartitions   = 256
	maxReplicas     = 5

	streamFormat  = "FAULTLINE_SPANS_P%d"
	subjectFormat = "faultline.spans.p%d"

	metaVersion    = "faultline.config_version"
	metaPartitions = "faultline.partitions"
)

type Compression uint8

const (
	CompressionNone Compression = iota
	CompressionS2
)

var compressions = map[Compression]jetstream.StoreCompression{
	CompressionNone: jetstream.NoCompression,
	CompressionS2:   jetstream.S2Compression,
}

type Topology struct {
	Partitions           int
	Replicas             int
	MaxBytesPerPartition int64
	DuplicateWindow      time.Duration
	Compression          Compression
	ConfigVersion        int
}

func (t Topology) Validate() error {
	for _, check := range []func() error{t.checkPartitions, t.checkReplicas, t.checkLimits, t.checkCompression} {
		if err := check(); err != nil {
			return err
		}
	}
	return nil
}

func (t Topology) checkPartitions() error {
	if t.Partitions < 1 || t.Partitions > MaxPartitions {
		return fmt.Errorf("%w: %d partitions, want 1..%d", ErrInvalidTopology, t.Partitions, MaxPartitions)
	}
	return nil
}

func (t Topology) checkReplicas() error {
	if t.Replicas < 1 || t.Replicas > maxReplicas || t.Replicas%2 == 0 {
		return fmt.Errorf("%w: %d replicas, want 1, 3 or 5", ErrInvalidTopology, t.Replicas)
	}
	return nil
}

func (t Topology) checkLimits() error {
	if t.MaxBytesPerPartition < MaxMessageBytes || t.DuplicateWindow <= 0 || t.ConfigVersion < 1 {
		return fmt.Errorf("%w: max bytes %d (at least %d), duplicate window %s and config version %d must be positive",
			ErrInvalidTopology, t.MaxBytesPerPartition, MaxMessageBytes, t.DuplicateWindow, t.ConfigVersion)
	}
	return nil
}

func (t Topology) checkCompression() error {
	if _, known := compressions[t.Compression]; !known {
		return fmt.Errorf("%w: compression %d", ErrInvalidTopology, t.Compression)
	}
	return nil
}

func (t Topology) Stream(partition uint16) string {
	return fmt.Sprintf(streamFormat, partition)
}

func (t Topology) Subject(partition uint16) string {
	return fmt.Sprintf(subjectFormat, partition)
}

func (t Topology) config(partition uint16) jetstream.StreamConfig {
	return jetstream.StreamConfig{
		Name:        t.Stream(partition),
		Subjects:    []string{t.Subject(partition)},
		Retention:   jetstream.LimitsPolicy,
		Discard:     jetstream.DiscardNew,
		Storage:     jetstream.FileStorage,
		Replicas:    t.Replicas,
		MaxAge:      0,
		MaxMsgSize:  MaxMessageBytes,
		MaxBytes:    t.MaxBytesPerPartition,
		Duplicates:  t.DuplicateWindow,
		Compression: compressions[t.Compression],
		Metadata: map[string]string{
			metaVersion:    strconv.Itoa(t.ConfigVersion),
			metaPartitions: strconv.Itoa(t.Partitions),
		},
	}
}
