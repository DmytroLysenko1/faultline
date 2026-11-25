package main

import (
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/DmytroLysenko1/faultline/internal/application/processing"
	"github.com/DmytroLysenko1/faultline/internal/infrastructure/natsjs/checkpoint"
)

const (
	envCheckpointRecovery = "CHECKPOINT_RECOVERY"

	recoveryDiscard   = "discard"
	recoveryNone      = "none"
	recoverySeparator = ":"
	firstSeqSeparator = "@"
	recoveryParts     = 3
	decimalBase       = 10
	sequenceBits      = 64
)

var (
	errRecoveryShape      = errors.New("must be discard:<partition>:<checkpoint> or discard:<partition>:none@<first_seq>")
	errRecoveryPartition  = errors.New("names another partition")
	errRecoveryCheckpoint = errors.New("checkpoint must be the 26 base32 characters of its object")
)

func parseRecovery(raw string, partition uint16) (*processing.DiscardTarget, error) {
	if raw == "" {
		return nil, nil
	}
	parts := strings.Split(raw, recoverySeparator)
	if len(parts) != recoveryParts || parts[0] != recoveryDiscard {
		return nil, errRecoveryShape
	}
	if parts[1] != partitionLabel(partition) {
		return nil, fmt.Errorf("%s %w", parts[1], errRecoveryPartition)
	}
	return recoveredTarget(partition, parts[2])
}

func recoveredTarget(partition uint16, name string) (*processing.DiscardTarget, error) {
	first, empty := strings.CutPrefix(name, recoveryNone+firstSeqSeparator)
	if empty {
		sequence, err := strconv.ParseUint(first, decimalBase, sequenceBits)
		if err != nil {
			return nil, fmt.Errorf("first_seq %q: %w", first, errRecoveryShape)
		}
		return &processing.DiscardTarget{
			Partition: partition,
			FirstSeq:  sequence,
		}, nil
	}
	if !checkpoint.ValidName(name) {
		return nil, errRecoveryCheckpoint
	}
	return &processing.DiscardTarget{
		Partition:  partition,
		Checkpoint: name,
	}, nil
}

func formatRecovery(target processing.DiscardTarget) string {
	return recoveryDiscard + recoverySeparator + partitionLabel(target.Partition) + recoverySeparator +
		recoveredCheckpoint(target)
}

func recoveredCheckpoint(target processing.DiscardTarget) string {
	if target.Checkpoint != "" {
		return target.Checkpoint
	}
	return recoveryNone + firstSeqSeparator + strconv.FormatUint(target.FirstSeq, decimalBase)
}

func recoveryHint(target *processing.DiscardTarget) string {
	if target == nil {
		return ""
	}
	return envCheckpointRecovery + "=" + formatRecovery(*target)
}
