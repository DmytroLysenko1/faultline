package main

import (
	"errors"
	"fmt"
	"io"
)

const (
	commandAll  = "all"
	usage       = "usage: faultline all | ingester | processor"
	exitOK      = 0
	exitFailure = 1
	exitConfig  = 2
)

var errUsage = errors.New(usage)

type process struct {
	Args           []string
	Lookup         lookupFunc
	ReadCgroup     cgroupReader
	SetMemoryLimit func(limit int64) int64
	Stderr         io.Writer
	Hostname       func() (string, error)
}

func enforceMemoryBudget(current process, parts []contribution) (memorySetting, error) {
	memory, err := memoryLimit(current.Lookup, current.ReadCgroup)
	if err != nil {
		return memorySetting{}, err
	}
	current.SetMemoryLimit(memory.Bytes)
	if err := checkParts(parts, memory.Bytes); err != nil {
		return memorySetting{}, err
	}
	return memory, nil
}

func fail(stderr io.Writer, reason error) int {
	if _, err := fmt.Fprintln(stderr, reason); err != nil {
		return exitFailure
	}
	return exitConfig
}
