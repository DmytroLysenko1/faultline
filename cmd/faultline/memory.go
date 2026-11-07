package main

import (
	"bytes"
	"errors"
	"fmt"
	"os"
)

const (
	envMemoryLimit            = "GOMEMLIMIT"
	memoryLimitOff            = "off"
	defaultMemoryLimit        = 1408 * mib
	manifestPodMemory         = 2 * gib
	containerShareNumerator   = defaultMemoryLimit / mib
	containerShareDenominator = manifestPodMemory / mib
	cgroupMemoryMaxPath       = "/sys/fs/cgroup/memory.max"
	cgroupUnlimited           = "max"
	sourceEnv                 = "env"
	sourceCgroup              = "cgroup"
	sourceDefault             = "default"
)

var (
	errMemoryLimitOff       = errors.New("GOMEMLIMIT=off disables the memory budget")
	errMemoryLimitInvalid   = errors.New("invalid GOMEMLIMIT")
	errMemoryLimitContainer = errors.New("GOMEMLIMIT is above the container memory limit")
)

type cgroupReader func() ([]byte, error)

func readCgroupMemoryMax() ([]byte, error) {
	return os.ReadFile(cgroupMemoryMaxPath)
}

func memoryLimit(lookup lookupFunc, readCgroup cgroupReader) (limit int64, source string, err error) {
	container, bounded := containerLimit(readCgroup)
	raw, set := lookup(envMemoryLimit)
	if !set {
		limit, source = derivedLimit(container, bounded)
		return limit, source, nil
	}
	limit, err = parseMemoryLimit(raw)
	if err != nil {
		return 0, "", err
	}
	if bounded && limit > container {
		return 0, "", fmt.Errorf("%w: %s > %s", errMemoryLimitContainer, formatSize(limit), formatSize(container))
	}
	return limit, sourceEnv, nil
}

func derivedLimit(container int64, bounded bool) (limit int64, source string) {
	if bounded {
		return container / containerShareDenominator * containerShareNumerator, sourceCgroup
	}
	return defaultMemoryLimit, sourceDefault
}

func parseMemoryLimit(raw string) (int64, error) {
	if raw == memoryLimitOff {
		return 0, errMemoryLimitOff
	}
	if limit, err := parseUnsigned(raw); err == nil {
		return limit, nil
	}
	limit, err := parseSize(raw, runtimeUnits())
	if err != nil {
		return 0, fmt.Errorf("%w %q: %w", errMemoryLimitInvalid, raw, err)
	}
	return limit, nil
}

func containerLimit(readCgroup cgroupReader) (int64, bool) {
	content, err := readCgroup()
	if err != nil {
		return 0, false
	}
	value := string(bytes.TrimSpace(content))
	if value == cgroupUnlimited {
		return 0, false
	}
	limit, err := parseUnsigned(value)
	if err != nil || limit == 0 {
		return 0, false
	}
	return limit, true
}
