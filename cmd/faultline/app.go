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

type startup struct {
	Config Config
	Memory memorySetting
}

func start(current process) (startup, error) {
	cfg, err := loadConfig(current.Lookup)
	if err != nil {
		return startup{}, err
	}
	memory, err := enforceMemoryBudget(current, contributions(&cfg))
	if err != nil {
		return startup{}, err
	}
	return startup{
		Config: cfg,
		Memory: memory,
	}, nil
}

func loadConfig(lookup lookupFunc) (Config, error) {
	cfg, err := Load(lookup)
	if err != nil {
		return Config{}, err
	}
	if cfg.RouteRules, err = loadRouteRules(cfg.RouteRulesFile); err != nil {
		return Config{}, err
	}
	return cfg, nil
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
