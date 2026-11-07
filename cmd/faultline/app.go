package main

import (
	"errors"
	"fmt"
	"io"
)

const (
	commandAll        = "all"
	usage             = "usage: faultline all"
	exitOK            = 0
	exitStderrFailure = 1
	exitConfig        = 2
)

type process struct {
	Args           []string
	Lookup         lookupFunc
	ReadCgroup     cgroupReader
	SetMemoryLimit func(limit int64) int64
	Stderr         io.Writer
}

var errUsage = errors.New(usage)

func realMain(current process) int {
	if _, err := start(current); err != nil {
		return fail(current.Stderr, err)
	}
	return exitOK
}

func start(current process) (Config, error) {
	if len(current.Args) != 1 || current.Args[0] != commandAll {
		return Config{}, errUsage
	}
	cfg, err := Load(current.Lookup)
	if err != nil {
		return Config{}, err
	}
	if err := enforceMemoryBudget(current, &cfg); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

func enforceMemoryBudget(current process, cfg *Config) error {
	limit, _, err := memoryLimit(current.Lookup, current.ReadCgroup)
	if err != nil {
		return err
	}
	current.SetMemoryLimit(limit)
	return checkBudget(cfg, limit)
}

func fail(stderr io.Writer, reason error) int {
	if _, err := fmt.Fprintln(stderr, reason); err != nil {
		return exitStderrFailure
	}
	return exitConfig
}
