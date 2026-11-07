package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
)

const (
	commandAll  = "all"
	usage       = "usage: faultline all"
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
}

type startup struct {
	Config Config
	Memory memorySetting
}

func realMain(current process) int {
	setup, err := start(current)
	if err != nil {
		return fail(current.Stderr, err, exitConfig)
	}
	return serve(current.Stderr, &setup)
}

func start(current process) (startup, error) {
	if len(current.Args) != 1 || current.Args[0] != commandAll {
		return startup{}, errUsage
	}
	cfg, err := Load(current.Lookup)
	if err != nil {
		return startup{}, err
	}
	memory, err := enforceMemoryBudget(current, &cfg)
	if err != nil {
		return startup{}, err
	}
	return startup{
		Config: cfg,
		Memory: memory,
	}, nil
}

func enforceMemoryBudget(current process, cfg *Config) (memorySetting, error) {
	memory, err := memoryLimit(current.Lookup, current.ReadCgroup)
	if err != nil {
		return memorySetting{}, err
	}
	current.SetMemoryLimit(memory.Bytes)
	if err := checkBudget(cfg, memory.Bytes); err != nil {
		return memorySetting{}, err
	}
	return memory, nil
}

func serve(stderr io.Writer, setup *startup) int {
	logger := slog.New(slog.NewJSONHandler(stderr, &slog.HandlerOptions{
		Level: setup.Config.LogLevel,
	}))
	logger.Info("config.memory_limit", "bytes", setup.Memory.Bytes, "source", setup.Memory.Source)
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := launch(ctx, stop, setup, logger); err != nil {
		return exitFailure
	}
	return exitOK
}

func launch(ctx context.Context, stop context.CancelFunc, setup *startup, logger *slog.Logger) error {
	sys, err := newSystem(&setup.Config, logger)
	if err != nil {
		logger.Error("lifecycle.start_failed", "error", err)
		return err
	}
	open, err := listen(ctx, &setup.Config)
	if err != nil {
		logger.Error("lifecycle.start_failed", "error", err)
		return err
	}
	return run(ctx, stop, sys, open, defaultShutdownBudget())
}

func fail(stderr io.Writer, reason error, code int) int {
	if _, err := fmt.Fprintln(stderr, reason); err != nil {
		return exitFailure
	}
	return code
}
