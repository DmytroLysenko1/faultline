package main

import (
	"log/slog"
	"time"

	"github.com/DmytroLysenko1/faultline/internal/application/processing"
	"github.com/DmytroLysenko1/faultline/internal/domain/topology"
	"github.com/DmytroLysenko1/faultline/internal/infrastructure/natsjs"
	"github.com/DmytroLysenko1/faultline/internal/infrastructure/natsjs/checkpoint"
	"github.com/DmytroLysenko1/faultline/internal/infrastructure/natsjs/feed"
	"github.com/DmytroLysenko1/faultline/internal/infrastructure/natsjs/lease"
	"github.com/DmytroLysenko1/faultline/internal/interfaces/otlp"
)

const (
	leaseTTL         = 25 * time.Second
	leaseLostAfter   = 20 * time.Second
	leaseRenewEvery  = 2500 * time.Millisecond
	leaseRetryEvery  = time.Second
	leaseAttemptTime = time.Second

	checkpointUploadRate = 64 * mib
	bucketPerImageTenths = 44
	tenths               = 10
	feedPullMaxBytes     = 16 * mib
	restoreFixedBudget   = 30 * time.Second
)

type partitionConfig struct {
	Partition          uint16
	Instance           string
	Streams            natsjs.Topology
	Limits             topology.Limits
	RulesHash          uint32
	FeedMaxBytes       int64
	IdleAfter          time.Duration
	CheckpointInterval time.Duration
	CheckpointDir      string
	Recovery           *processing.DiscardTarget
	RestoreBudget      time.Duration
}

func leaseOptions(cfg *partitionConfig, logger *slog.Logger) *lease.Options {
	return &lease.Options{
		Partition:   cfg.Partition,
		Topology:    cfg.Streams,
		Instance:    cfg.Instance,
		TTL:         leaseTTL,
		RenewEvery:  leaseRenewEvery,
		RetryEvery:  leaseRetryEvery,
		AttemptTime: leaseAttemptTime,
		LostAfter:   leaseLostAfter,
		Logger:      logger,
	}
}

func checkpointOptions(cfg *partitionConfig, epoch uint64) *checkpoint.Options {
	worst := cfg.Limits.WorstCheckpointBytes()
	return &checkpoint.Options{
		Topology:   cfg.Streams,
		Partition:  cfg.Partition,
		Epoch:      epoch,
		LeaseTTL:   leaseTTL,
		UploadRate: checkpointUploadRate,
		WorstImage: worst,
		MaxBytes:   bucketBytes(worst),
		SpoolDir:   cfg.CheckpointDir,
		LimitsHash: cfg.Limits.Hash(),
		RulesHash:  cfg.RulesHash,
	}
}

func bucketBytes(worstImage int64) int64 {
	return (worstImage*bucketPerImageTenths + tenths - 1) / tenths
}

func feedOptions(cfg *partitionConfig, epoch uint64) *feed.Options {
	return &feed.Options{
		Topology:     cfg.Streams,
		Partition:    cfg.Partition,
		Epoch:        epoch,
		MaxBytes:     cfg.FeedMaxBytes,
		MaxFootprint: otlp.MaxBatchFootprint(),
		PullMaxBytes: feedPullMaxBytes,
		IdleAfter:    cfg.IdleAfter,
	}
}

func resumeRequest(cfg *partitionConfig, now time.Time) processing.ResumeRequest {
	return processing.ResumeRequest{
		Partition: cfg.Partition,
		Limits:    &cfg.Limits,
		StartedAt: now,
		Discard:   cfg.Recovery,
	}
}

func partitionCadence(cfg *partitionConfig) processing.Cadence {
	return processing.Cadence{
		FirePeriod:         cfg.Limits.EarlyFirePeriod,
		MaxHeldBytes:       cfg.FeedMaxBytes - otlp.MaxBatchFootprint(),
		CheckpointInterval: cfg.CheckpointInterval,
	}
}

func restoreBudget(worstImage int64) time.Duration {
	return restoreFixedBudget + processing.ResumeBudget(checkpoint.ReadTime(worstImage))
}
