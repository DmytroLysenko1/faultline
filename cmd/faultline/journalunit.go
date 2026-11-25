package main

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync/atomic"
	"time"

	"github.com/DmytroLysenko1/faultline/internal/infrastructure/natsjs"
	"github.com/DmytroLysenko1/faultline/internal/infrastructure/natsjs/journal"
	"github.com/DmytroLysenko1/faultline/internal/infrastructure/natsjs/record"
	"github.com/DmytroLysenko1/faultline/internal/interfaces/otlp"
)

const (
	layerJournal      = "journal"
	stageAwaitStreams = "journal/await_streams"
	stageJournalOpen  = "journal/open"

	eventWaitPrefix  = "nats."
	eventStreamNotes = "nats.stream_notes"

	verifyBudget = 2 * time.Second
	verifyPause  = time.Second
	lineBreak    = "\n"
	lineJoin     = "; "
)

type connSource interface {
	conns() *natsjs.Conn
}

type streamWait struct {
	reminder
	warned    map[string]bool
	current   string
	lastError string
}

func (w *streamWait) details() []any {
	return []any{
		"reason", w.current,
		"last_error", w.lastError,
	}
}

type journalUnit struct {
	source  connSource
	options journal.Options
	phases  phaseLog
	logger  *slog.Logger
	publish atomic.Pointer[natsjs.Link]
	journal *journal.Journal
}

func journalLayer(
	source connSource,
	options *journal.Options,
	phases phaseLog,
	logger *slog.Logger,
) (layer, *journalUnit) {
	writer := &journalUnit{
		source:  source,
		options: *options,
		phases:  phases,
		logger:  logger,
	}
	return layer{
		name:          layerJournal,
		unit:          writer,
		stopBudget:    options.PublishTimeout + exitGrace,
		contributions: journalContributions(),
		ready:         writer.ready,
	}, writer
}

func journalContributions() []contribution {
	return []contribution{
		{
			name:  "records before pubAck",
			bytes: otlp.MaxConnections * otlp.MaxConcurrentStreams * record.MaxRecordBytes,
		},
		{
			name:  "publish buffer",
			bytes: natsjs.MaxMessageBytes,
		},
	}
}

func (u *journalUnit) start(ctx context.Context, _ unitSupervisor) error {
	conn := u.source.conns()
	if err := u.awaitStreams(ctx, conn); err != nil {
		return &stageError{
			stage: stageAwaitStreams,
			err:   err,
		}
	}
	opened, err := u.open(conn)
	if err != nil {
		return &stageError{
			stage: stageJournalOpen,
			err:   err,
		}
	}
	u.publish.Store(conn.Publish)
	u.journal = opened
	return nil
}

func (u *journalUnit) open(conn *natsjs.Conn) (*journal.Journal, error) {
	publisher, err := conn.Publisher(u.options.Topology)
	if err != nil {
		return nil, err
	}
	return journal.New(publisher, &u.options)
}

func (u *journalUnit) stop(ctx context.Context) error {
	if u.journal == nil {
		return nil
	}
	return u.journal.Close(ctx)
}

func (u *journalUnit) ready() bool {
	link := u.publish.Load()
	return link != nil && link.Connected()
}

func (u *journalUnit) opened() *journal.Journal {
	return u.journal
}

func (u *journalUnit) awaitStreams(ctx context.Context, conn *natsjs.Conn) error {
	wait := &streamWait{
		warned: map[string]bool{},
	}
	for {
		done, err := u.verifyOnce(ctx, conn, wait)
		if done || err != nil {
			return err
		}
	}
}

func (u *journalUnit) verifyOnce(ctx context.Context, conn *natsjs.Conn, wait *streamWait) (bool, error) {
	verified, err := u.verify(ctx, conn)
	if err == nil {
		u.note(ctx, verified.Notes)
		return true, nil
	}
	reason, waits := streamWaitOf(verified, err)
	if !waits {
		return false, err
	}
	u.keepWaiting(ctx, wait, reason, err)
	return false, pause(ctx, verifyPause)
}

func (u *journalUnit) verify(ctx context.Context, conn *natsjs.Conn) (natsjs.Verification, error) {
	verifyCtx, cancel := context.WithTimeout(ctx, verifyBudget)
	defer cancel()
	return conn.VerifyTopology(verifyCtx, u.options.Topology)
}

func (u *journalUnit) keepWaiting(
	ctx context.Context,
	wait *streamWait,
	reason string,
	err error,
) {
	wait.current = reason
	wait.lastError = strings.ReplaceAll(err.Error(), lineBreak, lineJoin)
	if wait.began.IsZero() {
		u.enterWait(wait, reason)
	}
	if !wait.warned[reason] {
		wait.warned[reason] = true
		u.logger.WarnContext(ctx, eventWaitPrefix+reason, "error", wait.lastError)
	}
	wait.remind(ctx, u.logger)
}

func (u *journalUnit) enterWait(wait *streamWait, reason string) {
	now := time.Now()
	wait.reminder = reminder{
		state:  stateWaitingStreams,
		level:  slog.LevelWarn,
		began:  now,
		next:   now.Add(waitReportPeriod),
		detail: wait.details,
	}
	u.phases.transition(stateWaitingStreams, reason)
}

func (u *journalUnit) note(ctx context.Context, notes []natsjs.Drift) {
	if len(notes) == 0 {
		return
	}
	drifted := &natsjs.DriftError{
		Drifts: notes,
	}
	u.logger.WarnContext(ctx, eventStreamNotes, "diff", drifted.Error())
}

func streamWaitOf(verified natsjs.Verification, err error) (string, bool) {
	if errors.Is(err, natsjs.ErrStreamDrift) && verified.RepairedByEnsure() {
		return reasonStreamsNotReady, true
	}
	if natsjs.Temporary(err) {
		return reasonJetStreamUnavailable, true
	}
	return "", false
}
