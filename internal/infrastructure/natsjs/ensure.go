package natsjs

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/samber/lo"
)

const (
	firstRetryWait = 100 * time.Millisecond
	lastRetryWait  = time.Second
	retryBackoff   = 2
	attemptTimeout = time.Second
	missingStream  = "missing"
	presentStream  = "present"
	fieldStream    = "stream"

	fieldDuplicateWindow = "duplicate_window"
)

type Drift struct {
	Stream string
	Field  string
	Want   string
	Got    string
}

type DriftError struct {
	Drifts []Drift
}

func (e *DriftError) Error() string {
	parts := make([]string, 0, len(e.Drifts))
	for _, drift := range e.Drifts {
		parts = append(parts, fmt.Sprintf("%s %s: want %s, nats has %s", drift.Stream, drift.Field, drift.Want, drift.Got))
	}
	return fmt.Sprintf("%v: %s", ErrStreamDrift, strings.Join(parts, "; "))
}

func (e *DriftError) Unwrap() error {
	return ErrStreamDrift
}

type field struct {
	name    string
	mutable bool
	value   func(*jetstream.StreamConfig) string
}

var fields = []field{
	{name: "subjects", value: func(c *jetstream.StreamConfig) string {
		return strings.Join(c.Subjects, ",")
	}},
	{name: "retention", value: func(c *jetstream.StreamConfig) string {
		return c.Retention.String()
	}},
	{name: "discard", value: func(c *jetstream.StreamConfig) string {
		return c.Discard.String()
	}},
	{name: "storage", value: func(c *jetstream.StreamConfig) string {
		return c.Storage.String()
	}},
	{name: "replicas", value: func(c *jetstream.StreamConfig) string {
		return strconv.Itoa(c.Replicas)
	}},
	{name: "max_age", value: func(c *jetstream.StreamConfig) string {
		return c.MaxAge.String()
	}},
	{name: "max_msg_size", value: func(c *jetstream.StreamConfig) string {
		return strconv.Itoa(int(c.MaxMsgSize))
	}},
	{name: metaPartitions, value: func(c *jetstream.StreamConfig) string {
		return c.Metadata[metaPartitions]
	}},
	{name: "max_bytes", mutable: true, value: func(c *jetstream.StreamConfig) string {
		return strconv.FormatInt(c.MaxBytes, 10)
	}},
	{name: fieldDuplicateWindow, mutable: true, value: func(c *jetstream.StreamConfig) string {
		return c.Duplicates.String()
	}},
	{name: "compression", mutable: true, value: func(c *jetstream.StreamConfig) string {
		return c.Compression.String()
	}},
	{name: metaVersion, mutable: true, value: func(c *jetstream.StreamConfig) string {
		return c.Metadata[metaVersion]
	}},
}

var (
	immutableFields = lo.Filter(fields, func(f field, _ int) bool {
		return !f.mutable
	})
	mutableFields = lo.Filter(fields, func(f field, _ int) bool {
		return f.mutable
	})
)

type plan struct {
	want   *jetstream.StreamConfig
	create bool
}

func (c *Conn) EnsureTopology(ctx context.Context, topology Topology) error {
	js, err := c.control(topology)
	if err != nil {
		return err
	}
	plans, drifts, err := planStreams(ctx, js, topology)
	if err != nil || len(drifts) > 0 {
		return errors.Join(err, driftError(drifts))
	}
	return applyAll(ctx, js, plans)
}

func applyAll(ctx context.Context, js jetstream.JetStream, plans []plan) error {
	for _, next := range plans {
		if err := apply(ctx, js, next); err != nil {
			return err
		}
	}
	return nil
}

type Verification struct {
	Blocking []Drift
	Notes    []Drift
}

func (c *Conn) VerifyTopology(ctx context.Context, topology Topology) (Verification, error) {
	js, err := c.control(topology)
	if err != nil {
		return Verification{}, err
	}
	var verified Verification
	for partition := range topology.Partitions {
		found, err := verifyStream(ctx, js, new(topology.config(uint16(partition))))
		if err != nil {
			return Verification{}, err
		}
		verified.Blocking = append(verified.Blocking, found.Blocking...)
		verified.Notes = append(verified.Notes, found.Notes...)
	}
	return verified, driftError(verified.Blocking)
}

func (c *Conn) control(topology Topology) (jetstream.JetStream, error) {
	if err := topology.Validate(); err != nil {
		return nil, err
	}
	js, err := jetstream.New(c.Control.NATS)
	if err != nil {
		return nil, fmt.Errorf("jetstream on the control connection: %w", err)
	}
	return js, nil
}

func planStreams(ctx context.Context, js jetstream.JetStream, topology Topology) ([]plan, []Drift, error) {
	plans := make([]plan, 0, topology.Partitions)
	var drifts []Drift
	for partition := range topology.Partitions {
		next, found, err := planStream(ctx, js, new(topology.config(uint16(partition))))
		if err != nil {
			return nil, nil, err
		}
		plans, drifts = append(plans, next), append(drifts, found...)
	}
	return plans, drifts, nil
}

func planStream(ctx context.Context, js jetstream.JetStream, want *jetstream.StreamConfig) (plan, []Drift, error) {
	got, err := streamConfig(ctx, js, want.Name)
	if errors.Is(err, jetstream.ErrStreamNotFound) {
		return plan{
			want:   want,
			create: true,
		}, nil, nil
	}
	if err != nil {
		return plan{}, nil, err
	}
	return plan{
		want: want,
	}, olderOrDrift(want, got), nil
}

func apply(ctx context.Context, js jetstream.JetStream, next plan) error {
	if err := createOrUpdate(ctx, js, next); err != nil {
		return wrapStream("ensure", next.want.Name, err)
	}
	return readBack(ctx, js, next.want)
}

func createOrUpdate(ctx context.Context, js jetstream.JetStream, next plan) error {
	if !next.create {
		return update(ctx, js, next.want)
	}
	err := retrying(ctx, func(attempt context.Context) error {
		_, err := js.CreateStream(attempt, *next.want)
		return err
	})
	if errors.Is(err, jetstream.ErrStreamNameAlreadyInUse) {
		return update(ctx, js, next.want)
	}
	return err
}

func update(ctx context.Context, js jetstream.JetStream, want *jetstream.StreamConfig) error {
	got, err := streamConfig(ctx, js, want.Name)
	if err != nil {
		return err
	}
	if drifts := olderOrDrift(want, got); len(drifts) > 0 {
		return driftError(drifts)
	}
	return retrying(ctx, func(attempt context.Context) error {
		_, err := js.UpdateStream(attempt, *want)
		return err
	})
}

func readBack(ctx context.Context, js jetstream.JetStream, want *jetstream.StreamConfig) error {
	got, err := streamConfig(ctx, js, want.Name)
	if err != nil {
		return err
	}
	return driftError(compare(want, got, fields))
}

func verifyStream(ctx context.Context, js jetstream.JetStream, want *jetstream.StreamConfig) (Verification, error) {
	got, err := streamConfig(ctx, js, want.Name)
	if errors.Is(err, jetstream.ErrStreamNotFound) {
		missing := Drift{
			Stream: want.Name,
			Field:  fieldStream,
			Want:   presentStream,
			Got:    missingStream,
		}
		return Verification{
			Blocking: []Drift{missing},
		}, nil
	}
	if err != nil {
		return Verification{}, err
	}
	blocking := compare(want, got, immutableFields)
	if got.Duplicates < want.Duplicates {
		blocking = append(blocking, Drift{
			Stream: want.Name,
			Field:  fieldDuplicateWindow,
			Want:   want.Duplicates.String(),
			Got:    got.Duplicates.String(),
		})
	}
	return Verification{
		Blocking: blocking,
		Notes:    compare(want, got, mutableFields),
	}, nil
}

func streamConfig(ctx context.Context, js jetstream.JetStream, name string) (*jetstream.StreamConfig, error) {
	var config *jetstream.StreamConfig
	err := retrying(ctx, func(attempt context.Context) error {
		stream, err := js.Stream(attempt, name)
		if err != nil {
			return err
		}
		config = &stream.CachedInfo().Config
		return nil
	})
	if err != nil {
		return nil, wrapStream("read", name, err)
	}
	return config, nil
}

func retrying(ctx context.Context, call func(attempt context.Context) error) error {
	wait := firstRetryWait
	err := attemptOnce(ctx, call)
	for temporary(err) {
		if waitErr := pause(ctx, wait); waitErr != nil {
			return errors.Join(err, waitErr)
		}
		wait = min(retryBackoff*wait, lastRetryWait)
		err = attemptOnce(ctx, call)
	}
	return err
}

func attemptOnce(ctx context.Context, call func(attempt context.Context) error) error {
	attempt, cancel := context.WithTimeout(ctx, attemptTimeout)
	defer cancel()
	return call(attempt)
}

func pause(ctx context.Context, wait time.Duration) error {
	timer := time.NewTimer(wait)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil

	}
}

func olderOrDrift(want, got *jetstream.StreamConfig) []Drift {
	drifts := compare(want, got, immutableFields)
	if newer(got, want) {
		drifts = append(drifts, Drift{
			Stream: want.Name,
			Field:  metaVersion,
			Want:   want.Metadata[metaVersion],
			Got:    got.Metadata[metaVersion],
		})
	}
	return drifts
}

func compare(want, got *jetstream.StreamConfig, compared []field) []Drift {
	var drifts []Drift
	for _, f := range compared {
		if wanted, found := f.value(want), f.value(got); wanted != found {
			drifts = append(drifts, Drift{
				Stream: want.Name,
				Field:  f.name,
				Want:   wanted,
				Got:    found,
			})
		}
	}
	return drifts
}

func newer(got, want *jetstream.StreamConfig) bool {
	gotVersion, gotErr := strconv.Atoi(got.Metadata[metaVersion])
	wantVersion, wantErr := strconv.Atoi(want.Metadata[metaVersion])
	return gotErr != nil ||
		wantErr != nil ||
		gotVersion > wantVersion
}

func driftError(drifts []Drift) error {
	if len(drifts) == 0 {
		return nil
	}
	return &DriftError{
		Drifts: drifts,
	}
}

func wrapStream(action, name string, err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("%s stream %s: %w", action, name, err)
}

func temporary(err error) bool {
	if err == nil {
		return false
	}
	failure := classify(err)
	return failure.Reason.retryable() ||
		failure.Code == uint16(codeNoSuitablePeers)
}
