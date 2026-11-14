package feed

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/DmytroLysenko1/faultline/internal/infrastructure/natsjs/record"
)

var errPulseConflict = errors.New("heartbeat would not land right after the last record")

const (
	pulseIDFormat              = "hb-p%d-%d"
	unconditionalPulseIDSuffix = "-u"
)

type pulse struct {
	after     uint64
	writtenAt time.Time
}

type journalPulses struct {
	js        jetstream.JetStream
	partition uint16
	stream    string
	subject   string
}

func (j journalPulses) Pulse(ctx context.Context, beat *pulse) error {
	return j.publish(ctx, beat, j.pulseID(beat), jetstream.WithExpectLastSequence(beat.after))
}

func (j journalPulses) PulseUnconditionally(ctx context.Context, beat *pulse) error {
	return j.publish(ctx, beat, j.pulseID(beat)+unconditionalPulseIDSuffix)
}

func (j journalPulses) pulseID(beat *pulse) string {
	return fmt.Sprintf(pulseIDFormat, j.partition, beat.after)
}

func (j journalPulses) publish(ctx context.Context, beat *pulse, msgID string, expect ...jetstream.PublishOpt) error {
	data, err := record.EncodeHeartbeat(record.HeartbeatTenant, j.partition, beat.writtenAt)
	if err != nil {
		return err
	}
	options := append([]jetstream.PublishOpt{
		jetstream.WithMsgID(msgID),
		jetstream.WithExpectStream(j.stream),
	}, expect...)
	ack, err := j.js.PublishMsg(ctx, &nats.Msg{Subject: j.subject, Data: data}, options...)
	if conflicting(err) {
		return fmt.Errorf("%w: %w", errPulseConflict, err)
	}
	if err != nil {
		return err
	}
	return acknowledgedBy(ack, j.stream)
}

func conflicting(err error) bool {
	apiErr, isAPI := errors.AsType[*jetstream.APIError](err)
	return isAPI && (apiErr.ErrorCode == jetstream.JSErrCodeStreamWrongLastSequence ||
		apiErr.ErrorCode == jetstream.JSErrCodeStreamWrongLastSequenceConstant)
}
