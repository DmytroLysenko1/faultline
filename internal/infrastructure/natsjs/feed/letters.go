package feed

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/DmytroLysenko1/faultline/internal/infrastructure/natsjs"
	"github.com/DmytroLysenko1/faultline/internal/infrastructure/natsjs/record"
)

var (
	errNoLetter        = errors.New("no dead letter of this partition")
	errMalformedLetter = errors.New("dead letter headers do not parse")
	errForeignAck      = errors.New("publish acknowledged by another stream")
)

const (
	headerPartition = "Dead-Letter-Partition"
	headerSequence  = "Dead-Letter-Sequence"
	headerCreated   = "Dead-Letter-Stream-Created"
	headerReason    = "Dead-Letter-Reason"
	headerTruncated = "Dead-Letter-Truncated"
	headerWindow    = "Dead-Letter-Window"
	windowSeparator = ","
	letterIDFormat  = "dlq-p%d-%d-%d"
	decimal         = 10
	int64Bits       = 64
)

type deadLetter struct {
	partition uint16
	sequence  uint64
	created   time.Time
	reason    letterReason
	data      []byte
	window    window
}

func (l *deadLetter) message(subject string) *nats.Msg {
	msg := nats.NewMsg(subject)
	msg.Data = l.data[:min(len(l.data), record.MaxRecordBytes)]
	msg.Header.Set(headerPartition, strconv.FormatUint(uint64(l.partition), decimal))
	msg.Header.Set(headerSequence, strconv.FormatUint(l.sequence, decimal))
	msg.Header.Set(headerCreated, strconv.FormatInt(l.created.UnixNano(), decimal))
	msg.Header.Set(headerReason, string(l.reason))
	msg.Header.Set(headerWindow, l.window.String())
	if len(l.data) > record.MaxRecordBytes {
		msg.Header.Set(headerTruncated, strconv.Itoa(len(l.data)))
	}
	return msg
}

func (l *deadLetter) msgID() string {
	return fmt.Sprintf(letterIDFormat, l.partition, l.created.UnixNano(), l.sequence)
}

type lastLetter struct {
	created time.Time
	window  window
}

type deadLetterQueue struct {
	js       jetstream.JetStream
	topology natsjs.Topology
}

func (q deadLetterQueue) Publish(ctx context.Context, letter *deadLetter) error {
	ack, err := q.js.PublishMsg(ctx, letter.message(q.topology.DeadLetterSubject(letter.partition)),
		jetstream.WithMsgID(letter.msgID()),
		jetstream.WithExpectStream(natsjs.DeadLetterStream),
	)
	if err != nil {
		return err
	}
	return acknowledgedBy(ack, natsjs.DeadLetterStream)
}

func (q deadLetterQueue) Last(ctx context.Context, partition uint16) (lastLetter, error) {
	stream, err := q.js.Stream(ctx, natsjs.DeadLetterStream)
	if errors.Is(err, jetstream.ErrStreamNotFound) {
		return lastLetter{}, errNoLetter
	}
	if err != nil {
		return lastLetter{}, err
	}
	msg, err := stream.GetLastMsgForSubject(ctx, q.topology.DeadLetterSubject(partition))
	if errors.Is(err, jetstream.ErrMsgNotFound) {
		return lastLetter{}, errNoLetter
	}
	if err != nil {
		return lastLetter{}, err
	}
	return parseLetter(msg.Header)
}
func parseLetter(header nats.Header) (lastLetter, error) {
	created, err := strconv.ParseInt(header.Get(headerCreated), decimal, int64Bits)
	if err != nil {
		return lastLetter{}, fmt.Errorf("%w: %s: %w", errMalformedLetter, headerCreated, err)
	}
	found, err := parseWindow(header.Get(headerWindow))
	if err != nil {
		return lastLetter{}, err
	}
	return lastLetter{
		created: time.Unix(0, created),
		window:  found,
	}, nil
}

func parseWindow(text string) (window, error) {
	low, high, cut := strings.Cut(text, windowSeparator)
	if !cut {
		return window{}, fmt.Errorf("%w: %s %q", errMalformedLetter, headerWindow, text)
	}
	lowest, lowErr := strconv.ParseUint(low, decimal, int64Bits)
	highest, highErr := strconv.ParseUint(high, decimal, int64Bits)
	if err := errors.Join(lowErr, highErr); err != nil {
		return window{}, fmt.Errorf("%w: %s %q: %w", errMalformedLetter, headerWindow, text, err)
	}
	return window{
		low:  lowest,
		high: highest,
	}, nil
}

func acknowledgedBy(ack *jetstream.PubAck, stream string) error {
	if ack.Stream == stream {
		return nil
	}
	return &natsjs.PublishError{
		Reason: natsjs.ReasonStreamMismatch,
		Cause:  fmt.Errorf("%w: acknowledged by %q, want %q", errForeignAck, ack.Stream, stream),
	}
}
