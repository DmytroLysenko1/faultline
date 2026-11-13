package natsjs

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

var errForeignAck = errors.New("publish acknowledged by another stream")

const (
	publishRetries   = 2
	publishRetryWait = 250 * time.Millisecond
)

type Publisher struct {
	JetStream jetstream.JetStream
	Streams   []string
	Subjects  []string
}

func (c *Conn) Publisher(topology Topology) (*Publisher, error) {
	if err := topology.Validate(); err != nil {
		return nil, err
	}
	js, err := jetstream.New(c.Publish.NATS)
	if err != nil {
		return nil, fmt.Errorf("jetstream on the publish connection: %w", err)
	}
	publisher := &Publisher{
		JetStream: js,
	}
	for partitions := range topology.Partitions {
		publisher.Streams = append(publisher.Streams, topology.Stream(uint16(partitions)))
		publisher.Subjects = append(publisher.Subjects, topology.Subject(uint16(partitions)))
	}
	return publisher, nil
}

func (p *Publisher) Publish(ctx context.Context, partition uint16, data []byte, msgID string) (bool, error) {
	if int(partition) >= len(p.Subjects) || int(partition) >= len(p.Streams) {
		return false, &PublishError{
			Reason: ReasonUnexpected,
			Cause:  fmt.Errorf("%w: %d of %d", ErrUnknownPartition, partition, len(p.Subjects)),
		}
	}
	message := &nats.Msg{
		Subject: p.Subjects[partition],
		Data:    data,
	}
	ack, err := p.JetStream.PublishMsg(ctx, message,
		jetstream.WithMsgID(msgID),
		jetstream.WithExpectStream(p.Streams[partition]),
		jetstream.WithRetryAttempts(publishRetries),
		jetstream.WithRetryWait(publishRetryWait),
	)
	if err != nil {
		return false, classify(err)
	}
	if ack.Stream != p.Streams[partition] {
		return false, &PublishError{
			Reason: ReasonStreamMismatch,
			Cause:  fmt.Errorf("%w: acknowledged by %q, want %q", errForeignAck, ack.Stream, p.Streams[partition]),
		}
	}
	return ack.Duplicate, nil
}
