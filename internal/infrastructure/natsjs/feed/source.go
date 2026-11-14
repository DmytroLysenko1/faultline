package feed

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

type reader interface {
	Next(wait time.Duration) (message, error)
	Consumer() string
	Stop()
}

type partitionStream interface {
	Describe(ctx context.Context) (identity, error)
	Purge(ctx context.Context, below uint64) error
}

type message struct {
	data     []byte
	sequence uint64
	stamped  time.Time
	pending  uint64
}

type identity struct {
	Created time.Time
	First   uint64
	Last    uint64
	Bytes   uint64
	Msgs    uint64
}

func (i identity) matches(start *Start) error {
	if !start.Created.IsZero() && !i.Created.Equal(start.Created) {
		return fmt.Errorf("%w: created %s, the checkpoint has %s", ErrStreamIdentity, i.Created, start.Created)
	}
	if after := start.After.Sequence; after > 0 && !i.holds(after) {
		return fmt.Errorf("%w: position %d, the stream holds %d..%d", ErrPositionRange, after, i.First, i.Last)
	}
	return nil
}

func (i identity) holds(after uint64) bool {
	return after+1 >= i.First && after <= i.Last
}

func identityOf(info *jetstream.StreamInfo) identity {
	return identity{
		Created: info.Created,
		First:   info.State.FirstSeq,
		Last:    info.State.LastSeq,
		Bytes:   info.State.Bytes,
		Msgs:    info.State.Msgs,
	}
}

type source struct {
	reader   reader
	stream   partitionStream
	identity identity
	letters  deadLetters
	pulses   pulses
}

func openSource(
	ctx context.Context,
	js jetstream.JetStream,
	start *Start,
	options *Options,
) (*source, error) {
	consumer, err := createConsumer(ctx, js, start, options)
	if err != nil {
		return nil, err
	}
	opened, err := readSource(ctx, js, consumer, start, options)
	if err != nil {
		return nil, errors.Join(err, discard(ctx, js, options.Topology.Stream(options.Partition), consumer))
	}
	return opened, nil
}

func readSource(
	ctx context.Context,
	js jetstream.JetStream,
	consumer jetstream.Consumer,
	start *Start,
	options *Options,
) (*source, error) {
	stream, found, err := describeStream(ctx, js, options.Topology.Stream(options.Partition))
	if err != nil {
		return nil, err
	}
	if err := found.matches(start); err != nil {
		return nil, err
	}
	messages, err := consumer.Messages(jetstream.PullMaxBytes(options.PullMaxBytes),
		jetstream.PullHeartbeat(pullHeartbeat))
	if err != nil {
		return nil, fmt.Errorf("read the consumer: %w", err)
	}
	return &source{
		reader: orderedReader{
			consumer: consumer,
			messages: messages,
		},
		stream:   stream,
		identity: found,
		letters:  letterQueue(js, options),
		pulses:   partitionPulses(js, options),
	}, nil
}

func letterQueue(js jetstream.JetStream, options *Options) deadLetterQueue {
	return deadLetterQueue{
		js:       js,
		topology: options.Topology,
	}
}

func partitionPulses(js jetstream.JetStream, options *Options) journalPulses {
	return journalPulses{
		js:        js,
		partition: options.Partition,
		stream:    options.Topology.Stream(options.Partition),
		subject:   options.Topology.Subject(options.Partition),
	}
}

func discard(
	ctx context.Context,
	js jetstream.JetStream,
	stream string,
	consumer jetstream.Consumer,
) error {
	deleting, cancel := context.WithTimeout(context.WithoutCancel(ctx), discardTimeout)
	defer cancel()
	if err := js.DeleteConsumer(deleting, stream, consumer.CachedInfo().Name); err != nil {
		return fmt.Errorf("discard the consumer: %w", err)
	}
	return nil
}

func describeStream(
	ctx context.Context,
	js jetstream.JetStream,
	name string,
) (journalStream, identity, error) {
	describing, cancel := context.WithTimeout(ctx, infoTimeout)
	defer cancel()
	stream, err := js.Stream(describing, name)
	if err != nil {
		return journalStream{}, identity{}, fmt.Errorf("read the stream: %w", err)
	}
	return journalStream{
		stream: stream,
	}, identityOf(stream.CachedInfo()), nil
}

func createConsumer(
	ctx context.Context,
	js jetstream.JetStream,
	start *Start,
	options *Options,
) (jetstream.Consumer, error) {
	creating, cancel := context.WithTimeout(ctx, consumerTimeout)
	defer cancel()
	config := jetstream.OrderedConsumerConfig{
		FilterSubjects:   []string{options.Topology.Subject(options.Partition)},
		DeliverPolicy:    jetstream.DeliverAllPolicy,
		MaxResetAttempts: maxResetAttempts,
		NamePrefix:       fmt.Sprintf(namePrefixFormat, options.Partition, options.Epoch, rand.Text()),
	}
	if start.After.Sequence > 0 {
		config.DeliverPolicy = jetstream.DeliverByStartSequencePolicy
		config.OptStartSeq = start.After.Sequence + 1
	}
	consumer, err := js.OrderedConsumer(creating, options.Topology.Stream(options.Partition), config)
	if errors.Is(err, jetstream.ErrStreamNotFound) && !start.Created.IsZero() {
		return nil, fmt.Errorf("%w: the stream is gone: %w", ErrStreamIdentity, err)
	}
	if err != nil {
		return nil, fmt.Errorf("create the ordered consumer: %w", err)
	}
	return consumer, nil
}

type orderedReader struct {
	consumer jetstream.Consumer
	messages jetstream.MessagesContext
}

func (r orderedReader) Next(wait time.Duration) (message, error) {
	msg, err := r.messages.Next(jetstream.NextMaxWait(wait))
	if err != nil {
		return message{}, err
	}
	metadata, err := msg.Metadata()
	if err != nil {
		return message{}, fmt.Errorf("record metadata: %w", err)
	}
	return message{
		data:     msg.Data(),
		sequence: metadata.Sequence.Stream,
		stamped:  metadata.Timestamp,
		pending:  metadata.NumPending,
	}, nil
}

func (r orderedReader) Consumer() string {
	info := r.consumer.CachedInfo()
	if info == nil {
		return ""
	}
	return info.Name
}

func (r orderedReader) Stop() {
	r.messages.Stop()
}

type journalStream struct {
	stream jetstream.Stream
}

func (s journalStream) Describe(ctx context.Context) (identity, error) {
	info, err := s.stream.Info(ctx)
	if err != nil {
		return identity{}, err
	}
	return identityOf(info), nil
}

func (s journalStream) Purge(ctx context.Context, below uint64) error {
	return s.stream.Purge(ctx, jetstream.WithPurgeSequence(below))
}
