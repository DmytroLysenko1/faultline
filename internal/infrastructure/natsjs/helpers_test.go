package natsjs_test

import (
	"context"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"go.uber.org/goleak"

	"github.com/DmytroLysenko1/faultline/internal/infrastructure/natsjs"
	"github.com/DmytroLysenko1/faultline/internal/infrastructure/natsjs/natsjstest"
)

const (
	connectTimeout   = 2 * time.Second
	drainTimeout     = 500 * time.Millisecond
	closeTimeout     = 5 * time.Second
	publishBudget    = 2 * time.Second
	singleNode       = 1
	threeNodes       = 3
	secondVersion    = 2
	onePartition     = 1
	smallRecordBytes = 64
	eventuallyWithin = 5 * time.Second
	eventuallyPoll   = 10 * time.Millisecond
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

const connName = "faultline-test"

func optionsFor(urls []string) *natsjs.ConnOptions {
	return &natsjs.ConnOptions{
		URLs:           urls,
		Name:           connName,
		ConnectTimeout: connectTimeout,
		DrainTimeout:   drainTimeout,
	}
}

func dial(t *testing.T, cluster *natsjstest.Cluster) *natsjs.Conn {
	t.Helper()
	return dialURLs(t, cluster.URLs())
}

func dialURLs(t *testing.T, urls []string) *natsjs.Conn {
	t.Helper()
	conn, err := natsjs.Dial(t.Context(), optionsFor(urls))
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	t.Cleanup(func() { closeConn(t, conn) })
	return conn
}

func closeConn(t *testing.T, conn *natsjs.Conn) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.WithoutCancel(t.Context()), closeTimeout)
	defer cancel()
	if err := conn.Close(ctx); err != nil {
		t.Errorf("Close: %v", err)
	}
}

// rawConn is the test's own connection, beside the code under test.
func rawConn(t *testing.T, cluster *natsjstest.Cluster) *nats.Conn {
	t.Helper()
	nc, err := nats.Connect(cluster.URLs()[0])
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(nc.Close)
	return nc
}

func rawJetStream(t *testing.T, cluster *natsjstest.Cluster) jetstream.JetStream {
	t.Helper()
	js, err := jetstream.New(rawConn(t, cluster))
	if err != nil {
		t.Fatalf("jetstream: %v", err)
	}
	return js
}

func ensured(t *testing.T, cluster *natsjstest.Cluster, topology natsjs.Topology) *natsjs.Conn {
	t.Helper()
	conn := dial(t, cluster)
	if err := conn.EnsureTopology(t.Context(), topology); err != nil {
		t.Fatalf("EnsureTopology: %v", err)
	}
	return conn
}

func publisherOf(t *testing.T, conn *natsjs.Conn, topology natsjs.Topology) *natsjs.Publisher {
	t.Helper()
	publisher, err := conn.Publisher(topology)
	if err != nil {
		t.Fatalf("Publisher: %v", err)
	}
	return publisher
}

func publishOnce(t *testing.T, publisher *natsjs.Publisher, partition uint16, data []byte, msgID string,
) (bool, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), publishBudget)
	defer cancel()
	return publisher.Publish(ctx, partition, data, msgID)
}

// eventually waits for what another process or the client library does on its own schedule.
func eventually(t *testing.T, what string, condition func() bool) {
	t.Helper()
	ticker := time.NewTicker(eventuallyPoll)
	defer ticker.Stop()
	deadline := time.NewTimer(eventuallyWithin)
	defer deadline.Stop()
	for !condition() {
		select {
		case <-ticker.C:
		case <-deadline.C:
			t.Fatalf("%s: not within %s", what, eventuallyWithin)
		}
	}
}
