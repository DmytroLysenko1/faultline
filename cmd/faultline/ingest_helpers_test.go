package main

import (
	"context"
	"net"
	"net/http"
	"strconv"
	"testing"
	"time"

	"github.com/DmytroLysenko1/faultline/internal/infrastructure/natsjs"
	"github.com/DmytroLysenko1/faultline/internal/infrastructure/natsjs/journal"
	"github.com/DmytroLysenko1/faultline/internal/infrastructure/natsjs/natsjstest"
)

const (
	testStreamBytes   = 64 << 20
	testDedupWindow   = 5 * time.Minute
	testPublishBudget = 2 * time.Second
	closeWithin       = 5 * time.Second
	readyPath         = "/readyz"
)

func testTopology() natsjs.Topology {
	return natsjs.Topology{
		Partitions:           1,
		Replicas:             1,
		MaxBytesPerPartition: testStreamBytes,
		DuplicateWindow:      testDedupWindow,
		Compression:          natsjs.CompressionNone,
		ConfigVersion:        natsjs.StreamConfigVersion,
	}
}

func testJournalOptions() *journal.Options {
	return &journal.Options{
		Tenant:         defaultTenant,
		Topology:       testTopology(),
		PublishTimeout: testPublishBudget,
		Clock:          systemClock{},
	}
}

func dialTest(t *testing.T, urls ...string) *natsjs.Conn {
	t.Helper()
	conn, err := natsjs.Dial(t.Context(), natsSetup(urls...).connOptions(nil))
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.WithoutCancel(t.Context()), closeWithin)
		defer cancel()
		if err := conn.Close(ctx); err != nil {
			t.Errorf("close: %v", err)
		}
	})
	return conn
}

func ensureStreams(t *testing.T, urls ...string) {
	t.Helper()
	if err := dialTest(t, urls...).EnsureTopology(t.Context(), testTopology(), natsjstest.Held{}); err != nil {
		t.Fatalf("EnsureTopology: %v", err)
	}
}

func publishingLayers(cfg *natsConfig, life *lifecycle, logs *logBuffer) []layer {
	conn, conns := connLayer(cfg, life, testLogger(logs))
	published, _ := journalLayer(conns, testJournalOptions(), life, testLogger(logs))
	return []layer{conn, published}
}

func localAddress(t *testing.T) string {
	t.Helper()
	return net.JoinHostPort("127.0.0.1", strconv.Itoa(freePort(t)))
}

func statusOf(t *testing.T, address, path string) int {
	t.Helper()
	request, err := http.NewRequestWithContext(t.Context(), http.MethodGet, "http://"+address+path, http.NoBody)
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		return 0
	}
	if err := response.Body.Close(); err != nil {
		t.Errorf("close body: %v", err)
	}
	return response.StatusCode
}
