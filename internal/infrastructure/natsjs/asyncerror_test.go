package natsjs_test

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"strings"
	"sync"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/nats-io/nats-server/v2/server"

	"github.com/DmytroLysenko1/faultline/internal/infrastructure/natsjs"
	"github.com/DmytroLysenko1/faultline/internal/infrastructure/natsjs/natsjstest"
)

const (
	deniedSubject     = "denied.x"
	deniedPublishes   = 200
	permittedUser     = "limited"
	permittedPassword = "limited"
)

type lockedBuffer struct {
	lock   sync.Mutex
	buffer bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.lock.Lock()
	defer b.lock.Unlock()
	return b.buffer.Write(p)
}

func (b *lockedBuffer) count(t *testing.T, message string) int {
	t.Helper()
	b.lock.Lock()
	defer b.lock.Unlock()
	count := 0
	for _, line := range strings.Split(strings.TrimSpace(b.buffer.String()), "\n") {
		record := map[string]any{}
		if err := json.Unmarshal([]byte(line), &record); err != nil {
			t.Fatalf("log line is not JSON: %v", err)
		}
		if record["msg"] == message {
			count++
		}
	}
	return count
}

func TestAnAsyncErrorIsWarnedOncePerClassAndCounted(t *testing.T) {
	cluster := natsjstest.Start(t, singleNode, func(options *server.Options) {
		options.Users = []*server.User{
			{
				Username: permittedUser,
				Password: permittedPassword,
				Permissions: &server.Permissions{
					Publish: &server.SubjectPermission{
						Deny: []string{deniedSubject},
					},
				},
			},
		}
	})
	logs := &lockedBuffer{}
	address := strings.TrimPrefix(cluster.URLs()[0], "nats://")
	options := optionsFor([]string{"nats://" + permittedUser + ":" + permittedPassword + "@" + address})
	options.Logger = slog.New(slog.NewJSONHandler(logs, nil))
	conn := dialURLsWith(t, options)

	for range deniedPublishes {
		if err := conn.Publish.NATS.Publish(deniedSubject, nil); err != nil {
			t.Fatalf("publish: %v", err)
		}
	}
	if err := conn.Publish.NATS.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	eventually(t, "every violation counted", func() bool {
		return conn.Publish.AsyncErrors.Load() == deniedPublishes
	})

	if diff := cmp.Diff(1, logs.count(t, "nats.error")); diff != "" {
		t.Errorf("nats.error lines (-want +got):\n%s", diff)
	}
}

func dialURLsWith(t *testing.T, options *natsjs.ConnOptions) *natsjs.Conn {
	t.Helper()
	conn, err := natsjs.Dial(t.Context(), options)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	t.Cleanup(func() { closeConn(t, conn) })
	return conn
}
