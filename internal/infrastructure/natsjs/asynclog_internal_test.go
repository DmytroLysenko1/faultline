package natsjs

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"strings"
	"syscall"
	"testing"
	"testing/synctest"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/nats-io/nats.go"
)

type asyncLine struct {
	Error      string
	Suppressed float64
	Total      float64
}

func asyncLines(t *testing.T, logs *bytes.Buffer) []asyncLine {
	t.Helper()
	var lines []asyncLine
	for _, raw := range strings.Split(strings.TrimSpace(logs.String()), "\n") {
		record := map[string]any{}
		if err := json.Unmarshal([]byte(raw), &record); err != nil {
			t.Fatalf("log line is not JSON: %v", err)
		}
		suppressed, _ := record["suppressed"].(float64)
		total, _ := record["total"].(float64)
		message, _ := record["error"].(string)
		lines = append(lines, asyncLine{
			Error:      message,
			Suppressed: suppressed,
			Total:      total,
		})
	}
	return lines
}

func TestAnAsyncErrorRepeatsOncePerPeriodWithWhatItSuppressed(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		logs := &bytes.Buffer{}
		log := newAsyncLog(slog.New(slog.NewJSONHandler(logs, nil)), rolePublish, &Link{})
		denied := fmt.Errorf("%w: publish to a", nats.ErrPermissionViolation)
		deniedElsewhere := fmt.Errorf("%w: publish to b", nats.ErrPermissionViolation)

		log.handle(nil, nil, denied)
		log.handle(nil, nil, deniedElsewhere)
		log.handle(nil, nil, denied)
		time.Sleep(asyncRepeatPeriod)
		log.handle(nil, nil, denied)
		log.handle(nil, nil, denied)
		time.Sleep(asyncRepeatPeriod)
		log.handle(nil, nil, deniedElsewhere)

		want := []asyncLine{
			{Error: denied.Error(), Total: 1},
			{Error: denied.Error(), Suppressed: 2, Total: 4},
			{Error: deniedElsewhere.Error(), Suppressed: 1, Total: 6},
		}
		if diff := cmp.Diff(want, asyncLines(t, logs)); diff != "" {
			t.Errorf("nats.error lines (-want +got):\n%s", diff)
		}
	})
}

func TestUnclassifiedAsyncErrorsKeepTheirOwnClassUpToTheCap(t *testing.T) {
	logs := &bytes.Buffer{}
	log := newAsyncLog(slog.New(slog.NewJSONHandler(logs, nil)), rolePublish, &Link{})

	for index := range maxAsyncClasses + 2 {
		log.handle(nil, nil, errors.New("unknown "+fmt.Sprint(index)))
	}

	if diff := cmp.Diff(maxAsyncClasses+1, len(asyncLines(t, logs))); diff != "" {
		t.Errorf("nats.error lines: one per distinct error up to the cap, then one unclassified (-want +got):\n%s", diff)
	}
}

func TestReconnectNoiseCannotHideALaterSentinel(t *testing.T) {
	logs := &bytes.Buffer{}
	log := newAsyncLog(slog.New(slog.NewJSONHandler(logs, nil)), rolePublish, &Link{})

	for port := range maxAsyncClasses + 1 {
		log.handle(nil, nil, &net.OpError{
			Op:     "write",
			Net:    "tcp",
			Source: &net.TCPAddr{Port: 40000 + port},
			Err:    syscall.EPIPE,
		})
	}
	noise := len(asyncLines(t, logs))
	for index := range maxAsyncClasses {
		log.handle(nil, nil, errors.New("unknown "+fmt.Sprint(index)))
	}
	log.handle(nil, nil, nats.ErrMaxSubscriptionsExceeded)

	lines := asyncLines(t, logs)
	if diff := cmp.Diff(nats.ErrMaxSubscriptionsExceeded.Error(), lines[len(lines)-1].Error); diff != "" {
		t.Errorf("the last nats.error line, after a full cap (-want +got):\n%s", diff)
	}
	if diff := cmp.Diff(1, noise); diff != "" {
		t.Errorf("nats.error lines for write errors from new local ports (-want +got):\n%s", diff)
	}
}
