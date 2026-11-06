package limit_test

import (
	"bytes"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"

	"github.com/DmytroLysenko1/faultline/internal/interfaces/connectapi/limit"
)

const (
	procedure    = "/faultline.v1.GraphService/GetGraph"
	writeTimeout = time.Minute
)

var errBrokenConnection = errors.New("broken connection")

type countingHandler struct {
	Calls int
}

func (h *countingHandler) ServeHTTP(response http.ResponseWriter, _ *http.Request) {
	h.Calls++
	response.WriteHeader(http.StatusOK)
}

type brokenWriter struct {
	Headers http.Header
}

func (w *brokenWriter) Header() http.Header {
	return w.Headers
}

func (w *brokenWriter) Write(_ []byte) (int, error) {
	return 0, errBrokenConnection
}

func (w *brokenWriter) WriteHeader(_ int) {
}

type deadlineWriter struct {
	*httptest.ResponseRecorder
	ReadDeadlines  []time.Time
	WriteDeadlines []time.Time
}

func (w *deadlineWriter) SetReadDeadline(deadline time.Time) error {
	w.ReadDeadlines = append(w.ReadDeadlines, deadline)
	return nil
}

func (w *deadlineWriter) SetWriteDeadline(deadline time.Time) error {
	w.WriteDeadlines = append(w.WriteDeadlines, deadline)
	return nil
}

func connectRequest(t *testing.T) *http.Request {
	t.Helper()
	request := httptest.NewRequestWithContext(t.Context(), http.MethodPost, procedure, strings.NewReader("{}"))
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Connect-Protocol-Version", "1")
	return request
}

func newHandler(t *testing.T, next http.Handler, maxConcurrent int64, logger *slog.Logger) *limit.Handler {
	t.Helper()
	handler, err := limit.New(next, maxConcurrent, writeTimeout, logger)
	if err != nil {
		t.Fatalf("limit.New: %v", err)
	}
	return handler
}

func silentLogger() *slog.Logger {
	return slog.New(slog.DiscardHandler)
}

func TestSlotIsReleasedAfterEachRequest(t *testing.T) {
	next := &countingHandler{}
	handler := newHandler(t, next, 1, silentLogger())
	for range 3 {
		recorder := &deadlineWriter{
			ResponseRecorder: httptest.NewRecorder(),
		}
		handler.ServeHTTP(recorder, connectRequest(t))
		if diff := cmp.Diff(http.StatusOK, recorder.Code); diff != "" {
			t.Fatalf("status mismatch, the slot leaked (-want +got):\n%s", diff)
		}
	}
	if diff := cmp.Diff(3, next.Calls); diff != "" {
		t.Errorf("next calls (-want +got):\n%s", diff)
	}
}

func TestEveryAcceptedRequestGetsReadAndWriteDeadlines(t *testing.T) {
	handler := newHandler(t, &countingHandler{}, 1, silentLogger())
	writer := &deadlineWriter{
		ResponseRecorder: httptest.NewRecorder(),
	}
	started := time.Now()
	handler.ServeHTTP(writer, connectRequest(t))
	finished := time.Now()
	if len(writer.ReadDeadlines) != 1 || len(writer.WriteDeadlines) != 1 {
		t.Fatalf("read deadlines %v, write deadlines %v, want one of each", writer.ReadDeadlines, writer.WriteDeadlines)
	}
	if diff := cmp.Diff(writer.ReadDeadlines[0], writer.WriteDeadlines[0]); diff != "" {
		t.Errorf("read and write deadlines differ (-read +write):\n%s", diff)
	}
	deadline := writer.WriteDeadlines[0]
	if deadline.Before(started.Add(writeTimeout)) || deadline.After(finished.Add(writeTimeout)) {
		t.Errorf("deadline %v is not %v after the request", deadline, writeTimeout)
	}
}

func TestUnsupportedDeadlineIsLoggedAndServed(t *testing.T) {
	var logs bytes.Buffer
	next := &countingHandler{}
	handler := newHandler(t, next, 1, slog.New(slog.NewTextHandler(&logs, nil)))
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, connectRequest(t))
	if diff := cmp.Diff(1, next.Calls); diff != "" {
		t.Errorf("next calls (-want +got):\n%s", diff)
	}
	if !strings.Contains(logs.String(), "api.io_deadline_unsupported") {
		t.Errorf("log %q does not report the unsupported deadline", logs.String())
	}
}

func TestBusySlotsRejectWithoutCallingNext(t *testing.T) {
	var logs bytes.Buffer
	next := &countingHandler{}
	handler := newHandler(t, next, 2, slog.New(slog.NewTextHandler(&logs, nil)))
	if !handler.Slots.TryAcquire(2) {
		t.Fatalf("could not occupy the slots")
	}
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, connectRequest(t))
	if diff := cmp.Diff(http.StatusTooManyRequests, recorder.Code); diff != "" {
		t.Errorf("status mismatch (-want +got):\n%s", diff)
	}
	if !strings.Contains(recorder.Body.String(), `"code":"resource_exhausted"`) {
		t.Errorf("body %s is not a Connect resource_exhausted error", recorder.Body.String())
	}
	if diff := cmp.Diff(0, next.Calls); diff != "" {
		t.Errorf("next was called while all slots were busy (-want +got):\n%s", diff)
	}
	for _, want := range []string{"api.busy", "procedure=" + procedure} {
		if !strings.Contains(logs.String(), want) {
			t.Errorf("log %q does not contain %q", logs.String(), want)
		}
	}
}

func TestFailedRejectionIsLogged(t *testing.T) {
	var logs bytes.Buffer
	handler := newHandler(t, &countingHandler{}, 1, slog.New(slog.NewTextHandler(&logs, nil)))
	if !handler.Slots.TryAcquire(1) {
		t.Fatalf("could not occupy the slot")
	}
	handler.ServeHTTP(&brokenWriter{
		Headers: http.Header{},
	}, connectRequest(t))
	wantFragments := []string{
		"api.reject_write_failed",
		"procedure=" + procedure,
		"broken connection",
	}
	for _, want := range wantFragments {
		if !strings.Contains(logs.String(), want) {
			t.Errorf("log %q does not contain %q", logs.String(), want)
		}
	}
}
