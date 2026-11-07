package connectapi

import (
	"io"
	"net/http"
	"time"
)

const (
	ReadHeaderTimeout = 5 * time.Second
	ReadTimeout       = 30 * time.Second
	IdleTimeout       = 120 * time.Second

	readyBody    = "ok\n"
	notReadyBody = "not ready\n"
)

type readiness interface {
	Ready() bool
}

func NewServer(handler http.Handler) *http.Server {
	protocols := new(http.Protocols)
	protocols.SetHTTP1(true)
	protocols.SetUnencryptedHTTP2(true)
	return &http.Server{
		Handler:           handler,
		ReadHeaderTimeout: ReadHeaderTimeout,
		ReadTimeout:       ReadTimeout,
		IdleTimeout:       IdleTimeout,
		Protocols:         protocols,
	}
}

func Health(ready readiness) http.Handler {
	return http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		status, body := http.StatusOK, readyBody
		if !ready.Ready() {
			status, body = http.StatusServiceUnavailable, notReadyBody
		}
		writer.WriteHeader(status)
		if _, err := io.WriteString(writer, body); err != nil {
			return
		}
	})
}
