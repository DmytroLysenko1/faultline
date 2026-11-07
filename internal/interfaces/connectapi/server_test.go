package connectapi_test

import (
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/google/go-cmp/cmp"

	"github.com/DmytroLysenko1/faultline/internal/interfaces/connectapi"
)

type readyFlag bool

func (r readyFlag) Ready() bool {
	return bool(r)
}

type serverSettings struct {
	ReadHeader      string
	Read            string
	Write           string
	Idle            string
	HTTP1           bool
	UnencryptedHTTP bool
	TLSHTTP2        bool
}

func TestNewServerSetsDeadlinesAndProtocols(t *testing.T) {
	server := connectapi.NewServer(http.NotFoundHandler())
	got := serverSettings{
		ReadHeader:      server.ReadHeaderTimeout.String(),
		Read:            server.ReadTimeout.String(),
		Write:           server.WriteTimeout.String(),
		Idle:            server.IdleTimeout.String(),
		HTTP1:           server.Protocols.HTTP1(),
		UnencryptedHTTP: server.Protocols.UnencryptedHTTP2(),
		TLSHTTP2:        server.Protocols.HTTP2(),
	}
	want := serverSettings{
		ReadHeader:      "5s",
		Read:            "30s",
		Write:           "0s",
		Idle:            "2m0s",
		HTTP1:           true,
		UnencryptedHTTP: true,
	}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("server (-want +got):\n%s", diff)
	}
}

type healthReply struct {
	Status int
	Body   string
}

func TestHealthReflectsReadiness(t *testing.T) {
	tests := []struct {
		Name string
		Args readyFlag
		Want healthReply
	}{
		{
			Name: "serving answers ok",
			Args: true,
			Want: healthReply{
				Status: http.StatusOK,
				Body:   "ok\n",
			},
		},
		{
			Name: "draining or aborting answers not ready",
			Args: false,
			Want: healthReply{
				Status: http.StatusServiceUnavailable,
				Body:   "not ready\n",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			recorder := httptest.NewRecorder()
			request := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/healthz", http.NoBody)
			connectapi.Health(tt.Args).ServeHTTP(recorder, request)
			body, err := io.ReadAll(recorder.Result().Body)
			if err != nil {
				t.Fatalf("read body: %v", err)
			}
			got := healthReply{
				Status: recorder.Code,
				Body:   string(body),
			}
			if diff := cmp.Diff(tt.Want, got); diff != "" {
				t.Errorf("healthz (-want +got):\n%s", diff)
			}
		})
	}
}
