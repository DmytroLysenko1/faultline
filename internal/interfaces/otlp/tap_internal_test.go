package otlp

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/tap"

	"github.com/DmytroLysenko1/faultline/internal/application/ingestauth"
	"github.com/DmytroLysenko1/faultline/internal/domain/credential"
	"github.com/DmytroLysenko1/faultline/internal/interfaces/otlp/otlptest"
)

const (
	benchTenant    = "default"
	benchStaleness = 45 * time.Second
	benchGrants    = 1024
	idAlphabet     = "abcdefghijklmnopqrstuvwxyz234567"
)

var errBrokenDirectory = errors.New("broken directory")

type benchClock struct{}

func (benchClock) Now() time.Time {
	return time.Unix(0, 0)
}

func benchOptions() Options {
	return DefaultOptions(slog.New(slog.DiscardHandler), benchClock{})
}

func answering(outcome error) otlptest.AuthenticatorFunc {
	return func(context.Context, string) error {
		return outcome
	}
}

func bearerHeader() metadata.MD {
	return metadata.Pairs(authorizationHeader, "Bearer "+otlptest.Token)
}

func TestTheTapNeverRefusesAStreamAndTheHandlerRefusesInstead(t *testing.T) {
	type args struct {
		header  metadata.MD
		outcome error
	}
	type want struct {
		TapRefused      bool
		TokenLeftInCtx  bool
		CountedAtTheTap TapStats
		Code            codes.Code
	}
	tests := []struct {
		name string
		args args
		want want
	}{
		{
			name: "an admitted token",
			args: args{
				header: bearerHeader(),
			},
			want: want{
				Code: codes.OK,
			},
		},
		{
			name: "no authorization header",
			args: args{
				header: metadata.MD{},
			},
			want: want{
				Code: codes.Unauthenticated,
			},
		},
		{
			name: "a malformed authorization header",
			args: args{
				header: metadata.Pairs(authorizationHeader, "Basic"),
			},
			want: want{
				Code: codes.Unauthenticated,
			},
		},
		{
			name: "a rejected token",
			args: args{
				header: bearerHeader(),
				outcome: ingestauth.RejectionError{
					Reason: ingestauth.ReasonUnknown,
				},
			},
			want: want{
				Code: codes.Unauthenticated,
			},
		},
		{
			name: "a stale directory",
			args: args{
				header:  bearerHeader(),
				outcome: ingestauth.ErrDirectoryStale,
			},
			want: want{
				Code: codes.Unavailable,
			},
		},
		{
			name: "an unexpected error",
			args: args{
				header:  bearerHeader(),
				outcome: errBrokenDirectory,
			},
			want: want{
				Code: codes.Internal,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gate := NewTap(answering(tt.args.outcome), benchOptions())
			ctx, err := gate.handle(t.Context(), &tap.Info{
				Header: tt.args.header,
			})
			counted := gate.Stats()
			got := want{
				TapRefused:      err != nil,
				TokenLeftInCtx:  carriesAuthorization(ctx),
				CountedAtTheTap: counted,
				Code:            status.Code(gate.enforce(ctx)),
			}
			if diff := cmp.Diff(tt.want, got); diff != "" {
				t.Errorf("mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestAStreamThatSkippedTheTapIsRefusedAndCountedAsFailed(t *testing.T) {
	gate := NewTap(otlptest.Admitting(), benchOptions())
	type outcome struct {
		Status *status.Status
		Stats  TapStats
	}
	want := outcome{
		Status: status.New(codes.Internal, internalMessage),
		Stats: TapStats{
			Failed: 1,
		},
	}
	got := outcome{
		Status: status.Convert(gate.enforce(t.Context())),
		Stats:  gate.Stats(),
	}
	if diff := cmp.Diff(want, got, cmp.Comparer(func(a, b *status.Status) bool {
		return a.Code() == b.Code() && a.Message() == b.Message()
	})); diff != "" {
		t.Errorf("mismatch (-want +got):\n%s", diff)
	}
}

type benchDirectory struct {
	snapshot ingestauth.Snapshot
}

func (d benchDirectory) Snapshot(context.Context) ingestauth.Snapshot {
	return d.snapshot
}

func fullGrants(b *testing.B) credential.Grants {
	b.Helper()
	token, err := credential.Parse(otlptest.Token)
	if err != nil {
		b.Fatalf("Parse: %v", err)
	}
	grants := credential.Grants{}
	for index := range benchGrants {
		id := benchID(index)
		if index == 0 {
			id = token.ID
		}
		grant, err := credential.NewGrant(id, strings.Clone(benchTenant), token.Digest(), time.Unix(0, 0))
		if err != nil {
			b.Fatalf("NewGrant: %v", err)
		}
		grants[grant.ID] = grant
	}
	return grants
}

func benchID(index int) credential.ID {
	id := []byte(strings.Repeat(idAlphabet[:1], credential.IDLength))
	for position := range id {
		id[position] = idAlphabet[index%len(idAlphabet)]
		index /= len(idAlphabet)
	}
	return credential.ID(id)
}

func realTap(b *testing.B) *Tap {
	b.Helper()
	authenticator, err := ingestauth.NewAuthenticator(benchDirectory{
		snapshot: ingestauth.Snapshot{
			Grants:    fullGrants(b),
			CheckedAt: time.Unix(0, 0),
		},
	}, benchClock{}, ingestauth.Policy{
		Tenant:       benchTenant,
		MaxStaleness: benchStaleness,
		CheckHorizon: benchStaleness,
	})
	if err != nil {
		b.Fatalf("NewAuthenticator: %v", err)
	}
	return NewTap(authenticator, benchOptions())
}

func BenchmarkTap(b *testing.B) {
	gate := realTap(b)
	info := &tap.Info{
		FullMethodName: exportFullMethod,
		Header: metadata.Pairs(
			authorizationHeader, "Bearer "+otlptest.Token,
			"content-type", "application/grpc",
			"user-agent", "OTel-OTLP-Exporter-Go/1.0",
			"traceparent", "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01",
			"grpc-accept-encoding", "gzip",
		),
	}
	ctx := b.Context()
	b.ReportAllocs()
	for b.Loop() {
		decided, err := gate.handle(ctx, info)
		if err != nil {
			b.Fatalf("handle: %v", err)
		}
		if err := gate.enforce(decided); err != nil {
			b.Fatalf("enforce: %v", err)
		}
	}
}

func carriesAuthorization(ctx context.Context) bool {
	incoming, _ := metadata.FromIncomingContext(ctx)
	return len(incoming.Get(authorizationHeader)) > 0
}

func TestAServerWithoutATapRefusesToBeBuilt(t *testing.T) {
	defer func() {
		recovered := recover()
		err, isError := recovered.(error)
		if !isError || !errors.Is(err, errNoTap) {
			t.Errorf("NewServer without a tap recovered %v, want %v", recovered, errNoTap)
		}
	}()
	NewServer(nil, nil, benchOptions())
	t.Errorf("NewServer without a tap returned a server")
}
