package role_test

import (
	"testing"

	"github.com/google/go-cmp/cmp"

	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/ids"
	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/kind"
	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/telemetrytest"
	"github.com/DmytroLysenko1/faultline/internal/domain/topology/role"
)

func TestRoleOf(t *testing.T) {
	type want struct {
		Key  ids.CallKey
		Role role.Role
	}
	tests := []struct {
		Name    string
		Args    telemetrytest.SpanSpec
		Want    want
		WantErr error
	}{
		{
			Name: "client is the caller keyed by its own span id",
			Args: telemetrytest.SpanSpec{
				Kind:    kind.Client,
				Trace:   1,
				SpanID:  7,
				Service: "frontend",
			},
			Want: want{
				Key: ids.NewCallKey(ids.TraceID{
					1,
				}, ids.SpanID{
					7,
				}),
				Role: role.Caller,
			},
		},
		{
			Name: "server is the callee keyed by its parent span id",
			Args: telemetrytest.SpanSpec{
				Kind:     kind.Server,
				Trace:    1,
				SpanID:   8,
				ParentID: 7,
				Service:  "cart",
			},
			Want: want{
				Key: ids.NewCallKey(ids.TraceID{
					1,
				}, ids.SpanID{
					7,
				}),
				Role: role.Callee,
			},
		},
		{
			Name: "client to a database never gets a pair",
			Args: telemetrytest.SpanSpec{
				Kind:    kind.Client,
				Trace:   1,
				SpanID:  7,
				Service: "cart",
				Attributes: map[string]string{
					"db.system.name": "redis",
				},
			},
			Want: want{
				Role: role.Unpairable,
			},
		},
		{
			Name: "client to a broker never gets a pair",
			Args: telemetrytest.SpanSpec{
				Kind:    kind.Client,
				Trace:   1,
				SpanID:  7,
				Service: "checkout",
				Attributes: map[string]string{
					"messaging.system": "kafka",
				},
			},
			Want: want{
				Role: role.Unpairable,
			},
		},
		{
			Name: "root server span has no caller",
			Args: telemetrytest.SpanSpec{
				Kind:    kind.Server,
				Trace:   1,
				SpanID:  8,
				Service: "frontend",
			},
			Want: want{
				Role: role.Unpairable,
			},
		},
		{
			Name: "internal",
			Args: telemetrytest.SpanSpec{
				Kind:    kind.Internal,
				Trace:   1,
				SpanID:  9,
				Service: "cart",
			},
			Want: want{
				Role: role.Ignored,
			},
		},
		{
			Name: "producer",
			Args: telemetrytest.SpanSpec{
				Kind:    kind.Producer,
				Trace:   1,
				SpanID:  9,
				Service: "checkout",
			},
			Want: want{
				Role: role.Ignored,
			},
		},
		{
			Name: "consumer",
			Args: telemetrytest.SpanSpec{
				Kind:    kind.Consumer,
				Trace:   1,
				SpanID:  9,
				Service: "accounting",
			},
			Want: want{
				Role: role.Ignored,
			},
		},
		{
			Name: "unspecified",
			Args: telemetrytest.SpanSpec{
				Kind:    kind.Unspecified,
				Trace:   1,
				SpanID:  9,
				Service: "cart",
			},
			Want: want{
				Role: role.Ignored,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			key, got := role.Of(telemetrytest.NewSpan(t, tt.Args))
			if diff := cmp.Diff(tt.Want, want{
				Key:  key,
				Role: got,
			}); diff != "" {
				t.Errorf("roleOf mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestRoleParticipation(t *testing.T) {
	type want struct {
		Participates bool
		Pairable     bool
	}
	tests := []struct {
		Name    string
		Args    role.Role
		Want    want
		WantErr error
	}{
		{
			Name: "ignored kinds stay out of accounting",
			Args: role.Ignored,
			Want: want{},
		},
		{
			Name: "unpairable spans are counted but never wait",
			Args: role.Unpairable,
			Want: want{
				Participates: true,
			},
		},
		{
			Name: "caller",
			Args: role.Caller,
			Want: want{
				Participates: true,
				Pairable:     true,
			},
		},
		{
			Name: "callee",
			Args: role.Callee,
			Want: want{
				Participates: true,
				Pairable:     true,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			got := want{
				Participates: tt.Args.Participates(),
				Pairable:     tt.Args.Pairable(),
			}
			if diff := cmp.Diff(tt.Want, got); diff != "" {
				t.Errorf("participation mismatch (-want +got):\n%s", diff)
			}
		})
	}
}
