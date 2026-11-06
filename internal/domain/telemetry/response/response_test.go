package response_test

import (
	"testing"

	"github.com/google/go-cmp/cmp"

	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/response"
)

func TestFailed(t *testing.T) {
	type args struct {
		Attributes  map[string]string
		StatusError bool
	}
	tests := []struct {
		Name    string
		Args    args
		Want    bool
		WantErr error
	}{
		{
			Name: "nothing set",
			Args: args{},
			Want: false,
		},
		{
			Name: "status ERROR only",
			Args: args{
				StatusError: true,
			},
			Want: true,
		},
		{
			Name: "error.type only",
			Args: args{
				Attributes: map[string]string{
					"error.type": "connection refused",
				},
			},
			Want: true,
		},
		{
			Name: "gRPC OK by name",
			Args: args{
				Attributes: map[string]string{
					"rpc.response.status_code": "OK",
				},
			},
			Want: false,
		},
		{
			Name: "gRPC NOT_FOUND by name",
			Args: args{
				Attributes: map[string]string{
					"rpc.response.status_code": "NOT_FOUND",
				},
			},
			Want: false,
		},
		{
			Name: "gRPC ALREADY_EXISTS by name",
			Args: args{
				Attributes: map[string]string{
					"rpc.response.status_code": "ALREADY_EXISTS",
				},
			},
			Want: false,
		},
		{
			Name: "gRPC CANCELLED by name",
			Args: args{
				Attributes: map[string]string{
					"rpc.response.status_code": "CANCELLED",
				},
			},
			Want: false,
		},
		{
			Name: "gRPC UNAVAILABLE by name",
			Args: args{
				Attributes: map[string]string{
					"rpc.response.status_code": "UNAVAILABLE",
				},
			},
			Want: true,
		},
		{
			Name: "gRPC DEADLINE_EXCEEDED by name",
			Args: args{
				Attributes: map[string]string{
					"rpc.response.status_code": "DEADLINE_EXCEEDED",
				},
			},
			Want: true,
		},
		{
			Name: "legacy gRPC 0",
			Args: args{
				Attributes: map[string]string{
					"rpc.grpc.status_code": "0",
				},
			},
			Want: false,
		},
		{
			Name: "legacy gRPC 1 CANCELLED",
			Args: args{
				Attributes: map[string]string{
					"rpc.grpc.status_code": "1",
				},
			},
			Want: false,
		},
		{
			Name: "legacy gRPC 5 NOT_FOUND",
			Args: args{
				Attributes: map[string]string{
					"rpc.grpc.status_code": "5",
				},
			},
			Want: false,
		},
		{
			Name: "legacy gRPC 6 ALREADY_EXISTS",
			Args: args{
				Attributes: map[string]string{
					"rpc.grpc.status_code": "6",
				},
			},
			Want: false,
		},
		{
			Name: "legacy gRPC 14 UNAVAILABLE",
			Args: args{
				Attributes: map[string]string{
					"rpc.grpc.status_code": "14",
				},
			},
			Want: true,
		},
		{
			Name: "NOT_FOUND code wins over error.type NOT_FOUND",
			Args: args{
				Attributes: map[string]string{
					"rpc.response.status_code": "NOT_FOUND",
					"error.type":               "NOT_FOUND",
				},
			},
			Want: false,
		},
		{
			Name: "gRPC code wins over HTTP code",
			Args: args{
				Attributes: map[string]string{
					"rpc.response.status_code":  "OK",
					"http.response.status_code": "503",
				},
			},
			Want: false,
		},
		{
			Name: "HTTP 404",
			Args: args{
				Attributes: map[string]string{
					"http.response.status_code": "404",
				},
			},
			Want: false,
		},
		{
			Name: "HTTP 499",
			Args: args{
				Attributes: map[string]string{
					"http.response.status_code": "499",
				},
			},
			Want: false,
		},
		{
			Name: "HTTP 500",
			Args: args{
				Attributes: map[string]string{
					"http.response.status_code": "500",
				},
			},
			Want: true,
		},
		{
			Name: "HTTP 503",
			Args: args{
				Attributes: map[string]string{
					"http.response.status_code": "503",
				},
			},
			Want: true,
		},
		{
			Name: "legacy HTTP 503",
			Args: args{
				Attributes: map[string]string{
					"http.status_code": "503",
				},
			},
			Want: true,
		},
		{
			Name: "non-numeric HTTP code falls back to legacy",
			Args: args{
				Attributes: map[string]string{
					"http.response.status_code": "n/a",
					"http.status_code":          "503",
				},
			},
			Want: true,
		},
		{
			Name: "non-numeric HTTP code counts as no code",
			Args: args{
				Attributes: map[string]string{
					"http.response.status_code": "n/a",
				},
				StatusError: true,
			},
			Want: true,
		},
		{
			Name: "code wins over status ERROR",
			Args: args{
				Attributes: map[string]string{
					"http.response.status_code": "404",
				},
				StatusError: true,
			},
			Want: false,
		},
		{
			Name: "error.type ignored when a code is present",
			Args: args{
				Attributes: map[string]string{
					"http.response.status_code": "200",
					"error.type":                "timeout",
				},
			},
			Want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			got := response.Attributes(tt.Args.Attributes).Failed(tt.Args.StatusError)
			if diff := cmp.Diff(tt.Want, got); diff != "" {
				t.Errorf("Failed mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestCallsExternalSystem(t *testing.T) {
	tests := []struct {
		Name    string
		Args    map[string]string
		Want    bool
		WantErr error
	}{
		{
			Name: "no attributes",
			Args: nil,
			Want: false,
		},
		{
			Name: "db.system.name",
			Args: map[string]string{
				"db.system.name": "postgresql",
			},
			Want: true,
		},
		{
			Name: "legacy db.system",
			Args: map[string]string{
				"db.system": "redis",
			},
			Want: true,
		},
		{
			Name: "messaging.system",
			Args: map[string]string{
				"messaging.system": "kafka",
			},
			Want: true,
		},
		{
			Name: "empty value is absent",
			Args: map[string]string{
				"db.system.name": "",
			},
			Want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			if diff := cmp.Diff(tt.Want, response.Attributes(tt.Args).CallsExternalSystem()); diff != "" {
				t.Errorf("CallsExternalSystem mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestFailedDoesNotAllocate(t *testing.T) {
	const runs = 100
	tests := []struct {
		Name string
		Args response.Attributes
	}{
		{
			Name: "no response code attributes",
			Args: response.Attributes{
				"service.version": "1.0.0",
			},
		},
		{
			Name: "numeric gRPC code",
			Args: response.Attributes{
				"rpc.grpc.status_code": "14",
			},
		},
		{
			Name: "numeric HTTP code",
			Args: response.Attributes{
				"http.response.status_code": "503",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			allocations := testing.AllocsPerRun(runs, func() {
				tt.Args.Failed(false)
			})
			if allocations != 0 {
				t.Errorf("Failed allocates %.0f times per span, want 0 on the ingest hot path", allocations)
			}
		})
	}
}
