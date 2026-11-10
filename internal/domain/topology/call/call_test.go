package call_test

import (
	"testing"

	"github.com/google/go-cmp/cmp"

	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/telemetrytest"
	"github.com/DmytroLysenko1/faultline/internal/domain/topology/call"
)

func TestBetween(t *testing.T) {
	type args struct {
		CallerFailed bool
		CalleeFailed bool
	}
	tests := []struct {
		Name    string
		Args    args
		Want    bool
		WantErr error
	}{
		{
			Name: "both sides succeeded",
			Args: args{},
			Want: false,
		},
		{
			Name: "caller gave up while callee answered",
			Args: args{
				CallerFailed: true,
			},
			Want: true,
		},
		{
			Name: "callee failed while caller saw success",
			Args: args{
				CalleeFailed: true,
			},
			Want: true,
		},
		{
			Name: "both sides failed",
			Args: args{
				CallerFailed: true,
				CalleeFailed: true,
			},
			Want: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			caller := call.Party{
				Service: telemetrytest.Service("frontend"),
				Failed:  tt.Args.CallerFailed,
			}
			callee := call.Party{
				Service: telemetrytest.Service("cart"),
				Failed:  tt.Args.CalleeFailed,
			}
			want := call.Call{
				Caller: caller.Service,
				Callee: callee.Service,
				Failed: tt.Want,
			}
			if diff := cmp.Diff(want, call.Between(&caller, &callee)); diff != "" {
				t.Errorf("call mismatch (-want +got):\n%s", diff)
			}
		})
	}
}
