package call_test

import (
	"testing"

	"github.com/google/go-cmp/cmp"

	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/eventtime"
	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/operation"
	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/telemetrytest"
	"github.com/DmytroLysenko1/faultline/internal/domain/topology/call"
)

func TestBetweenTimesTheCallByTheCallerAndNamesItByTheCallee(t *testing.T) {
	route := operation.Operation{Protocol: operation.ProtocolHTTP, Name: "GET /cart/{id}"}
	path := operation.Operation{Protocol: operation.ProtocolHTTP, Name: "GET /cart/{id}/items"}
	callerTime := eventtime.FromUnixNanos(100, 900)
	calleeTime := eventtime.FromUnixNanos(200, 800)
	party := func(name string, interval eventtime.Interval, op operation.Operation) call.Party {
		return call.Party{Service: telemetrytest.Service(name), Interval: interval, Operation: op}
	}
	tests := []struct {
		Name string
		Args [2]call.Party
		Want call.Call
	}{
		{
			Name: "callee operation wins",
			Args: [2]call.Party{party("frontend", callerTime, path), party("cart", calleeTime, route)},
			Want: call.Call{Caller: telemetrytest.Service("frontend"), Callee: telemetrytest.Service("cart"),
				Interval: callerTime, Operation: route},
		},
		{
			Name: "caller operation when the callee has none",
			Args: [2]call.Party{party("frontend", callerTime, path), party("cart", calleeTime, operation.Operation{})},
			Want: call.Call{Caller: telemetrytest.Service("frontend"), Callee: telemetrytest.Service("cart"),
				Interval: callerTime, Operation: path},
		},
		{
			Name: "untimed caller leaves the call untimed",
			Args: [2]call.Party{party("frontend", eventtime.Interval{}, path), party("cart", calleeTime, route)},
			Want: call.Call{Caller: telemetrytest.Service("frontend"), Callee: telemetrytest.Service("cart"),
				Operation: route},
		},
	}
	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			if diff := cmp.Diff(tt.Want, call.Between(&tt.Args[0], &tt.Args[1])); diff != "" {
				t.Errorf("Between mismatch (-want +got):\n%s", diff)
			}
		})
	}
}
