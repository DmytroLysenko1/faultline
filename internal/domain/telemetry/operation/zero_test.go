package operation_test

import (
	"testing"

	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/operation"
)

func TestIsZeroOnlyForTheEmptyOperation(t *testing.T) {
	tests := []struct {
		Name string
		Args operation.Operation
		Want bool
	}{
		{Name: "empty", Want: true},
		{Name: "named", Args: operation.Operation{Protocol: operation.ProtocolHTTP, Name: "GET /a"}},
		{Name: "protocol only", Args: operation.Operation{Protocol: operation.ProtocolGRPC}},
	}
	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			if got := tt.Args.IsZero(); got != tt.Want {
				t.Errorf("IsZero() = %v, want %v", got, tt.Want)
			}
		})
	}
}
