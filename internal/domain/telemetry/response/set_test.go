package response_test

import (
	"testing"

	"github.com/google/go-cmp/cmp"

	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/response"
)

func TestSetFillsTheFieldOfEachDecidingKey(t *testing.T) {
	tests := []struct {
		Name string
		Args string
		Want response.Attributes
	}{
		{Name: "rpc.response.status_code", Args: "rpc.response.status_code",
			Want: response.Attributes{RPCResponseStatusCode: "v"}},
		{Name: "rpc.grpc.status_code", Args: "rpc.grpc.status_code", Want: response.Attributes{RPCGRPCStatusCode: "v"}},
		{Name: "http.response.status_code", Args: "http.response.status_code",
			Want: response.Attributes{HTTPResponseStatusCode: "v"}},
		{Name: "http.status_code", Args: "http.status_code", Want: response.Attributes{HTTPStatusCode: "v"}},
		{Name: "error.type", Args: "error.type", Want: response.Attributes{ErrorType: "v"}},
		{Name: "db.system.name", Args: "db.system.name", Want: response.Attributes{DBSystemName: "v"}},
		{Name: "db.system", Args: "db.system", Want: response.Attributes{DBSystem: "v"}},
		{Name: "messaging.system", Args: "messaging.system", Want: response.Attributes{MessagingSystem: "v"}},
		{Name: "unrelated key is ignored", Args: "service.version", Want: response.Attributes{}},
	}
	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			var got response.Attributes
			accepted := got.Set(tt.Args, "v")
			if diff := cmp.Diff(tt.Want, got); diff != "" {
				t.Errorf("attributes mismatch (-want +got):\n%s", diff)
			}
			want := tt.Want != response.Attributes{}
			if diff := cmp.Diff([2]bool{want, want}, [2]bool{accepted, response.Relevant(tt.Args)}); diff != "" {
				t.Errorf("[Set accepted, Relevant] mismatch (-want +got):\n%s", diff)
			}
		})
	}
}
