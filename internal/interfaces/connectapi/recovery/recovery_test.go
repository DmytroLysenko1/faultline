package recovery_test

import (
	"bytes"
	"errors"
	"log/slog"
	"net/http"
	"strings"
	"testing"

	"connectrpc.com/connect"
	"github.com/google/go-cmp/cmp"

	"github.com/DmytroLysenko1/faultline/internal/interfaces/connectapi/recovery"
)

const procedure = "/faultline.v1.GraphService/GetGraph"

func TestPanicBecomesInternalAndIsLogged(t *testing.T) {
	var logs bytes.Buffer
	handle := recovery.Handler(slog.New(slog.NewTextHandler(&logs, nil)))
	err := handle(t.Context(), connect.Spec{
		Procedure: procedure,
	}, http.Header{}, "boom: secret detail")
	if diff := cmp.Diff(connect.CodeInternal, connect.CodeOf(err)); diff != "" {
		t.Errorf("code mismatch (-want +got):\n%s", diff)
	}
	if !errors.Is(err, recovery.ErrPanic) {
		t.Errorf("err = %v, want %v", err, recovery.ErrPanic)
	}
	if strings.Contains(err.Error(), "secret detail") {
		t.Errorf("the panic value leaked to the client: %v", err)
	}
	wantFragments := []string{
		"api.panic",
		"procedure=" + procedure,
		"boom: secret detail",
		"stack=",
	}
	for _, want := range wantFragments {
		if !strings.Contains(logs.String(), want) {
			t.Errorf("log %q does not contain %q", logs.String(), want)
		}
	}
}
