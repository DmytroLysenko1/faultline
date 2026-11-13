package resource_test

import (
	"bytes"
	"errors"
	"strings"
	"testing"

	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/resource"
	"github.com/DmytroLysenko1/faultline/pkg/checkpoint"
)

func readService(write func(w *checkpoint.Writer)) (resource.ServiceRef, error) {
	var out bytes.Buffer
	w := checkpoint.NewWriter(&out, 1)
	w.Begin(1)
	write(w)
	w.End()
	r := checkpoint.NewReader(&out)
	r.Begin(1)
	read := resource.ReadServiceRef(r)
	r.End()
	return read, r.Finish()
}

func TestAServiceRefReadsBackAndRefusesWhatResourceWouldRefuse(t *testing.T) {
	valid := resource.ServiceRef{Namespace: "shop", Name: "checkout"}
	tests := []struct {
		name    string
		write   func(w *checkpoint.Writer)
		want    resource.ServiceRef
		wantErr error
	}{
		{name: "a valid service", write: valid.WriteCheckpoint, want: valid},
		{name: "no name", write: resource.ServiceRef{Namespace: "shop"}.WriteCheckpoint, wantErr: checkpoint.ErrCorrupt},
		{name: "a control character", write: resource.ServiceRef{Name: "a\x01"}.WriteCheckpoint,
			wantErr: checkpoint.ErrCorrupt},
		{name: "a name over its limit", write: func(w *checkpoint.Writer) {
			w.Text("")
			w.Text(strings.Repeat("a", resource.MaxServiceNameBytes+1))
		}, wantErr: checkpoint.ErrCorrupt},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := readService(tt.write)

			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("err = %v, want %v", err, tt.wantErr)
			}
			if tt.wantErr == nil && got != tt.want {
				t.Errorf("read %+v, want %+v", got, tt.want)
			}
		})
	}
}
