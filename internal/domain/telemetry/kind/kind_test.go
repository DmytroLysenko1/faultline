package kind_test

import (
	"testing"

	"github.com/google/go-cmp/cmp"

	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/kind"
)

func TestNormalized(t *testing.T) {
	tests := []struct {
		Name    string
		Args    kind.Kind
		Want    kind.Kind
		WantErr error
	}{
		{
			Name: "unspecified stays",
			Args: kind.Unspecified,
			Want: kind.Unspecified,
		},
		{
			Name: "server stays",
			Args: kind.Server,
			Want: kind.Server,
		},
		{
			Name: "consumer is the last known kind",
			Args: kind.Consumer,
			Want: kind.Consumer,
		},
		{
			Name: "one past consumer becomes unspecified",
			Args: kind.Consumer + 1,
			Want: kind.Unspecified,
		},
		{
			Name: "the largest value becomes unspecified",
			Args: kind.Kind(255),
			Want: kind.Unspecified,
		},
	}
	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			if diff := cmp.Diff(tt.Want, tt.Args.Normalized()); diff != "" {
				t.Errorf("Normalized mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestValuesMatchOTLP(t *testing.T) {
	got := []kind.Kind{
		kind.Unspecified,
		kind.Internal,
		kind.Server,
		kind.Client,
		kind.Producer,
		kind.Consumer,
	}
	if diff := cmp.Diff([]kind.Kind{
		0,
		1,
		2,
		3,
		4,
		5,
	}, got); diff != "" {
		t.Errorf("kind values drifted from the OTLP enum (-want +got):\n%s", diff)
	}
}
