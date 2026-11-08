package reference_test

import (
	"errors"
	"path/filepath"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"

	"github.com/DmytroLysenko1/faultline/internal/interfaces/otlp/otlptest"
	"github.com/DmytroLysenko1/faultline/test/demo/reference"
)

const (
	cart     = "svc:otel-demo/cart"
	checkout = "svc:otel-demo/checkout"
	flagd    = "svc:otel-demo/flagd"
	frontend = "svc:otel-demo/frontend"
	bareCart = "svc:/cart"
)

func TestReadSkipsCommentsAndRequiresReasonsForOptionalEdges(t *testing.T) {
	type args struct {
		text        string
		needsReason bool
	}
	tests := []struct {
		Name    string
		Args    args
		Want    map[reference.Edge]bool
		WantErr error
	}{
		{
			Name: "comments, blank lines and an edge",
			Args: args{text: "# header\n\n" + checkout + " " + cart + "\n"},
			Want: map[reference.Edge]bool{{Source: checkout, Target: cart}: true},
		},
		{
			Name: "optional edge with a reason",
			Args: args{text: cart + " " + flagd + " # flags are cached\n", needsReason: true},
			Want: map[reference.Edge]bool{{Source: cart, Target: flagd}: true},
		},
		{
			Name:    "optional edge without a reason",
			Args:    args{text: "# header\n" + cart + " " + flagd + "\n", needsReason: true},
			WantErr: reference.ErrUnjustified,
		},
		{
			Name:    "optional edge with an empty reason",
			Args:    args{text: cart + " " + flagd + " #  \n", needsReason: true},
			WantErr: reference.ErrUnjustified,
		},
		{
			Name:    "three fields",
			Args:    args{text: cart + " " + flagd + " " + checkout + "\n"},
			WantErr: reference.ErrMalformedLine,
		},
	}
	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			got, err := reference.Read(strings.NewReader(tt.Args.text), tt.Args.needsReason)
			if !errors.Is(err, tt.WantErr) {
				t.Fatalf("Read = %v, want %v", err, tt.WantErr)
			}
			if diff := cmp.Diff(tt.Want, got); diff != "" {
				t.Errorf("edges (-want +got):\n%s", diff)
			}
		})
	}
}

func TestReadNamesTheBrokenLine(t *testing.T) {
	_, err := reference.Read(strings.NewReader("# one\n# two\n"+cart+" "+flagd+"\n"), true)
	if err == nil || !strings.Contains(err.Error(), "line 3") {
		t.Errorf("Read = %v, want the error to name line 3", err)
	}
}

func TestTheVersionedReferenceLoads(t *testing.T) {
	root := otlptest.ModuleRoot(t)
	loaded, err := reference.Load(
		filepath.Join(root, "test", "demo", "otel-demo", "required-edges.txt"),
		filepath.Join(root, "test", "demo", "otel-demo", "optional-edges.txt"),
	)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if len(loaded.Required) == 0 || len(loaded.Optional) == 0 {
		t.Errorf("loaded %d required and %d optional edges, want both non-empty", len(loaded.Required), len(loaded.Optional))
	}
}

func TestCheckSortsEveryDeviation(t *testing.T) {
	expected := reference.Reference{
		Required: map[reference.Edge]bool{
			{Source: checkout, Target: cart}:     true,
			{Source: frontend, Target: checkout}: true,
		},
		Optional: map[reference.Edge]bool{
			{Source: cart, Target: flagd}: true,
		},
	}
	tests := []struct {
		Name string
		Args []reference.Edge
		Want reference.Result
	}{
		{
			Name: "every required and one optional edge",
			Args: []reference.Edge{
				{Source: checkout, Target: cart}, {Source: frontend, Target: checkout}, {Source: cart, Target: flagd},
			},
		},
		{
			Name: "a required edge is missing",
			Args: []reference.Edge{{Source: checkout, Target: cart}},
			Want: reference.Result{Missing: []reference.Edge{{Source: frontend, Target: checkout}}},
		},
		{
			Name: "an edge outside both lists",
			Args: []reference.Edge{
				{Source: checkout, Target: cart}, {Source: frontend, Target: checkout}, {Source: frontend, Target: flagd},
			},
			Want: reference.Result{Phantoms: []reference.Edge{{Source: frontend, Target: flagd}}},
		},
		{
			Name: "a node lost its namespace",
			Args: []reference.Edge{
				{Source: checkout, Target: cart}, {Source: frontend, Target: checkout}, {Source: checkout, Target: bareCart},
			},
			Want: reference.Result{
				Phantoms:    []reference.Edge{{Source: checkout, Target: bareCart}},
				Unqualified: []string{bareCart},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			got := expected.Check(tt.Args)
			if diff := cmp.Diff(tt.Want, got); diff != "" {
				t.Errorf("Check (-want +got):\n%s", diff)
			}
			if got.Passed() != (len(tt.Want.Missing)+len(tt.Want.Phantoms)+len(tt.Want.Unqualified) == 0) {
				t.Errorf("Passed() = %v for %+v", got.Passed(), got)
			}
		})
	}
}
