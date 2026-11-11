package main

import (
	"errors"
	"iter"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"

	"github.com/DmytroLysenko1/faultline/internal/application/ingest"
	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/operation"
	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/resource"
	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/telemetrytest"
	"github.com/DmytroLysenko1/faultline/internal/infrastructure/memjournal"
	"github.com/DmytroLysenko1/faultline/internal/infrastructure/routerules"
)

const (
	routeFileMode = 0o600
	catalogRules  = `version: 1
services:
  - namespace: shop
    name: catalog
    routes:
      - /products/{sku}/reviews
`
	brokenRules = `version: 1
services:
  - namespace: shop
    name: catalog
    routes:
      - /products/{sku}/reviews
      - /products/{id}/reviews
`
)

func writeRules(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "routes.yaml")
	if err := os.WriteFile(path, []byte(content), routeFileMode); err != nil {
		t.Fatalf("write rules: %v", err)
	}
	return path
}

func linkRules(t *testing.T, target string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "routes.yaml")
	if err := os.Symlink(target, path); err != nil {
		t.Fatalf("link rules: %v", err)
	}
	return path
}

func relativeLink(t *testing.T, target string) string {
	t.Helper()
	link := linkRules(t, target)
	relative, err := filepath.Rel(filepath.Dir(link), target)
	if err != nil {
		t.Fatalf("relative target: %v", err)
	}
	if err := os.Remove(link); err != nil {
		t.Fatalf("remove link: %v", err)
	}
	if err := os.Symlink(relative, link); err != nil {
		t.Fatalf("relink rules: %v", err)
	}
	return link
}

func fifo(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "routes.fifo")
	if err := syscall.Mkfifo(path, routeFileMode); err != nil {
		t.Fatalf("mkfifo: %v", err)
	}
	return path
}

func normalizedReview(rules *operation.Rules) operation.Operation {
	catalog := resource.ServiceRef{Namespace: "shop", Name: "catalog"}
	return operation.NewNormalizer(rules).Normalize(catalog, &operation.Facts{
		HTTPMethod: "GET",
		Path:       "/products/ABC123/reviews",
	})
}

// TestTheRouteRulesFileReachesTheNormalizer: without a file the built-in templating names the
// path; with one, the service's own route wins.
func TestTheRouteRulesFileReachesTheNormalizer(t *testing.T) {
	tests := []struct {
		Name string
		Args string
		Want string
	}{
		{Name: "no file", Args: "", Want: "GET /products/ABC123/reviews"},
		{Name: "a file with the catalog routes", Args: writeRules(t, catalogRules), Want: "GET /products/{sku}/reviews"},
		{
			Name: "an absolute symlink to a file in another directory",
			Args: linkRules(t, writeRules(t, catalogRules)),
			Want: "GET /products/{sku}/reviews",
		},
		{
			Name: "a relative symlink that leaves its directory",
			Args: relativeLink(t, writeRules(t, catalogRules)),
			Want: "GET /products/{sku}/reviews",
		},
	}
	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			rules, err := loadRouteRules(tt.Args)
			if err != nil {
				t.Fatalf("loadRouteRules: %v", err)
			}
			if diff := cmp.Diff(tt.Want, normalizedReview(rules).Name); diff != "" {
				t.Errorf("operation (-want +got):\n%s", diff)
			}
		})
	}
}

func TestABrokenRouteRulesFileIsAConfigError(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "absent.yaml")
	tests := []struct {
		Name    string
		Args    string
		Want    string
		WantErr error
	}{
		{
			Name:    "missing file",
			Args:    missing,
			Want:    "FAULTLINE_ROUTE_RULES_FILE " + missing + ": stat " + missing + ": no such file",
			WantErr: os.ErrNotExist,
		},
		{
			Name:    "a directory",
			Args:    t.TempDir(),
			Want:    "not a regular file (d",
			WantErr: errNotRegular,
		},
		{
			Name:    "a named pipe does not block startup",
			Args:    fifo(t),
			Want:    "not a regular file (p",
			WantErr: errNotRegular,
		},
		{
			Name:    "a rule that clashes with another names its line",
			Args:    writeRules(t, brokenRules),
			Want:    "line 7:",
			WantErr: operation.ErrInvalidRules,
		},
		{
			Name:    "a file above the size limit",
			Args:    writeRules(t, strings.Repeat("#", routerules.MaxFileBytes+1)),
			Want:    "at most 1048576",
			WantErr: operation.ErrInvalidRules,
		},
	}
	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			_, err := loadRouteRules(tt.Args)
			if !errors.Is(err, tt.WantErr) {
				t.Fatalf("loadRouteRules = %v, want %v", err, tt.WantErr)
			}
			if !strings.Contains(err.Error(), tt.Want) {
				t.Errorf("error %q does not say %q", err, tt.Want)
			}
		})
	}
}

func TestStartRefusesABrokenRouteRulesFile(t *testing.T) {
	path := writeRules(t, brokenRules)
	var stderr strings.Builder
	limits := &recordedLimit{}
	code := realMain(process{
		Args:           []string{commandAll},
		Lookup:         environment(map[string]string{envRouteRulesFile: path}),
		ReadCgroup:     noCgroup,
		SetMemoryLimit: limits.setLimit,
		Stderr:         &stderr,
	})
	if code != exitConfig || !strings.Contains(stderr.String(), path) {
		t.Errorf("realMain = %d, %q; want exit %d naming %s", code, stderr.String(), exitConfig, path)
	}
}

type oneRecord struct {
	resource resource.Resource
	record   ingest.Record
}

func (s *oneRecord) Len() int {
	return 1
}

func (s *oneRecord) All() iter.Seq2[resource.Resource, *ingest.Record] {
	return func(yield func(resource.Resource, *ingest.Record) bool) {
		yield(s.resource, &s.record)
	}
}

const ingestWait = time.Second

// TestIngestNamesOperationsWithTheRouteRules: the rules loaded at startup reach the ingest path,
// so the span stored in the journal carries the service's own route.
func TestIngestNamesOperationsWithTheRouteRules(t *testing.T) {
	rules, err := loadRouteRules(writeRules(t, catalogRules))
	if err != nil {
		t.Fatalf("loadRouteRules: %v", err)
	}
	journal, err := memjournal.New(defaultConfig().Journal, systemClock{})
	if err != nil {
		t.Fatalf("memjournal.New: %v", err)
	}
	t.Cleanup(journal.Close)
	_, accept := provideIngest(journal, rules, testLogger(&logBuffer{}))
	source := &oneRecord{
		resource: telemetrytest.ResourceWith(func(catalog *resource.Resource) {
			catalog.K8sNamespaceName, catalog.ServiceName = "shop", "catalog"
		}),
		record: ingest.Record{
			Fields: telemetrytest.ValidFields(),
			Facts: operation.Facts{
				HTTPMethod: "GET",
				Path:       "/products/ABC123/reviews",
			},
		},
	}
	if _, err := accept.Execute(t.Context(), source); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	deliveries, err := journal.Receive(t.Context(), 1, ingestWait)
	if err != nil || len(deliveries) != 1 {
		t.Fatalf("Receive = %d deliveries, %v", len(deliveries), err)
	}
	if got := deliveries[0].Batch().Spans[0].Operation.Name; got != "GET /products/{sku}/reviews" {
		t.Errorf("stored operation %q, want the catalog route", got)
	}
}
