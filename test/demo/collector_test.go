package demo_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
	"go.yaml.in/yaml/v3"

	"github.com/DmytroLysenko1/faultline/internal/interfaces/otlp/otlptest"
)

const (
	faultlineValues   = "otel-demo-faultline-values.yaml"
	faultlineExporter = "otlp_grpc/faultline"
	authorizationKey  = "authorization"
	envReferenceStart = "${env:"
	envReferenceEnd   = "}"
)

type secretKeyRef struct {
	Name     string `yaml:"name"`
	Key      string `yaml:"key"`
	Optional bool   `yaml:"optional"`
}

type collectorValues struct {
	Collector struct {
		ExtraEnvs []struct {
			Name      string `yaml:"name"`
			ValueFrom struct {
				SecretKeyRef *secretKeyRef `yaml:"secretKeyRef"`
			} `yaml:"valueFrom"`
		} `yaml:"extraEnvs"`
		Config struct {
			Exporters map[string]struct {
				Headers map[string]string `yaml:"headers"`
			} `yaml:"exporters"`
		} `yaml:"config"`
	} `yaml:"opentelemetry-collector"`
}

type collectorAuth struct {
	Header string
	Secret *secretKeyRef
}

func readCollectorValues(t *testing.T) collectorValues {
	t.Helper()
	path := filepath.Join(otlptest.ModuleRoot(t), "deploy", "dev", faultlineValues)
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	var values collectorValues
	if err := yaml.Unmarshal(raw, &values); err != nil {
		t.Fatalf("decode %s: %v", path, err)
	}
	return values
}

func (v *collectorValues) secretOf(variable string) *secretKeyRef {
	for _, env := range v.Collector.ExtraEnvs {
		if env.Name == variable {
			return env.ValueFrom.SecretKeyRef
		}
	}
	return nil
}

func referencedVariable(header string) string {
	_, rest, found := strings.Cut(header, envReferenceStart)
	if !found {
		return ""
	}
	variable, _, _ := strings.Cut(rest, envReferenceEnd)
	return variable
}

func TestTheCollectorSendsTheIngestTokenFromItsSecret(t *testing.T) {
	values := readCollectorValues(t)
	header := values.Collector.Config.Exporters[faultlineExporter].Headers[authorizationKey]
	got := collectorAuth{
		Header: header,
		Secret: values.secretOf(referencedVariable(header)),
	}
	want := collectorAuth{
		Header: "Bearer ${env:INGEST_TOKEN}",
		Secret: &secretKeyRef{
			Name: "ingest-token",
			Key:  "token",
		},
	}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("the %s exporter authenticates with the token the demo stores (-want +got):\n%s", faultlineExporter, diff)
	}
}
