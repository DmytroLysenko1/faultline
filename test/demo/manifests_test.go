package demo_test

import (
	"bytes"
	"errors"
	"io"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/google/go-cmp/cmp"
	"go.yaml.in/yaml/v3"

	"github.com/DmytroLysenko1/faultline/internal/interfaces/otlp/otlptest"
)

const (
	kubectlBinary     = "kubectl"
	deploymentKind    = "Deployment"
	selfTracingEnv    = "FAULTLINE_SELF_TRACING_ENDPOINT"
	memoryLimitEnv    = "GOMEMLIMIT"
	shutdownBudgetSec = 10
)

type document struct {
	Kind string `yaml:"kind"`
	Spec struct {
		Replicas int `yaml:"replicas"`
		Strategy struct {
			Type string `yaml:"type"`
		} `yaml:"strategy"`
		Template struct {
			Spec podSpec `yaml:"spec"`
		} `yaml:"template"`
	} `yaml:"spec"`
}

type podSpec struct {
	AutomountServiceAccountToken  *bool      `yaml:"automountServiceAccountToken"`
	TerminationGracePeriodSeconds int        `yaml:"terminationGracePeriodSeconds"`
	SecurityContext               podContext `yaml:"securityContext"`
	Containers                    []struct {
		ImagePullPolicy string `yaml:"imagePullPolicy"`
		Env             []struct {
			Name  string `yaml:"name"`
			Value string `yaml:"value"`
		} `yaml:"env"`
		ReadinessProbe struct {
			HTTPGet struct {
				Path string `yaml:"path"`
			} `yaml:"httpGet"`
		} `yaml:"readinessProbe"`
		Resources struct {
			Limits struct {
				Memory string `yaml:"memory"`
			} `yaml:"limits"`
		} `yaml:"resources"`
		SecurityContext containerContext `yaml:"securityContext"`
	} `yaml:"containers"`
}

type podContext struct {
	RunAsNonRoot   bool `yaml:"runAsNonRoot"`
	RunAsUser      int  `yaml:"runAsUser"`
	RunAsGroup     int  `yaml:"runAsGroup"`
	SeccompProfile struct {
		Type string `yaml:"type"`
	} `yaml:"seccompProfile"`
}

type containerContext struct {
	AllowPrivilegeEscalation *bool `yaml:"allowPrivilegeEscalation"`
	ReadOnlyRootFilesystem   bool  `yaml:"readOnlyRootFilesystem"`
	Capabilities             struct {
		Drop []string `yaml:"drop"`
	} `yaml:"capabilities"`
}

type hardening struct {
	NoServiceAccountToken bool
	GraceCoversShutdown   bool
	PullsLocalImageOnly   bool
	ReadyOnHealthz        bool
	Pod                   podContext
	Container             containerContext
	MemoryLimit           string
	Env                   map[string]string
}

type rollout struct {
	Replicas int
	Strategy string
}

func renderSkeleton(t *testing.T) (rollout, podSpec) {
	t.Helper()
	if _, err := exec.LookPath(kubectlBinary); err != nil {
		t.Skipf("%s is not installed", kubectlBinary)
	}
	path := filepath.Join(otlptest.ModuleRoot(t), "deploy", "dev", "skeleton")
	rendered, err := exec.CommandContext(t.Context(), kubectlBinary, "kustomize", path).Output()
	if err != nil {
		t.Fatalf("kubectl kustomize %s: %v", path, err)
	}
	decoder := yaml.NewDecoder(bytes.NewReader(rendered))
	for {
		var doc document
		err := decoder.Decode(&doc)
		if errors.Is(err, io.EOF) {
			t.Fatalf("no %s in the rendered skeleton", deploymentKind)
		}
		if err != nil {
			t.Fatalf("decode rendered skeleton: %v", err)
		}
		if doc.Kind == deploymentKind {
			return rollout{Replicas: doc.Spec.Replicas, Strategy: doc.Spec.Strategy.Type}, doc.Spec.Template.Spec
		}
	}
}

func hardeningOf(spec *podSpec) hardening {
	container := spec.Containers[0]
	env := map[string]string{}
	for _, variable := range container.Env {
		env[variable.Name] = variable.Value
	}
	return hardening{
		NoServiceAccountToken: spec.AutomountServiceAccountToken != nil && !*spec.AutomountServiceAccountToken,
		GraceCoversShutdown:   spec.TerminationGracePeriodSeconds > shutdownBudgetSec,
		PullsLocalImageOnly:   container.ImagePullPolicy == "Never",
		ReadyOnHealthz:        container.ReadinessProbe.HTTPGet.Path == "/healthz",
		Pod:                   spec.SecurityContext,
		Container:             container.SecurityContext,
		MemoryLimit:           container.Resources.Limits.Memory,
		Env:                   env,
	}
}

func TestSkeletonDeploymentIsHardenedAndFitsTheMemoryBudget(t *testing.T) {
	deployment, spec := renderSkeleton(t)
	if diff := cmp.Diff(rollout{Replicas: 1, Strategy: "Recreate"}, deployment); diff != "" {
		t.Errorf("one pod at a time keeps one in-memory graph (-want +got):\n%s", diff)
	}
	if len(spec.Containers) != 1 {
		t.Fatalf("containers = %d, want 1", len(spec.Containers))
	}
	noEscalation := false
	want := hardening{
		NoServiceAccountToken: true,
		GraceCoversShutdown:   true,
		PullsLocalImageOnly:   true,
		ReadyOnHealthz:        true,
		Pod: podContext{
			RunAsNonRoot: true,
			RunAsUser:    65532,
			RunAsGroup:   65532,
			SeccompProfile: struct {
				Type string `yaml:"type"`
			}{Type: "RuntimeDefault"},
		},
		Container: containerContext{
			AllowPrivilegeEscalation: &noEscalation,
			ReadOnlyRootFilesystem:   true,
			Capabilities: struct {
				Drop []string `yaml:"drop"`
			}{Drop: []string{"ALL"}},
		},
		MemoryLimit: "2Gi",
		Env: map[string]string{
			"FAULTLINE_OTLP_GRPC_ADDR": "0.0.0.0:4317",
			"FAULTLINE_API_ADDR":       "0.0.0.0:8080",
			memoryLimitEnv:             "1600MiB",
		},
	}
	got := hardeningOf(&spec)
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("skeleton deployment (-want +got):\n%s", diff)
	}
	if _, set := got.Env[selfTracingEnv]; set {
		t.Errorf("%s is set: a faultline node would be a phantom edge in the demo graph", selfTracingEnv)
	}
}
