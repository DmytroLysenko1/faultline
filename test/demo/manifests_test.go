package demo_test

import (
	"bytes"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/nats-io/nats-server/v2/conf"
	"go.yaml.in/yaml/v3"

	"github.com/DmytroLysenko1/faultline/internal/interfaces/otlp/otlptest"
)

const (
	kubectlBinary   = "kubectl"
	deploymentKind  = "Deployment"
	selfTracingEnv  = "SELF_TRACING_ENDPOINT"
	memoryLimitEnv  = "GOMEMLIMIT"
	serviceKind     = "Service"
	claimKind       = "PersistentVolumeClaim"
	natsStoreClaim  = "nats-store"
	roleName        = "faultline"
	natsName        = "nats"
	nameLabel       = "app.kubernetes.io/name"
	partOfLabel     = "app.kubernetes.io/part-of"
	natsConfigPath  = "nats.conf"
	natsReadiness   = "/healthz?js-enabled-only=true"
	natsStoreVolume = "store"
	matchLabelsKey  = "matchLabels"
	natsMonitorPort = 8222
	natsImagePin    = "NATS_IMAGE="
)

type document struct {
	Kind     string `yaml:"kind"`
	Metadata struct {
		Name string `yaml:"name"`
	} `yaml:"metadata"`
	Spec struct {
		Selector    map[string]any `yaml:"selector"`
		Replicas    int            `yaml:"replicas"`
		AccessModes []string       `yaml:"accessModes"`
		Resources   struct {
			Requests struct {
				Storage string `yaml:"storage"`
			} `yaml:"requests"`
		} `yaml:"resources"`
		Strategy struct {
			Type string `yaml:"type"`
		} `yaml:"strategy"`
		Template struct {
			Metadata struct {
				Labels map[string]any `yaml:"labels"`
			} `yaml:"metadata"`
			Spec podSpec `yaml:"spec"`
		} `yaml:"template"`
	} `yaml:"spec"`
}

type podSpec struct {
	AutomountServiceAccountToken  *bool      `yaml:"automountServiceAccountToken"`
	TerminationGracePeriodSeconds int        `yaml:"terminationGracePeriodSeconds"`
	SecurityContext               podContext `yaml:"securityContext"`
	Containers                    []struct {
		Image           string `yaml:"image"`
		ImagePullPolicy string `yaml:"imagePullPolicy"`
		Env             []struct {
			Name  string `yaml:"name"`
			Value string `yaml:"value"`
		} `yaml:"env"`
		ReadinessProbe probe `yaml:"readinessProbe"`
		LivenessProbe  probe `yaml:"livenessProbe"`
		Ports          []struct {
			Name          string `yaml:"name"`
			ContainerPort int    `yaml:"containerPort"`
		} `yaml:"ports"`
		VolumeMounts []struct {
			Name      string `yaml:"name"`
			MountPath string `yaml:"mountPath"`
			ReadOnly  bool   `yaml:"readOnly"`
		} `yaml:"volumeMounts"`
		Resources struct {
			Limits struct {
				Memory string `yaml:"memory"`
			} `yaml:"limits"`
		} `yaml:"resources"`
		SecurityContext containerContext `yaml:"securityContext"`
	} `yaml:"containers"`
	Volumes []struct {
		Name     string `yaml:"name"`
		EmptyDir *struct {
			Medium    string `yaml:"medium"`
			SizeLimit string `yaml:"sizeLimit"`
		} `yaml:"emptyDir"`
		PersistentVolumeClaim *struct {
			ClaimName string `yaml:"claimName"`
		} `yaml:"persistentVolumeClaim"`
	} `yaml:"volumes"`
}

type probe struct {
	HTTPGet struct {
		Path string `yaml:"path"`
		Port string `yaml:"port"`
	} `yaml:"httpGet"`
}

type disk struct {
	Mounted   bool
	OnDisk    bool
	SizeLimit bool
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
	PullsLocalImageOnly   bool
	ReadyOn               string
	LiveOn                string
	Pod                   podContext
	Container             containerContext
	MemoryLimit           string
	Env                   map[string]string
	Spool                 disk
}

type rollout struct {
	Replicas int
	Strategy string
}

type claim struct {
	AccessModes []string
	Storage     string
}

type skeleton struct {
	Deployments map[string]document
	Selectors   map[string]map[string]any
	Claims      map[string]claim
}

func renderSkeleton(t *testing.T) skeleton {
	t.Helper()
	if _, err := exec.LookPath(kubectlBinary); err != nil {
		t.Skipf("%s is not installed", kubectlBinary)
	}
	path := filepath.Join(otlptest.ModuleRoot(t), "deploy", "dev", "skeleton")
	rendered, err := exec.CommandContext(t.Context(), kubectlBinary, "kustomize", path).Output()
	if err != nil {
		t.Fatalf("kubectl kustomize %s: %v", path, err)
	}
	found := skeleton{
		Deployments: map[string]document{},
		Selectors:   map[string]map[string]any{},
		Claims:      map[string]claim{},
	}
	decoder := yaml.NewDecoder(bytes.NewReader(rendered))
	for {
		var doc document
		err := decoder.Decode(&doc)
		if errors.Is(err, io.EOF) {
			return found
		}
		if err != nil {
			t.Fatalf("decode rendered skeleton: %v", err)
		}
		found.add(&doc)
	}
}

func (s *skeleton) add(doc *document) {
	switch doc.Kind {
	case deploymentKind:
		s.Deployments[doc.Metadata.Name] = *doc
	case serviceKind:
		s.Selectors[doc.Metadata.Name] = doc.Spec.Selector
	case claimKind:
		s.Claims[doc.Metadata.Name] = claim{
			AccessModes: doc.Spec.AccessModes,
			Storage:     doc.Spec.Resources.Requests.Storage,
		}
	}
}

func (s *skeleton) pod(t *testing.T, name string) podSpec {
	t.Helper()
	deployment, found := s.Deployments[name]
	if !found {
		t.Fatalf("no %s %s in the rendered skeleton", deploymentKind, name)
	}
	spec := deployment.Spec.Template.Spec
	if len(spec.Containers) != 1 {
		t.Fatalf("%s containers = %d, want 1", name, len(spec.Containers))
	}
	return spec
}

func (p *podSpec) diskAt(path string) disk {
	return p.diskNamed(p.volumeAt(path))
}

func (p *podSpec) writableAt(path string) bool {
	for _, mount := range p.Containers[0].VolumeMounts {
		if mount.MountPath == path {
			return !mount.ReadOnly
		}
	}
	return false
}

func (p *podSpec) volumeAt(path string) string {
	for _, mount := range p.Containers[0].VolumeMounts {
		if mount.MountPath == path {
			return mount.Name
		}
	}
	return ""
}

func (s *skeleton) matchLabels() map[string]any {
	found := map[string]any{}
	for name := range s.Deployments {
		found[name] = s.Deployments[name].Spec.Selector[matchLabelsKey]
	}
	return found
}

func (s *skeleton) podsSelectedBy(selector map[string]any) []string {
	selected := []string{}
	for name := range s.Deployments {
		if selects(selector, s.Deployments[name].Spec.Template.Metadata.Labels) {
			selected = append(selected, name)
		}
	}
	slices.Sort(selected)
	return selected
}

func selects(selector, labels map[string]any) bool {
	if len(selector) == 0 {
		return false
	}
	for key, value := range selector {
		if labels[key] != value {
			return false
		}
	}
	return true
}

func (p *podSpec) claimAt(path string) string {
	name := p.volumeAt(path)
	for _, volume := range p.Volumes {
		if volume.Name == name && volume.PersistentVolumeClaim != nil {
			return volume.PersistentVolumeClaim.ClaimName
		}
	}
	return ""
}

func (p *podSpec) diskNamed(name string) disk {
	for _, volume := range p.Volumes {
		if volume.Name == name && volume.EmptyDir != nil {
			return disk{
				Mounted:   true,
				OnDisk:    volume.EmptyDir.Medium == "",
				SizeLimit: volume.EmptyDir.SizeLimit != "",
			}
		}
	}
	return disk{}
}

func hardeningOf(spec *podSpec) hardening {
	container := spec.Containers[0]
	env := map[string]string{}
	for _, variable := range container.Env {
		env[variable.Name] = variable.Value
	}
	return hardening{
		NoServiceAccountToken: spec.AutomountServiceAccountToken != nil && !*spec.AutomountServiceAccountToken,
		PullsLocalImageOnly:   container.ImagePullPolicy == "Never",
		ReadyOn:               container.ReadinessProbe.HTTPGet.Path,
		LiveOn:                container.LivenessProbe.HTTPGet.Path,
		Pod:                   spec.SecurityContext,
		Container:             container.SecurityContext,
		MemoryLimit:           container.Resources.Limits.Memory,
		Env:                   env,
		Spool:                 spec.diskAt(env["CHECKPOINT_DIR"]),
	}
}

func TestSkeletonDeploymentIsHardenedAndFitsTheMemoryBudget(t *testing.T) {
	rendered := renderSkeleton(t)
	spec := rendered.pod(t, roleName)
	noEscalation := false
	want := hardening{
		NoServiceAccountToken: true,
		PullsLocalImageOnly:   true,
		ReadyOn:               "/readyz",
		LiveOn:                "/healthz",
		Pod: podContext{
			RunAsNonRoot: true,
			RunAsUser:    65532,
			RunAsGroup:   65532,
			SeccompProfile: struct {
				Type string `yaml:"type"`
			}{
				Type: "RuntimeDefault",
			},
		},
		Container: containerContext{
			AllowPrivilegeEscalation: &noEscalation,
			ReadOnlyRootFilesystem:   true,
			Capabilities: struct {
				Drop []string `yaml:"drop"`
			}{
				Drop: []string{"ALL"},
			},
		},
		MemoryLimit: "2Gi",
		Env: map[string]string{
			"OTLP_GRPC_ADDR":   "0.0.0.0:4317",
			"API_ADDR":         "0.0.0.0:8080",
			memoryLimitEnv:     "1600MiB",
			"NATS_URL":         "nats://nats:4222",
			"CHECKPOINT_DIR":   "/spool",
			"PARTITIONS":       "1",
			"STREAM_MAX_BYTES": "2GiB",
		},
		Spool: disk{
			Mounted:   true,
			OnDisk:    true,
			SizeLimit: true,
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

func TestSkeletonRollsOutOnePodAtATime(t *testing.T) {
	rendered := renderSkeleton(t)
	tests := []struct {
		name string
		want rollout
	}{
		{
			name: roleName,
			want: rollout{
				Replicas: 1,
				Strategy: "Recreate",
			},
		},
		{
			name: natsName,
			want: rollout{
				Replicas: 1,
				Strategy: "Recreate",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			deployment := rendered.Deployments[tt.name]
			got := rollout{
				Replicas: deployment.Spec.Replicas,
				Strategy: deployment.Spec.Strategy.Type,
			}
			if diff := cmp.Diff(tt.want, got); diff != "" {
				t.Errorf("one pod at a time keeps one graph and one store (-want +got):\n%s", diff)
			}
		})
	}
}

func TestSkeletonDeploymentsKeepTheirImmutableSelectors(t *testing.T) {
	rendered := renderSkeleton(t)
	want := map[string]any{
		roleName: map[string]any{
			nameLabel:   roleName,
			partOfLabel: roleName,
		},
		natsName: map[string]any{
			nameLabel:   natsName,
			partOfLabel: roleName,
		},
	}
	if diff := cmp.Diff(want, rendered.matchLabels()); diff != "" {
		t.Errorf("deployment selectors are immutable on a live cluster (-want +got):\n%s", diff)
	}
}

func TestSkeletonServicesSelectOnlyTheirOwnPods(t *testing.T) {
	rendered := renderSkeleton(t)
	want := map[string][]string{
		roleName: {roleName},
		natsName: {natsName},
	}
	got := map[string][]string{}
	for service, selector := range rendered.Selectors {
		got[service] = rendered.podsSelectedBy(selector)
	}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("pods each service selects (-want +got):\n%s", diff)
	}
}

type natsShape struct {
	Image         string
	ReadyOn       string
	ReadyPort     string
	MonitorPort   int
	HTTPPort      int64
	StoreVolume   string
	StoreWritable bool
	StoreClaim    string
	Claim         claim
}

func TestSkeletonNATSServesJetStreamFromItsClaim(t *testing.T) {
	rendered := renderSkeleton(t)
	spec := rendered.pod(t, natsName)
	path := filepath.Join(otlptest.ModuleRoot(t), "deploy", "dev", "skeleton", natsConfigPath)
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	settings, err := conf.Parse(string(raw))
	if err != nil {
		t.Fatalf("parse %s: %v", path, err)
	}
	jetstream, _ := settings["jetstream"].(map[string]any)
	storeDir, _ := jetstream["store_dir"].(string)
	httpPort, _ := settings["http_port"].(int64)
	container := spec.Containers[0]
	got := natsShape{
		Image:         container.Image,
		ReadyOn:       container.ReadinessProbe.HTTPGet.Path,
		ReadyPort:     container.ReadinessProbe.HTTPGet.Port,
		HTTPPort:      httpPort,
		StoreVolume:   spec.volumeAt(storeDir),
		StoreWritable: spec.writableAt(storeDir),
		StoreClaim:    spec.claimAt(storeDir),
	}
	got.Claim = rendered.Claims[got.StoreClaim]
	for _, port := range container.Ports {
		if port.Name == got.ReadyPort {
			got.MonitorPort = port.ContainerPort
		}
	}
	want := natsShape{
		Image:         pinnedVersion(t, natsImagePin),
		ReadyOn:       natsReadiness,
		ReadyPort:     "monitor",
		MonitorPort:   natsMonitorPort,
		HTTPPort:      natsMonitorPort,
		StoreVolume:   natsStoreVolume,
		StoreWritable: true,
		StoreClaim:    natsStoreClaim,
		Claim: claim{
			AccessModes: []string{"ReadWriteOnce"},
			Storage:     "6Gi",
		},
	}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("nats in the skeleton (-want +got):\n%s", diff)
	}
}

func pinnedVersion(t *testing.T, pin string) string {
	t.Helper()
	path := filepath.Join(otlptest.ModuleRoot(t), "hack", "versions.env")
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	for line := range strings.Lines(string(raw)) {
		if value, found := strings.CutPrefix(strings.TrimSpace(line), pin); found {
			return value
		}
	}
	t.Fatalf("%s has no %s", path, pin)
	return ""
}
