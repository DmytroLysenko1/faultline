package main

import (
	"bytes"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/conf"
	"go.yaml.in/yaml/v3"

	"github.com/DmytroLysenko1/faultline/internal/infrastructure/natsjs"
	"github.com/DmytroLysenko1/faultline/internal/interfaces/otlp/otlptest"
)

const (
	roleManifest       = "deployment.yaml"
	natsManifest       = "nats.yaml"
	natsConfigFile     = "nats.conf"
	manifestKind       = "Deployment"
	claimKind          = "PersistentVolumeClaim"
	killSlack          = 2 * time.Second
	spoolSlack         = 64 * mib
	spoolImages        = 2
	sleepCommand       = "sleep"
	quantityGi         = "Gi"
	quantityMi         = "Mi"
	natsJetStreamKey   = "jetstream"
	natsFileStoreKey   = "max_file_store"
	natsMaxPayloadKey  = "max_payload"
	natsStoreDirKey    = "store_dir"
	manifestEnvMissing = "the manifest does not set it"
)

var errUnpricedPreStop = errors.New("preStop is neither a sleep action nor an exec of sleep")

type manifest struct {
	Kind     string `yaml:"kind"`
	Metadata struct {
		Name string `yaml:"name"`
	} `yaml:"metadata"`
	Spec struct {
		Template struct {
			Spec manifestPod `yaml:"spec"`
		} `yaml:"template"`
		Resources struct {
			Requests struct {
				Storage string `yaml:"storage"`
			} `yaml:"requests"`
		} `yaml:"resources"`
	} `yaml:"spec"`
}

type manifestPod struct {
	TerminationGracePeriodSeconds int `yaml:"terminationGracePeriodSeconds"`
	Containers                    []struct {
		Env []struct {
			Name  string `yaml:"name"`
			Value string `yaml:"value"`
		} `yaml:"env"`
		Lifecycle struct {
			PreStop *manifestHandler `yaml:"preStop"`
		} `yaml:"lifecycle"`
		VolumeMounts []struct {
			Name      string `yaml:"name"`
			MountPath string `yaml:"mountPath"`
		} `yaml:"volumeMounts"`
		Resources struct {
			Limits struct {
				Memory string `yaml:"memory"`
			} `yaml:"limits"`
		} `yaml:"resources"`
	} `yaml:"containers"`
	Volumes []struct {
		Name     string `yaml:"name"`
		EmptyDir *struct {
			SizeLimit string `yaml:"sizeLimit"`
		} `yaml:"emptyDir"`
		PersistentVolumeClaim *struct {
			ClaimName string `yaml:"claimName"`
		} `yaml:"persistentVolumeClaim"`
	} `yaml:"volumes"`
}

type manifestHandler struct {
	Sleep *struct {
		Seconds int `yaml:"seconds"`
	} `yaml:"sleep"`
	Exec *struct {
		Command []string `yaml:"command"`
	} `yaml:"exec"`
}

func skeletonFile(t *testing.T, name string) []byte {
	t.Helper()
	path := filepath.Join(otlptest.ModuleRoot(t), "deploy", "dev", "skeleton", name)
	content, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return content
}

func skeletonDocuments(t *testing.T, name string) []manifest {
	t.Helper()
	decoder := yaml.NewDecoder(bytes.NewReader(skeletonFile(t, name)))
	var documents []manifest
	for {
		var doc manifest
		err := decoder.Decode(&doc)
		if errors.Is(err, io.EOF) {
			return documents
		}
		if err != nil {
			t.Fatalf("decode %s: %v", name, err)
		}
		documents = append(documents, doc)
	}
}

func skeletonPod(t *testing.T, name string) manifestPod {
	t.Helper()
	for _, doc := range skeletonDocuments(t, name) {
		if doc.Kind != manifestKind {
			continue
		}
		if containers := len(doc.Spec.Template.Spec.Containers); containers != 1 {
			t.Fatalf("%s: %s containers = %d, want 1", name, manifestKind, containers)
		}
		return doc.Spec.Template.Spec
	}
	t.Fatalf("no %s in %s", manifestKind, name)
	return manifestPod{}
}

func skeletonClaimBytes(t *testing.T, name, claim string) int64 {
	t.Helper()
	for _, doc := range skeletonDocuments(t, name) {
		if doc.Kind == claimKind && doc.Metadata.Name == claim {
			return quantityBytes(t, doc.Spec.Resources.Requests.Storage)
		}
	}
	t.Fatalf("no %s %q in %s", claimKind, claim, name)
	return 0
}

func (p *manifestPod) requiredEnv(t *testing.T, name string) string {
	t.Helper()
	value, set := p.env()[name]
	if !set {
		t.Fatalf("%s: %s", name, manifestEnvMissing)
	}
	return value
}

func (p *manifestPod) env() map[string]string {
	values := map[string]string{}
	for _, variable := range p.Containers[0].Env {
		values[variable.Name] = variable.Value
	}
	return values
}

func (p *manifestPod) volumeLimit(t *testing.T, name string) int64 {
	t.Helper()
	for _, volume := range p.Volumes {
		if volume.Name == name && volume.EmptyDir != nil {
			return quantityBytes(t, volume.EmptyDir.SizeLimit)
		}
	}
	t.Fatalf("no emptyDir volume %q", name)
	return 0
}

func (p *manifestPod) claimOf(t *testing.T, name string) string {
	t.Helper()
	for _, volume := range p.Volumes {
		if volume.Name == name && volume.PersistentVolumeClaim != nil {
			return volume.PersistentVolumeClaim.ClaimName
		}
	}
	t.Fatalf("volume %q is not a %s: a new NATS pod would start on an empty store", name, claimKind)
	return ""
}

func (p *manifestPod) mountedAt(t *testing.T, path string) string {
	t.Helper()
	for _, mount := range p.Containers[0].VolumeMounts {
		if mount.MountPath == path {
			return mount.Name
		}
	}
	t.Fatalf("nothing is mounted at %q", path)
	return ""
}

func quantityBytes(t *testing.T, quantity string) int64 {
	t.Helper()
	units := map[string]int64{
		quantityGi: gib,
		quantityMi: mib,
	}
	for suffix, unit := range units {
		number, found := strings.CutSuffix(quantity, suffix)
		if !found {
			continue
		}
		value, err := strconv.ParseInt(number, 10, 64)
		if err != nil {
			t.Fatalf("quantity %q: %v", quantity, err)
		}
		return value * unit
	}
	t.Fatalf("quantity %q has no Gi or Mi suffix", quantity)
	return 0
}

func preStopBudget(handler *manifestHandler) (time.Duration, error) {
	if handler == nil {
		return 0, nil
	}
	if handler.Sleep != nil {
		return time.Duration(handler.Sleep.Seconds) * time.Second, nil
	}
	if handler.Exec == nil {
		return 0, errUnpricedPreStop
	}
	return execSleepBudget(handler.Exec.Command)
}

func execSleepBudget(command []string) (time.Duration, error) {
	index := len(command) - 1
	if index < 1 || command[index-1] != sleepCommand {
		return 0, errUnpricedPreStop
	}
	seconds, err := strconv.Atoi(command[index])
	if err != nil {
		return 0, errors.Join(errUnpricedPreStop, err)
	}
	return time.Duration(seconds) * time.Second, nil
}

func natsSetting[T any](t *testing.T, settings map[string]any, path ...string) T {
	t.Helper()
	current := any(settings)
	for _, key := range path {
		section, ok := current.(map[string]any)
		if !ok {
			t.Fatalf("%s: %v is not a section", strings.Join(path, "."), current)
		}
		current = section[key]
	}
	value, ok := current.(T)
	if !ok {
		t.Fatalf("%s = %v, want a %T", strings.Join(path, "."), current, value)
	}
	return value
}

func manifestAll(t *testing.T, pod *manifestPod, overrides map[string]string) allSetup {
	t.Helper()
	values := pod.env()
	for name, value := range overrides {
		values[name] = value
	}
	setup, err := loadAll(processOf(values))
	if err != nil {
		t.Fatalf("loadAll on the manifest environment: %v", err)
	}
	return setup
}

func stackStopBudget(role *stack) time.Duration {
	var total time.Duration
	for _, step := range role.layers {
		total += step.stopBudget + exitGrace
	}
	return total
}

func TestStopBudgetOfEveryRoleFitsItsGracePeriod(t *testing.T) {
	tests := []struct {
		name     string
		manifest string
		build    func(t *testing.T, pod *manifestPod) *stack
	}{
		{
			name:     commandAll,
			manifest: roleManifest,
			build: func(t *testing.T, pod *manifestPod) *stack {
				t.Helper()
				setup := manifestAll(t, pod, map[string]string{
					envCheckpointDir:  t.TempDir(),
					envPublishTimeout: maxPublishTimeout.String(),
				})
				return allStack(&setup, newLifecycle(testLogger(&logBuffer{})), testLogger(&logBuffer{}))
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pod := skeletonPod(t, tt.manifest)
			preStop, err := preStopBudget(pod.Containers[0].Lifecycle.PreStop)
			if err != nil {
				t.Fatalf("%s: %v", tt.manifest, err)
			}
			stop := stackStopBudget(tt.build(t, &pod))
			grace := time.Duration(pod.TerminationGracePeriodSeconds) * time.Second
			if need := stop + killSlack + preStop; need > grace {
				t.Errorf("%s: stop %v + slack %v + preStop %v = %v > terminationGracePeriodSeconds %v",
					tt.name, stop, killSlack, preStop, need, grace)
			}
		})
	}
}

func TestTheSkeletonNATSHoldsWhatTheAllRoleWrites(t *testing.T) {
	pod := skeletonPod(t, roleManifest)
	nats := skeletonPod(t, natsManifest)
	pod.requiredEnv(t, envNATSURL)
	spoolDir := pod.requiredEnv(t, envCheckpointDir)
	spoolLimit := pod.volumeLimit(t, pod.mountedAt(t, spoolDir))
	setup := manifestAll(t, &pod, map[string]string{
		envCheckpointDir: t.TempDir(),
	})
	settings, err := conf.Parse(string(skeletonFile(t, natsConfigFile)))
	if err != nil {
		t.Fatalf("parse %s: %v", natsConfigFile, err)
	}
	fileStore := natsSetting[int64](t, settings, natsJetStreamKey, natsFileStoreKey)
	storeDir := natsSetting[string](t, settings, natsJetStreamKey, natsStoreDirKey)
	streams := setup.Partition.Streams
	worst := setup.Partition.Limits.WorstCheckpointBytes()
	tests := []struct {
		name string
		need int64
		have int64
	}{
		{
			name: "partition streams, checkpoint buckets and the dead letter stream fit max_file_store",
			need: int64(streams.Partitions)*(streams.MaxBytesPerPartition+bucketBytes(worst)) + natsjs.DeadLetterMaxBytes,
			have: fileStore,
		},
		{
			name: "max_file_store fits the claim mounted at store_dir",
			need: fileStore,
			have: skeletonClaimBytes(t, natsManifest, nats.claimOf(t, nats.mountedAt(t, storeDir))),
		},
		{
			name: "max_payload carries the largest record with its headers",
			need: natsjs.PayloadNeed(),
			have: natsSetting[int64](t, settings, natsMaxPayloadKey),
		},
		{
			name: "the spool volume holds two worst images and slack",
			need: spoolImages*worst + spoolSlack,
			have: spoolLimit,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.need > tt.have {
				t.Errorf("need %d B (%.1f MiB) > have %d B (%.1f MiB)",
					tt.need, float64(tt.need)/mib, tt.have, float64(tt.have)/mib)
			}
		})
	}
}

func TestTheSkeletonNATSGoHeapLimitStaysBelowItsContainerLimit(t *testing.T) {
	nats := skeletonPod(t, natsManifest)
	raw, set := nats.env()[envMemoryLimit]
	if !set {
		t.Fatalf("%s: %s", envMemoryLimit, manifestEnvMissing)
	}
	heap, err := parseMemoryLimit(raw)
	if err != nil {
		t.Fatalf("%s=%q: %v", envMemoryLimit, raw, err)
	}
	container := quantityBytes(t, nats.Containers[0].Resources.Limits.Memory)
	if heap >= container {
		t.Errorf("%s %s >= limits.memory %s: the nats-server heap leaves no room for its stacks and buffers",
			envMemoryLimit, raw, nats.Containers[0].Resources.Limits.Memory)
	}
}
