package main

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"

	"github.com/DmytroLysenko1/faultline/internal/infrastructure/natsjs"
	"github.com/DmytroLysenko1/faultline/internal/infrastructure/natsjs/record"
)

const (
	testNATSURL   = "nats://127.0.0.1:4222"
	testHostname  = "ingester-7f9c.faultline.svc"
	secretMarker  = "hunter2"
	userinfoURL   = "nats://faultline:" + secretMarker + "@127.0.0.1:4222"
	credsFileName = "faultline.creds"
	credsFileMode = 0o600
)

var errTestNoHostname = errors.New("no hostname")

func podHostname() (string, error) {
	return testHostname, nil
}

func missingHostname() (string, error) {
	return "", errTestNoHostname
}

func TestLoadNATSDefaults(t *testing.T) {
	got, err := loadNATS(environment(map[string]string{envNATSURL: testNATSURL}), podHostname)
	if err != nil {
		t.Fatalf("loadNATS: %v", err)
	}

	want := natsConfig{
		URLs:           []string{testNATSURL},
		ConnectTimeout: 10 * time.Second,
		Instance:       "ingester-7f9c",
	}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("loadNATS (-want +got):\n%s", diff)
	}
}

func TestLoadNATSTakesEveryURLAndFile(t *testing.T) {
	creds := filepath.Join(t.TempDir(), credsFileName)
	if err := os.WriteFile(creds, []byte("creds"), credsFileMode); err != nil {
		t.Fatalf("write creds: %v", err)
	}
	lookup := environment(map[string]string{
		envNATSURL:            "nats://nats-0:4222, tls://nats-1.nats:4222",
		envNATSCredsFile:      creds,
		envNATSTLSCAFile:      creds,
		envNATSConnectTimeout: "3s",
		envInstance:           "ingester-a",
	})

	got, err := loadNATS(lookup, missingHostname)
	if err != nil {
		t.Fatalf("loadNATS: %v", err)
	}

	want := natsConfig{
		URLs:           []string{"nats://nats-0:4222", "tls://nats-1.nats:4222"},
		CredsFile:      creds,
		TLSCAFile:      creds,
		ConnectTimeout: 3 * time.Second,
		Instance:       "ingester-a",
	}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("loadNATS (-want +got):\n%s", diff)
	}
}

func TestLoadNATSRefusesWhatItCannotDial(t *testing.T) {
	tests := []struct {
		name    string
		args    map[string]string
		want    string
		wantErr error
	}{
		{
			name:    "no url",
			args:    map[string]string{},
			want:    "NATS_URL: required",
			wantErr: errRequired,
		},
		{
			name: "an empty url",
			args: map[string]string{
				envNATSURL: "",
			},
			want:    "NATS_URL: empty value",
			wantErr: errEmpty,
		},
		{
			name: "credentials in the url",
			args: map[string]string{
				envNATSURL: testNATSURL + "," + userinfoURL,
			},
			want:    "NATS_URL: url 2 carries credentials, put them in NATS_CREDS_FILE",
			wantErr: errURLUserinfo,
		},
		{
			name: "the same url twice",
			args: map[string]string{
				envNATSURL: testNATSURL + "," + testNATSURL,
			},
			want:    "NATS_URL: url 2 repeats an earlier url",
			wantErr: errDuplicateURL,
		},
		{
			name: "another scheme",
			args: map[string]string{
				envNATSURL: "http://127.0.0.1:4222",
			},
			want:    "NATS_URL: url 1 must be nats://host:port or tls://host:port",
			wantErr: errURLShape,
		},
		{
			name: "no port",
			args: map[string]string{
				envNATSURL: "nats://127.0.0.1",
			},
			want:    "url 1 must be nats://host:port or tls://host:port (address 127.0.0.1: missing port in address)",
			wantErr: errURLShape,
		},
		{
			name: "a path after the port",
			args: map[string]string{
				envNATSURL: "nats://127.0.0.1:4222/x",
			},
			wantErr: errURLShape,
		},
		{
			name: "a url that does not parse never echoes its text",
			args: map[string]string{
				envNATSURL: "nats://faultline:" + secretMarker + "@[::1",
			},
			want:    "NATS_URL: url 1 must be nats://host:port or tls://host:port",
			wantErr: errURLShape,
		},
		{
			name: "a missing creds file",
			args: map[string]string{
				envNATSURL:       testNATSURL,
				envNATSCredsFile: "/nonexistent/faultline.creds",
			},
			wantErr: os.ErrNotExist,
		},
		{
			name: "a connect timeout below a second",
			args: map[string]string{
				envNATSURL:            testNATSURL,
				envNATSConnectTimeout: "500ms",
			},
			wantErr: errOutOfRange,
		},
		{
			name: "an instance that is not a DNS label",
			args: map[string]string{
				envNATSURL:  testNATSURL,
				envInstance: "Ingester_A",
			},
			want:    `INSTANCE: "Ingester_A" is not a DNS label of at most 63 bytes`,
			wantErr: errNotDNSLabel,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := loadNATS(environment(tt.args), podHostname)

			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("loadNATS = %v, want %v", err, tt.wantErr)
			}
			if !strings.Contains(err.Error(), tt.want) {
				t.Errorf("loadNATS = %q, want it to contain %q", err, tt.want)
			}
			if strings.Contains(err.Error(), secretMarker) {
				t.Errorf("loadNATS = %q echoes the password", err)
			}
		})
	}
}

func TestTheInstanceComesFromTheHostname(t *testing.T) {
	tests := []struct {
		name    string
		args    func() (string, error)
		want    string
		wantErr error
	}{
		{
			name: "the first label of a pod hostname",
			args: podHostname,
			want: "ingester-7f9c",
		},
		{
			name: "a developer hostname, lowered",
			args: func() (string, error) { return "Dev-MacBook.local", nil },
			want: "dev-macbook",
		},
		{
			name:    "no hostname",
			args:    missingHostname,
			wantErr: errNoHostname,
		},
		{
			name:    "no hostname function",
			wantErr: errNoHostname,
		},
		{
			name:    "a hostname that is no label",
			args:    func() (string, error) { return "under_score", nil },
			wantErr: errNotDNSLabel,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := loadNATS(environment(map[string]string{envNATSURL: testNATSURL}), tt.args)

			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("loadNATS = %v, want %v", err, tt.wantErr)
			}
			if diff := cmp.Diff(tt.want, got.Instance); diff != "" {
				t.Errorf("instance (-want +got):\n%s", diff)
			}
		})
	}
}

func TestLoadStreamsDefaults(t *testing.T) {
	got, err := loadStreams(environment(map[string]string{}))
	if err != nil {
		t.Fatalf("loadStreams: %v", err)
	}

	want := natsjs.Topology{
		Partitions:           1,
		Replicas:             1,
		MaxBytesPerPartition: 2 << 30,
		DuplicateWindow:      5 * time.Minute,
		Compression:          natsjs.CompressionNone,
		ConfigVersion:        natsjs.StreamConfigVersion,
	}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("loadStreams (-want +got):\n%s", diff)
	}
	if err := got.Validate(); err != nil {
		t.Errorf("the default topology is invalid: %v", err)
	}
}

func TestLoadStreamsRefusesWhatTheStreamsCannotKeep(t *testing.T) {
	tests := []struct {
		name    string
		args    map[string]string
		wantErr error
	}{
		{
			name: "two partitions before step 11",
			args: map[string]string{
				envPartitions: "2",
			},
			wantErr: errOutOfRange,
		},
		{
			name: "an even replica count",
			args: map[string]string{
				envStreamReplicas: "2",
			},
			wantErr: natsjs.ErrInvalidTopology,
		},
		{
			name: "seven replicas",
			args: map[string]string{
				envStreamReplicas: "7",
			},
			wantErr: errOutOfRange,
		},
		{
			name: "a stream below one GiB",
			args: map[string]string{
				envStreamMaxBytes: "512MiB",
			},
			wantErr: errOutOfRange,
		},
		{
			name: "a dedup window shorter than the client retries",
			args: map[string]string{
				envDuplicateWindow: "1m",
			},
			wantErr: errOutOfRange,
		},
		{
			name: "an unknown compression",
			args: map[string]string{
				envStreamCompression: "zstd",
			},
			wantErr: errUnknownChoice,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := loadStreams(environment(tt.args))

			if !errors.Is(err, tt.wantErr) {
				t.Errorf("loadStreams = %v, want %v", err, tt.wantErr)
			}
		})
	}
}

func TestLoadStreamsTakesEveryValue(t *testing.T) {
	lookup := environment(map[string]string{
		envStreamReplicas:    "3",
		envStreamMaxBytes:    "8GiB",
		envDuplicateWindow:   "15m",
		envStreamCompression: "s2",
	})

	got, err := loadStreams(lookup)
	if err != nil {
		t.Fatalf("loadStreams: %v", err)
	}

	want := natsjs.Topology{
		Partitions:           1,
		Replicas:             3,
		MaxBytesPerPartition: 8 << 30,
		DuplicateWindow:      15 * time.Minute,
		Compression:          natsjs.CompressionS2,
		ConfigVersion:        natsjs.StreamConfigVersion,
	}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("loadStreams (-want +got):\n%s", diff)
	}
}

func TestLoadPublish(t *testing.T) {
	tests := []struct {
		name    string
		args    map[string]string
		want    publishConfig
		wantErr error
	}{
		{
			name: "defaults",
			args: map[string]string{},
			want: publishConfig{
				Tenant:         "default",
				PublishTimeout: 2 * time.Second,
			},
		},
		{
			name: "a tenant and a timeout",
			args: map[string]string{
				envTenant:         "team-a",
				envPublishTimeout: "3s",
			},
			want: publishConfig{
				Tenant:         "team-a",
				PublishTimeout: 3 * time.Second,
			},
		},
		{
			name: "a tenant that is not a label",
			args: map[string]string{
				envTenant: "Team A",
			},
			want: publishConfig{
				Tenant:         "default",
				PublishTimeout: 2 * time.Second,
			},
			wantErr: record.ErrInvalidTenant,
		},
		{
			name: "a timeout above the client deadline",
			args: map[string]string{
				envPublishTimeout: "5s",
			},
			want: publishConfig{
				Tenant:         "default",
				PublishTimeout: 2 * time.Second,
			},
			wantErr: errOutOfRange,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := loadPublish(environment(tt.args))

			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("loadPublish = %v, want %v", err, tt.wantErr)
			}
			if diff := cmp.Diff(tt.want, got); diff != "" {
				t.Errorf("loadPublish (-want +got):\n%s", diff)
			}
		})
	}
}
