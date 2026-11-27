package demo_test

import (
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"

	"github.com/DmytroLysenko1/faultline/internal/interfaces/otlp/otlptest"
)

const (
	bashBinary        = "bash"
	demoTenant        = "default"
	otherTenantGrant  = "idother team-b"
	demoNamespacePin  = "OTEL_DEMO_NAMESPACE="
	tokenOfPrefix     = "token-of-"
	tokenIDAnnotation = "ingest-token-id="
	stubFileMode      = 0o755
	clientSwitchDelay = 30 * time.Second
	helmUpgradeEvent  = "helm upgrade"
	sleepEventPrefix  = "sleep "
	stateFileMode     = 0o600
)

var rotationStubs = []string{
	"kubectl",
	"helm",
	"sleep",
}

type demoShell struct {
	root  string
	bin   string
	state string
}

type storedSecret struct {
	Namespace string
	Name      string
	Key       string
	TokenOf   string
	TokenID   string
}

type rotationOutcome struct {
	Exits        []int
	Live         []string
	Secret       storedSecret
	RevokedInUse []string
}

type initOutcome struct {
	Exit       int
	Attempts   string
	ShowsCause bool
}

func newDemoShell(t *testing.T) demoShell {
	t.Helper()
	shell := demoShell{
		root:  otlptest.ModuleRoot(t),
		bin:   t.TempDir(),
		state: t.TempDir(),
	}
	for _, stub := range rotationStubs {
		raw, err := os.ReadFile(filepath.Join("testdata", "rotation", stub))
		if err != nil {
			t.Fatalf("read the %s stub: %v", stub, err)
		}
		if err := os.WriteFile(filepath.Join(shell.bin, stub), raw, stubFileMode); err != nil {
			t.Fatalf("install the %s stub: %v", stub, err)
		}
	}
	shell.writeState(t, "grants", otherTenantGrant+"\n")
	return shell
}

func (s demoShell) writeState(t *testing.T, name, content string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(s.state, name), []byte(content), stateFileMode); err != nil {
		t.Fatalf("write state %s: %v", name, err)
	}
}

func (s demoShell) readState(t *testing.T, name string) string {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(s.state, name))
	if errors.Is(err, os.ErrNotExist) {
		return ""
	}
	if err != nil {
		t.Fatalf("read state %s: %v", name, err)
	}
	return string(raw)
}

func (s demoShell) run(t *testing.T, function, failAt string) (int, string) {
	t.Helper()
	script := `source hack/demo-1.sh && work="$ROTATION_STATE/work" && mkdir -p "$work" && ` + function
	command := exec.CommandContext(t.Context(), bashBinary, "-c", script)
	command.Dir = s.root
	command.Env = append(os.Environ(),
		"PATH="+s.bin+string(os.PathListSeparator)+os.Getenv("PATH"),
		"ROTATION_STATE="+s.state,
		"ROTATION_FAIL_AT="+failAt,
	)
	output, err := command.CombinedOutput()
	if err == nil {
		return 0, string(output)
	}
	exit, ok := errors.AsType[*exec.ExitError](err)
	if !ok {
		t.Fatalf("run %s: %v", function, err)
	}
	return exit.ExitCode(), string(output)
}

func (s demoShell) secret(t *testing.T) storedSecret {
	t.Helper()
	fields := strings.Fields(s.readState(t, "secret"))
	if len(fields) < 5 {
		return storedSecret{}
	}
	stored := storedSecret{
		Namespace: fields[1],
		Name:      fields[2],
		Key:       fields[3],
		TokenOf:   strings.TrimPrefix(fields[4], tokenOfPrefix),
	}
	for _, annotation := range fields[5:] {
		if id, found := strings.CutPrefix(annotation, tokenIDAnnotation); found {
			stored.TokenID = id
		}
	}
	return stored
}

func (s demoShell) rotate(t *testing.T, runs []string) (rotationOutcome, string) {
	t.Helper()
	got := rotationOutcome{}
	var logs strings.Builder
	for _, failAt := range runs {
		exit, output := s.run(t, "rotate_token", failAt)
		got.Exits = append(got.Exits, exit)
		logs.WriteString(output)
	}
	got.Live = lines(s.readState(t, "grants"))
	got.Secret = s.secret(t)
	got.RevokedInUse = lines(s.readState(t, "revoked-in-use"))
	return got, logs.String()
}

func lines(text string) []string {
	return slices.Sorted(slices.Values(strings.FieldsFunc(text, func(r rune) bool {
		return r == '\n'
	})))
}

func lastIssued(issued string) string {
	all := strings.Fields(issued)
	if len(all) == 0 {
		return ""
	}
	return all[len(all)-1]
}

func wantRotation(t *testing.T, shell demoShell, exits []int) rotationOutcome {
	t.Helper()
	values := readCollectorValues(t)
	read := values.secretOf(referencedVariable(values.Collector.Config.Exporters[faultlineExporter].Headers[authorizationKey]))
	if read == nil {
		t.Fatalf("the %s exporter reads no Secret", faultlineExporter)
	}
	current := lastIssued(shell.readState(t, "issued"))
	return rotationOutcome{
		Exits: exits,
		Live:  lines(current + " " + demoTenant + "\n" + otherTenantGrant + "\n"),
		Secret: storedSecret{
			Namespace: pinnedVersion(t, demoNamespacePin),
			Name:      read.Name,
			Key:       read.Key,
			TokenOf:   current,
			TokenID:   current,
		},
	}
}

func TestEveryRotationLeavesOnlyTheTokenTheCollectorReads(t *testing.T) {
	tests := []struct {
		name  string
		runs  []string
		exits []int
	}{
		{
			name:  "the first run on an empty bucket stores its token",
			runs:  []string{""},
			exits: []int{0},
		},
		{
			name:  "a repeated run revokes the token of the run before",
			runs:  []string{"", "", ""},
			exits: []int{0, 0, 0},
		},
		{
			name:  "a helm failure after the Secret is written leaves no grant behind the next run",
			runs:  []string{"", "helm", ""},
			exits: []int{0, 1, 0},
		},
		{
			name:  "a collector rollout that does not finish leaves no grant behind the next run",
			runs:  []string{"", "collector", ""},
			exits: []int{0, 1, 0},
		},
		{
			name:  "a token issued but never stored is revoked by the next run",
			runs:  []string{"", "store", ""},
			exits: []int{0, 1, 0},
		},
		{
			name:  "a revoke that fails is finished by the next run",
			runs:  []string{"", "revoke", ""},
			exits: []int{0, 0, 0},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			shell := newDemoShell(t)
			got, logs := shell.rotate(t, tt.runs)
			if diff := cmp.Diff(wantRotation(t, shell, tt.exits), got); diff != "" {
				t.Errorf("grants and Secret after the runs (-want +got):\n%s\nlog:\n%s", diff, logs)
			}
		})
	}
}

func TestInitStopsAtAnErrorOfTheCommandAndRetriesAPodNotReached(t *testing.T) {
	tests := []struct {
		name   string
		failAt string
		want   initOutcome
	}{
		{
			name:   "an error of faultline tokens fails at once with its message",
			failAt: "init_fatal",
			want: initOutcome{
				Exit:       1,
				Attempts:   "1",
				ShowsCause: true,
			},
		},
		{
			name:   "a pod not reached yet is retried until init succeeds",
			failAt: "init_unreachable",
			want: initOutcome{
				Exit:     0,
				Attempts: "3",
			},
		},
		{
			name:   "a NATS not reachable yet is retried until init succeeds",
			failAt: "init_nats_unreachable",
			want: initOutcome{
				Exit:     0,
				Attempts: "3",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			shell := newDemoShell(t)
			exit, output := shell.run(t, "init_tokens", tt.failAt)
			got := initOutcome{
				Exit:       exit,
				Attempts:   strings.TrimSpace(shell.readState(t, "init-attempts")),
				ShowsCause: strings.Contains(output, "faultline tokens: reconcile the token bucket: stub refuses"),
			}
			if diff := cmp.Diff(tt.want, got); diff != "" {
				t.Errorf("init_tokens (-want +got):\n%s\nlog:\n%s", diff, output)
			}
		})
	}
}

func sleptBeforeInstall(t *testing.T, events string) time.Duration {
	t.Helper()
	slept := time.Duration(0)
	for _, event := range strings.Split(strings.TrimSpace(events), "\n") {
		if event == helmUpgradeEvent {
			return slept
		}
		seconds, found := strings.CutPrefix(event, sleepEventPrefix)
		if !found {
			continue
		}
		value, err := strconv.Atoi(seconds)
		if err != nil {
			t.Fatalf("sleep %q: %v", seconds, err)
		}
		slept += time.Duration(value) * time.Second
	}
	t.Fatalf("no helm upgrade in the events %q", events)
	return 0
}

func TestTheCollectorGetsTheNewTokenOnlyAfterEveryIngesterCanAcceptIt(t *testing.T) {
	shell := newDemoShell(t)
	started := time.Now()

	exit, output := shell.run(t, "rotate_token", "")

	elapsed := time.Since(started)
	if exit != 0 {
		t.Fatalf("rotate_token exited %d:\n%s", exit, output)
	}
	switched := sleptBeforeInstall(t, shell.readState(t, "events")) + elapsed
	if switched < clientSwitchDelay {
		t.Errorf("the collector got the token %s after the issue, want at least %s:\n%s", switched, clientSwitchDelay, output)
	}
}
