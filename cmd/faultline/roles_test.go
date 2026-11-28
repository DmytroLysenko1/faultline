package main

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"hash/crc32"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/DmytroLysenko1/faultline/internal/infrastructure/natsjs/natsjstest"
	"github.com/DmytroLysenko1/faultline/internal/interfaces/otlp"
)

const (
	refusedWithin     = time.Second
	ingesterBudgetMiB = 655.28125
	allWorstBytes     = 1_405_224_329
	budgetMiB         = float64(mib)
)

func ingesterOn(t *testing.T, urls []string) (*ingesterSetup, string, string) {
	t.Helper()
	rules, err := loadRules("")
	if err != nil {
		t.Fatalf("rules: %v", err)
	}
	api, otlpAddress := localAddress(t), localAddress(t)
	setup := &ingesterSetup{
		Config: Config{
			APIAddr:    api,
			OTLPAddr:   otlpAddress,
			RouteRules: rules.Rules,
		},
		Ingest:  testIngest(),
		NATS:    *natsSetup(urls...),
		Journal: *testJournalOptions(),
	}
	return setup, api, otlpAddress
}

func serveIngester(t *testing.T, setup *ingesterSetup, logs *logBuffer) (*fakeNotify, <-chan int, *lifecycle) {
	t.Helper()
	grantTestToken(t, setup.Journal.Tenant, setup.NATS.URLs...)
	return launchIngester(t, setup, logs)
}

func launchIngester(t *testing.T, setup *ingesterSetup, logs *logBuffer) (*fakeNotify, <-chan int, *lifecycle) {
	t.Helper()
	notify := newFakeNotify()
	life := newLifecycle(testLogger(logs))
	done := launchRoleWith(t, ingesterStack(setup, life, testLogger(logs)), notify.notify, life)
	return notify, done, life
}

func TestIngesterStopOrder(t *testing.T) {
	cluster := natsjstest.Start(t, singleNATSNode)
	ensureStreams(t, cluster.URLs()...)
	logs := &logBuffer{}
	setup, _, _ := ingesterOn(t, cluster.URLs())
	notify, done, life := serveIngester(t, setup, logs)
	eventually(t, "serving", life.Served)

	notify.send()

	assertCode(t, <-done, exitOK, logs)
	stopped := valuesOf(logs.all(t, eventLayerStopped), "layer")
	assertDiff(t, "stop order", []any{"otlp", "journal", "directory", "stats", "conn", "http", "tracing"}, stopped)
	want := []string{"starting", "connecting", "serving", "draining", timelineFinalStats, "stopped"}
	assertDiff(t, "timeline", want, logs.timeline(t))
	final := logs.all(t, eventPipelineStats)
	if _, found := final[len(final)-1]["ingest"]; !found {
		t.Errorf("the final pipeline.stats has no ingest group: %v", final[len(final)-1])
	}
}

func TestOTLPBeforeServingIsRefusedNotHung(t *testing.T) {
	cluster := natsjstest.Start(t, singleNATSNode)
	logs := &logBuffer{}
	setup, api, otlpAddress := ingesterOn(t, cluster.URLs())
	notify, done, _ := serveIngester(t, setup, logs)
	eventually(t, "waiting for the streams", lineWith(t, logs, eventState, "state", string(stateWaitingStreams)))

	began := time.Now()
	var dialer net.Dialer
	conn, err := dialer.DialContext(t.Context(), network, otlpAddress)
	took := time.Since(began)

	if err == nil {
		closeQuietly(t, conn)
		t.Fatal("the OTLP port accepted a connection before the ingester serves")
	}
	if !errors.Is(err, syscall.ECONNREFUSED) || took > refusedWithin {
		t.Errorf("dial OTLP before serving = %v after %s, want refused at once", err, took)
	}
	assertDiff(t, "health and readiness while waiting", []int{http.StatusOK, http.StatusServiceUnavailable}, []int{
		statusOf(t, api, healthPath),
		statusOf(t, api, readyPath),
	})
	notify.send()
	assertCode(t, <-done, exitOK, logs)
	assertDiff(t, "canceled stage", stageAwaitStreams, logs.event(t, eventStartCanceled)["stage"])
}

func closeQuietly(t *testing.T, conn net.Conn) {
	t.Helper()
	if err := conn.Close(); err != nil {
		t.Errorf("close: %v", err)
	}
}

func TestEveryRoleFitsItsMemoryBudgetAtDefaults(t *testing.T) {
	current := &process{
		Lookup: environment(map[string]string{
			envNATSURL: testNATSURL,
		}),
		Hostname: podHostname,
	}
	setup, err := loadIngester(current)
	if err != nil {
		t.Fatalf("loadIngester: %v", err)
	}
	logs := &logBuffer{}
	life := newLifecycle(testLogger(logs))

	parts := ingesterStack(&setup, life, testLogger(logs)).contributions()

	assertDiff(t, "ingester MiB", ingesterBudgetMiB, float64(total(parts))/budgetMiB)
	if err := checkParts(parts, defaultMemoryLimit); err != nil {
		t.Errorf("the ingester does not fit the default GOMEMLIMIT: %v", err)
	}
	want := "otlp requests 440.0 MiB, otlp connections 145.0 MiB, records before pubAck 60.0 MiB, publish buffer 8.0 MiB, " +
		"nats connections 1.0 MiB, token directory 0.8 MiB, admission tokens 0.4 MiB"
	assertDiff(t, "breakdown", want, breakdown(parts))

	everything := allContributions(t, map[string]string{})
	assertDiff(t, "all bytes", int64(allWorstBytes), total(everything))
	if err := checkParts(everything, defaultMemoryLimit); err != nil {
		t.Errorf("all does not fit the default GOMEMLIMIT: %v", err)
	}
}

func TestTheIngesterCommandRefusesItsConfigWithCodeTwo(t *testing.T) {
	tests := []struct {
		name string
		args map[string]string
		want string
	}{
		{
			name: "no NATS url",
			args: map[string]string{},
			want: "NATS_URL: required",
		},
		{
			name: "credentials in the url never reach the log",
			args: map[string]string{
				envNATSURL: userinfoURL,
			},
			want: "NATS_URL: url 1 carries credentials, put them in NATS_CREDS_FILE",
		},
		{
			name: "a url that does not parse never reaches the log",
			args: map[string]string{
				envNATSURL: "nats://faultline:" + secretMarker + "@[::1",
			},
			want: "NATS_URL: url 1 must be nats://host:port or tls://host:port",
		},
		{
			name: "a memory limit below the ingester's worst case",
			args: map[string]string{
				envNATSURL:     testNATSURL,
				envMemoryLimit: "512MiB",
			},
			want: "worst-case memory exceeds the budget: 655.3 MiB > 85% of GOMEMLIMIT 512.0 MiB",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var stderr bytes.Buffer
			limits := &recordedLimit{}

			code := realMain(&process{
				Args:           []string{commandIngester},
				Lookup:         environment(tt.args),
				ReadCgroup:     func() ([]byte, error) { return nil, os.ErrNotExist },
				SetMemoryLimit: limits.setLimit,
				Stderr:         &stderr,
				Hostname:       podHostname,
			})

			assertDiff(t, "code", exitConfig, code)
			if !strings.Contains(stderr.String(), tt.want) {
				t.Errorf("stderr = %q, want it to contain %q", stderr.String(), tt.want)
			}
			if strings.Contains(stderr.String(), secretMarker) {
				t.Errorf("stderr = %q carries the password", stderr.String())
			}
		})
	}
}

func TestASecondSignalReachesTheSystemThroughTheIngesterWiring(t *testing.T) {
	cluster := natsjstest.Start(t, singleNATSNode)
	ensureStreams(t, cluster.URLs()...)
	grantTestToken(t, defaultTenant, cluster.URLs()...)
	otlpAddress := localAddress(t)
	ctx, cancel := context.WithTimeout(t.Context(), secondSignalCap)
	defer cancel()
	command := exec.CommandContext(ctx, os.Args[0], commandIngester)
	command.Env = append(os.Environ(),
		reexecFlag+"=1",
		envNATSURL+"="+cluster.URLs()[0],
		envInstance+"="+testInstance,
		envAPIAddr+"="+localAddress(t),
		envOTLPAddr+"="+otlpAddress,
	)
	stderr, err := command.StderrPipe()
	if err != nil {
		t.Fatalf("stderr pipe: %v", err)
	}
	if err := command.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	lines := bufio.NewScanner(stderr)
	awaitState(t, lines, stateServing)
	silent := dialRawOTLP(t, otlpAddress)
	if !answers(t, silent) {
		t.Fatal("the silent OTLP connection was not accepted")
	}
	accepted := time.Now()

	signalChild(t, command)
	awaitState(t, lines, stateDraining)
	signalChild(t, command)
	if window := time.Since(accepted); window >= otlp.ConnectionTimeout {
		t.Fatalf("the second SIGTERM came %s after the silent connection was accepted; GracefulStop holds draining "+
			"only for otlp.ConnectionTimeout %s, so the test can no longer prove the second signal", window,
			otlp.ConnectionTimeout)
	}

	if _, err := io.Copy(io.Discard, stderr); err != nil {
		t.Errorf("drain stderr: %v", err)
	}
	waitErr := command.Wait()
	status, _ := command.ProcessState.Sys().(syscall.WaitStatus)
	if !status.Signaled() || status.Signal() != syscall.SIGTERM {
		t.Fatalf("child ended with %v (%v), want killed by the second SIGTERM", command.ProcessState, waitErr)
	}
}

func TestTheRulesHashIsTheCRC32COfTheFileBytes(t *testing.T) {
	path := writeRules(t, catalogRules)
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read rules: %v", err)
	}
	setup, err := loadIngester(&process{
		Lookup: environment(map[string]string{
			envNATSURL:        testNATSURL,
			envRouteRulesFile: path,
		}),
		Hostname: podHostname,
	})
	if err != nil {
		t.Fatalf("loadIngester: %v", err)
	}
	without, err := loadRules("")
	if err != nil {
		t.Fatalf("loadRules: %v", err)
	}

	got := []uint32{setup.Journal.RulesVersion, without.Hash}

	assertDiff(t, "rules versions", []uint32{crc32.Checksum(data, crc32.MakeTable(crc32.Castagnoli)), 0}, got)
	if setup.Config.RouteRules == nil {
		t.Error("the ingester lost the parsed rules")
	}
}

func TestTheIngesterSetsItsMemoryLimitBeforeItsBudget(t *testing.T) {
	logs := &logBuffer{}
	limits := &recordedLimit{}

	_, _, err := prepareRole(&process{
		Lookup: environment(map[string]string{
			envNATSURL: testNATSURL,
		}),
		ReadCgroup:     func() ([]byte, error) { return nil, os.ErrNotExist },
		SetMemoryLimit: limits.setLimit,
		Stderr:         logs,
		Hostname:       podHostname,
	}, ingesterRole())
	if err != nil {
		t.Fatalf("prepareRole: %v", err)
	}

	assertDiff(t, "GOMEMLIMIT set", []int64{defaultMemoryLimit}, limits.set)
	logged := logs.event(t, "config.memory_limit")
	want := []any{float64(defaultMemoryLimit), string(sourceDefault)}
	assertDiff(t, "logged limit", want, []any{logged["bytes"], logged["source"]})
}

func TestTheIngesterRefusesRouteRulesItCannotRead(t *testing.T) {
	tests := []struct {
		name  string
		rules string
	}{
		{
			name:  "a rules file that does not parse",
			rules: "this is not a rules file",
		},
		{
			name:  "a rules file that is missing",
			rules: "",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "rules.yaml")
			if tt.rules != "" {
				if err := os.WriteFile(path, []byte(tt.rules), 0o600); err != nil {
					t.Fatalf("write rules: %v", err)
				}
			}

			_, err := loadIngester(&process{
				Lookup: environment(map[string]string{
					envNATSURL:        testNATSURL,
					envRouteRulesFile: path,
				}),
				Hostname: podHostname,
			})

			if err == nil || !strings.Contains(err.Error(), envRouteRulesFile) {
				t.Errorf("loadIngester error = %v, want one naming %s", err, envRouteRulesFile)
			}
		})
	}
}

func preparedRole[S any](recipe roleRecipe[S]) func(*process) error {
	return func(current *process) error {
		_, _, err := prepareRole(current, recipe)
		return err
	}
}

func TestEveryRoleLogsAtItsLogLevel(t *testing.T) {
	roles := map[string]func(*process) error{
		commandIngester:  preparedRole(ingesterRole()),
		commandProcessor: preparedRole(processorRole()),
		commandAll:       preparedRole(allRole()),
	}
	type args struct {
		role  string
		level string
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		{"the ingester at info", args{commandIngester, "info"}, true},
		{"the ingester at warn", args{commandIngester, "warn"}, false},
		{"the processor at info", args{commandProcessor, "info"}, true},
		{"the processor at warn", args{commandProcessor, "warn"}, false},
		{"all at info", args{commandAll, "info"}, true},
		{"all at warn", args{commandAll, "warn"}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var stderr bytes.Buffer
			current := processOf(processorEnvironment(t, map[string]string{
				envLogLevel: tt.args.level,
			}))
			current.ReadCgroup = noCgroup
			current.SetMemoryLimit = (&recordedLimit{}).setLimit
			current.Stderr = &stderr
			if err := roles[tt.args.role](current); err != nil {
				t.Fatalf("prepareRole: %v", err)
			}
			assertDiff(t, "config.memory_limit logged", tt.want, strings.Contains(stderr.String(), "config.memory_limit"))
		})
	}
}
