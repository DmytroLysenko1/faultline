package main

import (
	"bytes"
	"context"
	"errors"
	"io"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"google.golang.org/grpc/codes"

	"github.com/DmytroLysenko1/faultline/internal/application/tokenadmin"
	"github.com/DmytroLysenko1/faultline/internal/domain/credential"
	"github.com/DmytroLysenko1/faultline/internal/infrastructure/natsjs"
	"github.com/DmytroLysenko1/faultline/internal/infrastructure/natsjs/natsjstest"
	"github.com/DmytroLysenko1/faultline/internal/infrastructure/natsjs/tokens"
	"github.com/DmytroLysenko1/faultline/internal/interfaces/otlp/otlptest"
)

const (
	tokenFileName      = "token"
	ownerOnly          = fs.FileMode(0o600)
	visibleOnIngesters = 22*time.Second + time.Second
	exportPoll         = 250 * time.Millisecond
	tokenPurgePrefix   = "$JS.API.STREAM.PURGE." + tokenStream
	shownAndRevoked    = "issued ingest token could not be shown and was revoked"
	anotherTenant      = "shop"
	secondTokenID      = "secondtoken2"
	sharedStdoutTail   = "pass --output-file <path> instead of --stdout\n"
)

type tokensRun struct {
	Code   int
	Stdout string
	Stderr string
}

func tokensEnvironment(url string) map[string]string {
	return map[string]string{
		envNATSURL:  url,
		envInstance: testInstance,
	}
}

func capturedStdout(t *testing.T) *os.File {
	t.Helper()
	file, err := os.CreateTemp(t.TempDir(), "stdout")
	if err != nil {
		t.Fatalf("CreateTemp: %v", err)
	}
	t.Cleanup(func() {
		if err := file.Close(); err != nil {
			t.Errorf("close stdout: %v", err)
		}
	})
	return file
}

func contentsOf(t *testing.T, path string) string {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	return string(data)
}

func tokensWith(t *testing.T, values map[string]string, stdout *os.File, args ...string) tokensRun {
	t.Helper()
	var stderr bytes.Buffer
	code := realMain(&process{
		Args:     append([]string{commandTokens}, args...),
		Lookup:   environment(values),
		Stdout:   stdout,
		Stderr:   &stderr,
		Hostname: podHostname,
	})
	return tokensRun{
		Code:   code,
		Stdout: contentsOf(t, stdout.Name()),
		Stderr: stderr.String(),
	}
}

func tokensOn(t *testing.T, url string, args ...string) tokensRun {
	t.Helper()
	return tokensWith(t, tokensEnvironment(url), capturedStdout(t), args...)
}

func shownToken(t *testing.T, stdout string) credential.IngestToken {
	t.Helper()
	lines := strings.Split(strings.TrimSuffix(stdout, "\n"), "\n")
	if len(lines) != 1 {
		t.Fatalf("stdout holds %d lines, want the token alone: %q", len(lines), stdout)
	}
	token, err := credential.Parse(lines[0])
	if err != nil {
		t.Fatalf("stdout %q is not a token: %v", stdout, err)
	}
	return token
}

func secretOf(token credential.IngestToken) string {
	return credential.Reveal(token)[len(credential.Prefix)+credential.IDLength+1:]
}

func bucketGrants(t *testing.T, url string) []credential.Grant {
	t.Helper()
	conn := dialOnce(t, url)
	defer closeOnce(t, conn)
	store, err := tokens.OpenStore(t.Context(), conn)
	if err != nil {
		t.Fatalf("OpenStore: %v", err)
	}
	grants, err := store.List(t.Context())
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	return grants
}

func bucketConfig(t *testing.T, url string) (jetstream.StreamConfig, bool) {
	t.Helper()
	stream, err := natsjstest.JetStream(t, url).Stream(t.Context(), tokenStream)
	if errors.Is(err, jetstream.ErrStreamNotFound) {
		return jetstream.StreamConfig{}, false
	}
	if err != nil {
		t.Fatalf("Stream: %v", err)
	}
	return stream.CachedInfo().Config, true
}

func grantIDs(grants []credential.Grant) []credential.ID {
	ids := make([]credential.ID, 0, len(grants))
	for index := range grants {
		ids = append(ids, grants[index].ID)
	}
	return ids
}

func TestTokensIssuePrintsTheTokenOnce(t *testing.T) {
	cluster := natsjstest.Start(t, singleNATSNode)

	run := tokensOn(t, cluster.URLs()[0], actionIssue, "--stdout", "--tenant", anotherTenant)

	token := shownToken(t, run.Stdout)
	grants := bucketGrants(t, cluster.URLs()[0])
	if len(grants) != 1 || !grants[0].Digest.Equal(token.Digest()) {
		t.Fatalf("bucket grants %v, want the shown token alone", grants)
	}
	want := "issuing ingest token " + string(token.ID) + " for tenant shop\n" +
		"ingest token " + string(token.ID) + " for tenant shop is stored; " +
		"every ingester accepts it within 22s while NATS is reachable; switch clients after 30s\n"
	assertDiff(t, "code, stderr and the stored grant", []any{exitOK, want, token.ID, anotherTenant}, []any{
		run.Code,
		run.Stderr,
		grants[0].ID,
		grants[0].Tenant,
	})
}

func TestIssueTakesItsTenantFromTheEnvironmentUnlessTheFlagNamesOne(t *testing.T) {
	tests := []struct {
		name string
		args []string
		want string
	}{
		{
			name: "no flag takes TENANT",
			args: []string{
				actionIssue,
				"--stdout",
			},
			want: "orders",
		},
		{
			name: "the flag overrides TENANT",
			args: []string{
				actionIssue,
				"--stdout",
				"--tenant=" + anotherTenant,
			},
			want: anotherTenant,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cluster := natsjstest.Start(t, singleNATSNode)
			values := tokensEnvironment(cluster.URLs()[0])
			values[envTenant] = "orders"

			run := tokensWith(t, values, capturedStdout(t), tt.args...)

			grants := bucketGrants(t, cluster.URLs()[0])
			if run.Code != exitOK || len(grants) != 1 {
				t.Fatalf("code %d, grants %v, stderr %s", run.Code, grants, run.Stderr)
			}
			assertDiff(t, "tenant of the grant", tt.want, grants[0].Tenant)
		})
	}
}

func TestIssueRefusesANonTerminalStdoutWithoutAFlag(t *testing.T) {
	tests := []struct {
		name string
		args []string
		want string
	}{
		{
			name: "no output named",
			args: []string{
				actionIssue,
			},
			want: "stdout is not a terminal: pass --output-file <path> or --stdout\n",
		},
		{
			name: "both outputs named",
			args: []string{
				actionIssue,
				"--stdout",
				"--output-file",
				filepath.Join(os.TempDir(), "never-written"),
			},
			want: "pass only one of --output-file and --stdout\n",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cluster := natsjstest.Start(t, singleNATSNode)

			run := tokensOn(t, cluster.URLs()[0], tt.args...)

			_, created := bucketConfig(t, cluster.URLs()[0])
			assertDiff(t, "refusal before any I/O", tokensRun{
				Code:   exitConfig,
				Stderr: tt.want,
			}, run)
			assertDiff(t, "bucket created", false, created)
		})
	}
}

func TestTokensRefusesItsArgumentsWithCodeTwo(t *testing.T) {
	tests := []struct {
		name string
		args []string
		want string
	}{
		{
			name: "no action",
			want: usage + "\n",
		},
		{
			name: "an unknown action",
			args: []string{
				"rotate",
			},
			want: tokensUsage + "\n",
		},
		{
			name: "list with an argument",
			args: []string{
				actionList,
				"extra",
			},
			want: tokensUsage + "\n",
		},
		{
			name: "revoke without an id",
			args: []string{
				actionRevoke,
			},
			want: tokensUsage + "\n",
		},
		{
			name: "revoke of a malformed id",
			args: []string{
				actionRevoke,
				"NOT-AN-ID",
			},
			want: "revoke \"NOT-AN-ID\": malformed ingest token\n",
		},
		{
			name: "issue with an unknown flag",
			args: []string{
				actionIssue,
				"--stdout",
				"--ttl=1h",
			},
			want: tokensUsage + "\nflag provided but not defined: -ttl\n",
		},
		{
			name: "issue with a stray argument",
			args: []string{
				actionIssue,
				"--stdout",
				"shop",
			},
			want: tokensUsage + "\n",
		},
		{
			name: "issue for an invalid tenant",
			args: []string{
				actionIssue,
				"--stdout",
				"--tenant=Shop_1",
			},
			want: "--tenant: journal record tenant invalid: \"Shop_1\" is not a DNS label of at most 63 bytes\n",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			run := tokensOn(t, natsjstest.RefusedURL(t), tt.args...)

			assertDiff(t, "refusal", tokensRun{
				Code:   exitConfig,
				Stderr: tt.want,
			}, run)
		})
	}
}

func TestTokensNeedNATSConfig(t *testing.T) {
	run := tokensWith(t, map[string]string{}, capturedStdout(t), actionList)

	assertDiff(t, "refusal", tokensRun{
		Code:   exitConfig,
		Stderr: "NATS_URL: required\n",
	}, run)
}

func TestIssueToAPipeWithStdoutFlagKeepsTheToken(t *testing.T) {
	cluster := natsjstest.Start(t, singleNATSNode)
	reader, writer, err := os.Pipe()
	if err != nil {
		t.Fatalf("Pipe: %v", err)
	}
	defer func() {
		if err := reader.Close(); err != nil {
			t.Errorf("close reader: %v", err)
		}
	}()
	var stderr bytes.Buffer

	code := realMain(&process{
		Args: []string{
			commandTokens,
			actionIssue,
			"--stdout",
		},
		Lookup:   environment(tokensEnvironment(cluster.URLs()[0])),
		Stdout:   writer,
		Stderr:   &stderr,
		Hostname: podHostname,
	})
	if err := writer.Close(); err != nil {
		t.Fatalf("close writer: %v", err)
	}
	piped, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("read the pipe: %v", err)
	}

	token := shownToken(t, string(piped))
	assertDiff(t, "code and stored ids", []any{exitOK, []credential.ID{token.ID}}, []any{
		code,
		grantIDs(bucketGrants(t, cluster.URLs()[0])),
	})
}

func TestIssueSurvivesAClosedPipe(t *testing.T) {
	cluster := natsjstest.Start(t, singleNATSNode)
	reader, writer, err := os.Pipe()
	if err != nil {
		t.Fatalf("Pipe: %v", err)
	}
	if err := reader.Close(); err != nil {
		t.Fatalf("close reader: %v", err)
	}
	command := exec.CommandContext(t.Context(), os.Args[0], commandTokens, actionIssue, "--stdout")
	command.Env = append(os.Environ(), reexecFlag+"=1", envNATSURL+"="+cluster.URLs()[0], envInstance+"="+testInstance)
	command.Stdout = writer
	var stderr bytes.Buffer
	command.Stderr = &stderr

	runErr := command.Run()
	if err := writer.Close(); err != nil {
		t.Fatalf("close writer: %v", err)
	}

	status, _ := command.ProcessState.Sys().(syscall.WaitStatus)
	if status.Signaled() {
		t.Fatalf("the command died of %v (%v); stderr %s", status.Signal(), runErr, stderr.String())
	}
	assertDiff(t, "code, revoked token and grants left", []any{exitFailure, true, 0}, []any{
		command.ProcessState.ExitCode(),
		strings.Contains(stderr.String(), shownAndRevoked),
		len(bucketGrants(t, cluster.URLs()[0])),
	})
}

func TestIssueWritesTheTokenOnlyToTheOutputFile(t *testing.T) {
	cluster := natsjstest.Start(t, singleNATSNode)
	path := filepath.Join(t.TempDir(), tokenFileName)

	first := tokensOn(t, cluster.URLs()[0], actionIssue, "--output-file", path)
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}
	written := contentsOf(t, path)
	token := shownToken(t, written)
	second := tokensOn(t, cluster.URLs()[0], actionIssue, "--output-file", path)

	assertDiff(t, "first issue: code, stdout and mode", []any{exitOK, "", ownerOnly}, []any{
		first.Code,
		first.Stdout,
		info.Mode().Perm(),
	})
	assertDiff(t, "second issue into the same file", []any{exitConfig, "", written}, []any{
		second.Code,
		second.Stdout,
		contentsOf(t, path),
	})
	assertDiff(t, "stored ids", []credential.ID{token.ID}, grantIDs(bucketGrants(t, cluster.URLs()[0])))
}

type failingFile struct {
	file     *os.File
	failing  string
	failures error
}

func (f *failingFile) Write(p []byte) (int, error) {
	if f.failing == "write" {
		return 0, f.failures
	}
	return f.file.Write(p)
}

func (f *failingFile) Sync() error {
	if f.failing == "sync" {
		return f.failures
	}
	return f.file.Sync()
}

func (f *failingFile) Close() error {
	closed := f.file.Close()
	if f.failing == "close" {
		return f.failures
	}
	return closed
}

func TestAFailedShowLeavesNoTokenFile(t *testing.T) {
	errDisk := errors.New("disk failed")
	tests := []struct {
		name    string
		args    string
		wantErr error
	}{
		{
			name:    "a failed write",
			args:    "write",
			wantErr: errDisk,
		},
		{
			name:    "a failed sync",
			args:    "sync",
			wantErr: errDisk,
		},
		{
			name:    "a failed close",
			args:    "close",
			wantErr: errDisk,
		},
		{
			name:    "a failed sync of the directory",
			args:    "directory",
			wantErr: errDisk,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), tokenFileName)
			out, err := openTokenOutput(&process{}, &tokensRequest{
				file: path,
			})
			if err != nil {
				t.Fatalf("openTokenOutput: %v", err)
			}
			out.file = &failingFile{
				file:     out.file.(*os.File),
				failing:  tt.args,
				failures: errDisk,
			}
			out.syncDir = func(directory string) error {
				if tt.args == "directory" {
					return errDisk
				}
				return syncDirectory(directory)
			}
			token, err := credential.Parse(otlptest.Token)
			if err != nil {
				t.Fatalf("Parse: %v", err)
			}

			showErr := out.Show(t.Context(), token)

			_, statErr := os.Stat(path)
			if !errors.Is(showErr, tt.wantErr) || !errors.Is(statErr, fs.ErrNotExist) {
				t.Errorf("Show = %v, file %v; want %v and no file", showErr, statErr, tt.wantErr)
			}
		})
	}
}

func TestAnOutputFileIsOpenedBeforeAnyIOToNATS(t *testing.T) {
	tests := []struct {
		name    string
		args    string
		want    bool
		wantErr error
	}{
		{
			name:    "an existing file",
			args:    tokenFileName,
			want:    true,
			wantErr: fs.ErrExist,
		},
		{
			name:    "a missing directory",
			args:    filepath.Join("missing", tokenFileName),
			want:    false,
			wantErr: fs.ErrNotExist,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			directory := t.TempDir()
			existing := filepath.Join(directory, tokenFileName)
			if err := os.WriteFile(existing, []byte("kept\n"), ownerOnly); err != nil {
				t.Fatalf("WriteFile: %v", err)
			}
			path := filepath.Join(directory, tt.args)

			run := tokensOn(t, natsjstest.RefusedURL(t), actionIssue, "--output-file", path)
			_, openErr := openTokenOutput(&process{}, &tokensRequest{
				file: path,
			})

			assertDiff(t, "code, the named file, the hint, the existing file", []any{
				exitConfig,
				true,
				tt.want,
				true,
				"kept\n",
			}, []any{
				run.Code,
				strings.HasPrefix(run.Stderr, "create "+path+": "),
				strings.HasSuffix(run.Stderr, "\nremove "+path+" if it is empty\n"),
				errors.Is(openErr, tt.wantErr),
				contentsOf(t, existing),
			})
		})
	}
}

func TestAFailedIssueRemovesItsEmptyOutputFile(t *testing.T) {
	cluster := natsjstest.Start(t, singleNATSNode)
	url := cluster.URLs()[0]
	createTokenBucket(t, url)
	driftTokenBucket(t, url)
	path := filepath.Join(t.TempDir(), tokenFileName)

	run := tokensOn(t, url, actionIssue, "--output-file", path)

	_, statErr := os.Stat(path)
	assertDiff(t, "code and the file left", []any{exitFailure, true}, []any{
		run.Code,
		errors.Is(statErr, fs.ErrNotExist),
	})
}

func TestStdoutTakesTheTokenOnlyIfOthersCannotReadIt(t *testing.T) {
	tests := []struct {
		name string
		args fs.FileMode
		want tokensRun
	}{
		{
			name: "a file of its owner",
			args: ownerOnly,
			want: tokensRun{
				Code:   exitFailure,
				Stderr: "faultline tokens: connect to NATS: ",
			},
		},
		{
			name: "a file the group reads",
			args: 0o640,
			want: tokensRun{
				Code:   exitConfig,
				Stderr: "stdout is a file with mode -rw-r----- that others can read: " + sharedStdoutTail,
			},
		},
		{
			name: "a file everyone reads",
			args: 0o604,
			want: tokensRun{
				Code:   exitConfig,
				Stderr: "stdout is a file with mode -rw----r-- that others can read: " + sharedStdoutTail,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stdout := capturedStdout(t)
			if err := stdout.Chmod(tt.args); err != nil {
				t.Fatalf("Chmod: %v", err)
			}

			run := tokensWith(t, tokensEnvironment(natsjstest.RefusedURL(t)), stdout, actionIssue, "--stdout")

			run.Stderr = run.Stderr[:min(len(run.Stderr), len(tt.want.Stderr))]
			assertDiff(t, "code and stderr", tt.want, run)
		})
	}
}

func TestACanceledCommandShowsNothing(t *testing.T) {
	reader, writer, err := os.Pipe()
	if err != nil {
		t.Fatalf("Pipe: %v", err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	token, err := credential.Parse(otlptest.Token)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	out := &tokenOutput{
		stdout: writer,
	}

	showErr := out.Show(ctx, token)
	if err := writer.Close(); err != nil {
		t.Fatalf("close writer: %v", err)
	}
	piped, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("read the pipe: %v", err)
	}
	if err := reader.Close(); err != nil {
		t.Fatalf("close reader: %v", err)
	}

	assertDiff(t, "a canceled show and the pipe", []any{true, ""}, []any{
		errors.Is(showErr, context.Canceled),
		string(piped),
	})
}

func TestRevokeExplainsEachOutcome(t *testing.T) {
	cluster := natsjstest.Start(t, singleNATSNode)
	proxy := natsjstest.StartProxy(t, cluster.URLs()[0])
	issued := tokensOn(t, cluster.URLs()[0], actionIssue, "--stdout")
	lost := tokensOn(t, cluster.URLs()[0], actionIssue, "--stdout")
	revokedID, lostID := shownToken(t, issued.Stdout).ID, shownToken(t, lost.Stdout).ID

	revoked := tokensOn(t, cluster.URLs()[0], actionRevoke, string(revokedID))
	again := tokensOn(t, cluster.URLs()[0], actionRevoke, string(revokedID))
	proxy.Intercept(once(tokenPurgePrefix, natsjstest.LoseReply))
	absent := tokensOn(t, proxy.URL(), actionRevoke, string(lostID))

	assertDiff(t, "revoked, nothing to revoke, absent after a lost reply", []tokensRun{
		{
			Code: exitOK,
			Stderr: "ingest token " + string(revokedID) +
				" revoked; every ingester refuses it within 22s while NATS is reachable\n",
		},
		{
			Code: exitFailure,
			Stderr: "faultline tokens: revoke ingest token " + string(revokedID) +
				": unknown or already revoked ingest token id\ncheck faultline tokens list\n",
		},
		{
			Code: exitOK,
			Stderr: "ingest token " + string(lostID) +
				" is absent: revoked just now or never existed; check faultline tokens list\n",
		},
	}, []tokensRun{revoked, again, absent})
	assertDiff(t, "grants left", 0, len(bucketGrants(t, cluster.URLs()[0])))
}

func once(prefix string, verdict func() natsjstest.Verdict) func(f *natsjstest.Frame) natsjstest.Verdict {
	var spent bool
	return func(f *natsjstest.Frame) natsjstest.Verdict {
		if spent || !strings.HasPrefix(f.Subject, prefix) {
			return natsjstest.Forward()
		}
		spent = true
		return verdict()
	}
}

func TestListPrintsEveryGrant(t *testing.T) {
	cluster := natsjstest.Start(t, singleNATSNode)
	grantToken(t, tokenWith(secondTokenID), anotherTenant, cluster.URLs()...)
	grantTestToken(t, defaultTenant, cluster.URLs()...)
	testID := otlptest.Token[len(credential.Prefix) : len(credential.Prefix)+credential.IDLength]

	run := tokensOn(t, cluster.URLs()[0], actionList)

	want := secondTokenID + "\tshop\t2026-11-01T00:00:00Z\n" + testID + "\tdefault\t2026-11-01T00:00:00Z\n"
	if testID < secondTokenID {
		want = testID + "\tdefault\t2026-11-01T00:00:00Z\n" + secondTokenID + "\tshop\t2026-11-01T00:00:00Z\n"
	}
	assertDiff(t, "list", tokensRun{
		Code:   exitOK,
		Stdout: want,
	}, run)
}

func TestRevokeAndListAskForInitOnAMissingBucket(t *testing.T) {
	tests := []struct {
		name string
		args []string
	}{
		{
			name: "list",
			args: []string{
				actionList,
			},
		},
		{
			name: "revoke",
			args: []string{
				actionRevoke,
				secondTokenID,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cluster := natsjstest.Start(t, singleNATSNode)

			run := tokensOn(t, cluster.URLs()[0], tt.args...)

			assertDiff(t, "code and hint", []any{exitFailure, true}, []any{
				run.Code,
				strings.HasSuffix(run.Stderr, "\nrun faultline tokens init to create the token bucket\n"),
			})
		})
	}
}

func TestInitReconcilesTheBucketAndCountsItsGrants(t *testing.T) {
	cluster := natsjstest.Start(t, singleNATSNode)
	url := cluster.URLs()[0]

	created := tokensOn(t, url, actionInit)
	grantTestToken(t, defaultTenant, url)
	driftTokenBucket(t, url)
	changeTokenBucket(t, natsjstest.JetStream(t, url), func(config *jetstream.StreamConfig) {
		config.Compression = jetstream.S2Compression
	})
	repaired := tokensOn(t, url, actionInit)
	config, _ := bucketConfig(t, url)

	assertDiff(t, "init on nothing, init on a drifted bucket", []tokensRun{
		{
			Code:   exitOK,
			Stderr: "token bucket ready, grants: 0\n",
		},
		{
			Code:   exitOK,
			Stderr: "token bucket ready, grants: 1; kept as found: Compression\n",
		},
	}, []tokensRun{created, repaired})
	assertDiff(t, "max age and compression after init", []any{time.Duration(0), jetstream.S2Compression}, []any{
		config.MaxAge,
		config.Compression,
	})
}

func TestTheBucketSummaryListsEveryFieldKeptAsFound(t *testing.T) {
	assertDiff(t, "summary", "token bucket ready, grants: 2; kept as found: Compression, Replicas",
		bucketSummary(2, []string{"Compression", "Replicas"}))
}

func TestInitSendsAnIrreversibleBucketToTheRunbook(t *testing.T) {
	cluster := natsjstest.Start(t, singleNATSNode)
	createTokenBucket(t, cluster.URLs()...)
	changeTokenBucket(t, natsjstest.JetStream(t, cluster.URLs()[0]), func(config *jetstream.StreamConfig) {
		config.DenyPurge = true
	})

	run := tokensOn(t, cluster.URLs()[0], actionInit)

	assertDiff(t, "code and hint", []any{exitFailure, true}, []any{
		run.Code,
		strings.HasSuffix(run.Stderr, "\nfollow the runbook Tokens / recreate the bucket\n"),
	})
}

func TestIssueNeverRepairsADriftedBucket(t *testing.T) {
	cluster := natsjstest.Start(t, singleNATSNode)
	url := cluster.URLs()[0]
	createTokenBucket(t, url)
	driftTokenBucket(t, url)

	run := tokensOn(t, url, actionIssue, "--stdout")

	config, _ := bucketConfig(t, url)
	assertDiff(t, "code, stdout, hint, max age and grants", []any{exitFailure, "", true, driftedMaxAge, 0}, []any{
		run.Code,
		run.Stdout,
		strings.HasSuffix(run.Stderr, "\n"+hintBucketDrift+"\n"),
		config.MaxAge,
		len(bucketGrants(t, url)),
	})
}

func TestEveryTokenFailureNamesItsNextStep(t *testing.T) {
	tests := []struct {
		name string
		args error
		want string
	}{
		{
			name: "a token that may be live",
			args: tokenadmin.ErrLiveTokenNotShown,
			want: "faultline tokens: issued ingest token was not shown and may be live\n" +
				"the token may be live: run faultline tokens revoke announced12, then faultline tokens list\n",
		},
		{
			name: "a missing bucket",
			args: tokens.ErrBucketMissing,
			want: "faultline tokens: token bucket missing\nrun faultline tokens init to create the token bucket\n",
		},
		{
			name: "a bucket that needs recreating",
			args: tokens.ErrNeedsRecreate,
			want: "faultline tokens: token bucket drift cannot be fixed in place\n" +
				"follow the runbook Tokens / recreate the bucket\n",
		},
		{
			name: "an irreversible bucket",
			args: tokens.ErrIrreversible,
			want: "faultline tokens: token bucket denies purge or is sealed\n" +
				"follow the runbook Tokens / recreate the bucket\n",
		},
		{
			name: "a drifted bucket",
			args: tokens.ErrBucketDrift,
			want: "faultline tokens: token bucket configuration drifted\n" + hintBucketDrift + "\n",
		},
		{
			name: "an unknown id",
			args: tokenadmin.ErrUnknownToken,
			want: "faultline tokens: unknown or already revoked ingest token id\ncheck faultline tokens list\n",
		},
		{
			name: "a server whose max_payload no role accepts",
			args: natsjs.ErrPayloadLimit,
			want: "faultline tokens: nats max_payload is below the journal record limit\n" +
				"raise max_payload on the NATS server: tokens refuses a server that every faultline role refuses\n",
		},
		{
			name: "anything else",
			args: tokenadmin.ErrStoreUnavailable,
			want: "faultline tokens: ingest token store unavailable\n",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var stderr bytes.Buffer
			out := &tokenOutput{
				stderr:    &stderr,
				announced: "announced12",
			}

			code := out.failed(tt.args)

			assertDiff(t, "code and stderr", []any{exitFailure, tt.want}, []any{code, stderr.String()})
		})
	}
}

func TestIssuedTokenIsAcceptedAndRevokedOneIsNot(t *testing.T) {
	if testing.Short() {
		t.Skip("waits for the token directory to reload twice")
	}
	cluster := natsjstest.Start(t, singleNATSNode)
	url := cluster.URLs()[0]
	ensureStreams(t, url)
	first := shownToken(t, tokensOn(t, url, actionIssue, "--stdout").Stdout)
	logs := &logBuffer{}
	setup, _, otlpAddress := ingesterOn(t, cluster.URLs())
	notify, done, life := launchIngester(t, setup, logs)
	eventually(t, "serving", life.Served)
	firstCode := exportOnce(t, otlpAddress, otlptest.WithToken(credential.Reveal(first)))

	second := shownToken(t, tokensOn(t, url, actionIssue, "--stdout").Stdout)
	issuedAt := time.Now()
	eventuallyWithin(t, "the new token accepted", visibleOnIngesters, accepts(t, otlpAddress, second, codes.OK))
	acceptedAfter := time.Since(issuedAt)
	revoked := tokensOn(t, url, actionRevoke, string(first.ID))
	revokedAt := time.Now()
	eventuallyWithin(t, "the revoked token refused", visibleOnIngesters, accepts(t, otlpAddress, first, codes.Unauthenticated))
	refusedAfter := time.Since(revokedAt)
	secondCode := exportOnce(t, otlpAddress, otlptest.WithToken(credential.Reveal(second)))

	notify.send()
	assertCode(t, <-done, exitOK, logs)
	assertDiff(t, "first token, revoke code, second token", []any{codes.OK, exitOK, codes.OK}, []any{
		firstCode,
		revoked.Code,
		secondCode,
	})
	t.Logf("accepted after %s, refused after %s", acceptedAfter, refusedAfter)
}

func accepts(t *testing.T, address string, token credential.IngestToken, want codes.Code) func() bool {
	t.Helper()
	return func() bool {
		if exportOnce(t, address, otlptest.WithToken(credential.Reveal(token))) == want {
			return true
		}
		time.Sleep(exportPoll)
		return false
	}
}

var tokenShape = regexp.MustCompile(`flt_[a-z2-7]{12}_[A-Za-z0-9_-]{43}`)

func tamperedSecret(t *testing.T, token credential.IngestToken) string {
	t.Helper()
	raw := credential.Reveal(token)
	at := len(credential.Prefix) + credential.IDLength + 1
	replacement := "A"
	if raw[at:at+1] == replacement {
		replacement = "B"
	}
	return raw[:at] + replacement + raw[at+1:]
}

func TestTheTokenNeverAppearsInLogsErrorsOrStats(t *testing.T) {
	cluster := natsjstest.Start(t, singleNATSNode)
	url := cluster.URLs()[0]
	ensureStreams(t, url)
	issued := tokensOn(t, url, actionIssue, "--stdout")
	token := shownToken(t, issued.Stdout)
	secret := secretOf(token)
	logs := &logBuffer{}
	setup, _, otlpAddress := ingesterOn(t, cluster.URLs())
	notify, done, life := launchIngester(t, setup, logs)
	eventually(t, "serving", life.Served)
	codesSeen := []codes.Code{
		exportOnce(t, otlpAddress, otlptest.WithToken(credential.Reveal(token))),
		exportOnce(t, otlpAddress, otlptest.WithToken(tamperedSecret(t, token))),
	}
	path := filepath.Join(t.TempDir(), tokenFileName)
	if err := os.WriteFile(path, []byte("taken\n"), ownerOnly); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	refused := tokensOn(t, url, actionIssue, "--output-file", path)
	listed := tokensOn(t, url, actionList)
	revoked := tokensOn(t, url, actionRevoke, string(token.ID))
	notify.send()
	assertCode(t, <-done, exitOK, logs)

	assertDiff(t, "accepted, then refused with a wrong secret", []codes.Code{codes.OK, codes.Unauthenticated}, codesSeen)
	assertDiff(t, "the one show", []any{1, 1}, []any{
		strings.Count(issued.Stdout, secret),
		len(tokenShape.FindAllString(issued.Stdout, -1)),
	})
	outputs := map[string]string{
		"issue stderr":        issued.Stderr,
		"failed issue stdout": refused.Stdout,
		"failed issue stderr": refused.Stderr,
		"list stdout":         listed.Stdout,
		"list stderr":         listed.Stderr,
		"revoke stderr":       revoked.Stderr,
		"ingester logs":       logs.String(),
	}
	for name, output := range outputs {
		if strings.Contains(output, secret) || tokenShape.MatchString(output) {
			t.Errorf("%s carries a token: %s", name, output)
		}
	}
	rejected := logs.event(t, "otlp.auth.rejected")
	assertDiff(t, "the rejection names the id, the refused issue its file", []any{string(token.ID), true}, []any{
		rejected["token_id"],
		strings.Contains(refused.Stderr, path),
	})
}

func changeTokenBucket(t *testing.T, js jetstream.JetStream, change func(config *jetstream.StreamConfig)) {
	t.Helper()
	stream, err := js.Stream(t.Context(), tokenStream)
	if err != nil {
		t.Fatalf("Stream: %v", err)
	}
	config := stream.CachedInfo().Config
	change(&config)
	if _, err := js.UpdateStream(t.Context(), config); err != nil {
		t.Fatalf("UpdateStream: %v", err)
	}
}

func closedPipe(t *testing.T) *os.File {
	t.Helper()
	reader, writer, err := os.Pipe()
	if err != nil {
		t.Fatalf("Pipe: %v", err)
	}
	if err := reader.Close(); err != nil {
		t.Fatalf("close reader: %v", err)
	}
	t.Cleanup(func() {
		if err := writer.Close(); err != nil {
			t.Errorf("close writer: %v", err)
		}
	})
	return writer
}

func TestALiveTokenHintNamesTheAnnouncedID(t *testing.T) {
	cluster := natsjstest.Start(t, singleNATSNode)
	proxy := natsjstest.StartProxy(t, cluster.URLs()[0])
	proxy.Intercept(once(tokenPurgePrefix, natsjstest.LoseReply))
	stdout := closedPipe(t)
	var stderr bytes.Buffer

	code := realMain(&process{
		Args: []string{
			commandTokens,
			actionIssue,
			"--stdout",
		},
		Lookup:   environment(tokensEnvironment(proxy.URL())),
		Stdout:   stdout,
		Stderr:   &stderr,
		Hostname: podHostname,
	})

	announced := strings.Fields(strings.SplitN(stderr.String(), "\n", 2)[0] + " _ _ _ _")[3]
	_, malformed := credential.ParseID(announced)
	assertDiff(t, "code, the announced id and the hint", []any{exitFailure, nil, true}, []any{
		code,
		malformed,
		strings.HasSuffix(stderr.String(), "\nthe token may be live: run faultline tokens revoke "+announced+
			", then faultline tokens list\n"),
	})
}

type closedConn struct {
	err error
}

func (c closedConn) Close(context.Context) error {
	return c.err
}

func TestAFailedCloseOnlyAddsANote(t *testing.T) {
	tests := []struct {
		name string
		args error
		want string
	}{
		{
			name: "a closed connection",
			want: "",
		},
		{
			name: "a close past its budget",
			args: context.DeadlineExceeded,
			want: "close the NATS connection: context deadline exceeded\n",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var stderr bytes.Buffer

			err := closeTokens(t.Context(), closedConn{
				err: tt.args,
			}, &tokenOutput{
				stderr: &stderr,
			})

			assertDiff(t, "error and stderr", []any{nil, tt.want}, []any{err, stderr.String()})
		})
	}
}

func TestIssueRegistersWithinTheSettleBudgetNotTheSetupOne(t *testing.T) {
	cluster := natsjstest.Start(t, singleNATSNode)
	proxy := natsjstest.StartProxy(t, cluster.URLs()[0])
	firstPut := sync.OnceValue(time.Now)
	proxy.Intercept(func(f *natsjstest.Frame) natsjstest.Verdict {
		if !strings.HasPrefix(f.Subject, tokenPutPrefix) {
			return natsjstest.Forward()
		}
		if time.Since(firstPut()) < tokenSetupBudget {
			return natsjstest.Drop()
		}
		return natsjstest.Forward()
	})

	started := time.Now()
	run := tokensOn(t, proxy.URL(), actionIssue, "--stdout")

	token := shownToken(t, run.Stdout)
	assertDiff(t, "code, past the setup budget, stored ids", []any{exitOK, true, []credential.ID{token.ID}}, []any{
		run.Code,
		time.Since(started) > tokenSetupBudget,
		grantIDs(bucketGrants(t, cluster.URLs()[0])),
	})
}
