package main

import (
	"context"
	"crypto/rand"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"golang.org/x/term"

	"github.com/DmytroLysenko1/faultline/internal/application/tokenadmin"
	"github.com/DmytroLysenko1/faultline/internal/domain/credential"
	"github.com/DmytroLysenko1/faultline/internal/infrastructure/natsjs"
	"github.com/DmytroLysenko1/faultline/internal/infrastructure/natsjs/record"
	"github.com/DmytroLysenko1/faultline/internal/infrastructure/natsjs/tokens"
)

const (
	commandTokens = "tokens"
	actionInit    = "init"
	actionIssue   = "issue"
	actionRevoke  = "revoke"
	actionList    = "list"

	flagTenant     = "tenant"
	flagOutputFile = "output-file"
	flagStdout     = "stdout"

	tokenSettle       = 7 * time.Second
	tokenCompensation = 6 * time.Second
	tokenSetupBudget  = 3 * time.Second

	tokensUsage = "usage: faultline tokens init | issue [--tenant <tenant>] (--output-file <path> | --stdout) | " +
		"revoke <id> | list"
	revokedWithin    = "every ingester refuses it within 22s while NATS is reachable"
	acceptedWithin   = "every ingester accepts it within 22s while NATS is reachable; switch clients after 30s"
	revocationAbsent = "is absent: revoked just now or never existed; " + hintCheckList
)

var (
	errTokensUsage = errors.New(tokensUsage)
	errTwoOutputs  = errors.New("pass only one of --output-file and --stdout")
	errNoOutput    = errors.New("stdout is not a terminal: pass --output-file <path> or --stdout")
)

type tokensRequest struct {
	nats     natsConfig
	replicas int
	tenant   string
	file     string
	id       credential.ID
}

type connCloser interface {
	Close(ctx context.Context) error
}

type tokensAction struct {
	parse func(current *process, args []string) (tokensRequest, error)
	run   func(ctx context.Context, conn *natsjs.Conn, request *tokensRequest, out *tokenOutput) error
}

func tokensCommand(current *process) int {
	signal.Ignore(syscall.SIGPIPE)
	request, action, err := parseTokens(current)
	if err != nil {
		return fail(current.Stderr, err)
	}
	ctx, stopSignals := signalNotify(context.Background())
	defer stopSignals()
	out, err := openTokenOutput(current, &request)
	if err != nil {
		return fail(current.Stderr, err)
	}
	if err := errors.Join(runTokens(ctx, &request, action, out), out.release()); err != nil {
		return out.failed(err)
	}
	return exitOK
}

func tokensActions() map[string]tokensAction {
	return map[string]tokensAction{
		actionInit: {
			parse: parseBucketAction,
			run:   initTokens,
		},
		actionIssue: {
			parse: parseIssue,
			run:   issueToken,
		},
		actionRevoke: {
			parse: parseRevoke,
			run:   revokeToken,
		},
		actionList: {
			parse: parsePlain,
			run:   listTokens,
		},
	}
}

func parseTokens(current *process) (tokensRequest, tokensAction, error) {
	action, known := tokensActions()[current.Args[1]]
	if !known {
		return tokensRequest{}, tokensAction{}, errTokensUsage
	}
	request, err := action.parse(current, current.Args[2:])
	nats, natsErr := loadNATS(current.Lookup, current.Hostname)
	request.nats = nats
	return request, action, errors.Join(err, natsErr)
}

func parsePlain(_ *process, args []string) (tokensRequest, error) {
	if len(args) > 0 {
		return tokensRequest{}, errTokensUsage
	}
	return tokensRequest{}, nil
}

func parseBucketAction(current *process, args []string) (tokensRequest, error) {
	request, err := parsePlain(current, args)
	streams, streamsErr := loadStreams(current.Lookup)
	request.replicas = streams.Replicas
	return request, errors.Join(err, streamsErr)
}

func parseRevoke(_ *process, args []string) (tokensRequest, error) {
	if len(args) != 1 {
		return tokensRequest{}, errTokensUsage
	}
	id, err := credential.ParseID(args[0])
	if err != nil {
		return tokensRequest{}, fmt.Errorf("revoke %q: %w", args[0], err)
	}
	return tokensRequest{
		id: id,
	}, nil
}

func parseIssue(current *process, args []string) (tokensRequest, error) {
	flags := flag.NewFlagSet(actionIssue, flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	tenant := flags.String(flagTenant, "", "")
	file := flags.String(flagOutputFile, "", "")
	stdout := flags.Bool(flagStdout, false, "")
	if err := flags.Parse(args); err != nil || flags.NArg() > 0 {
		return tokensRequest{}, errors.Join(errTokensUsage, err)
	}
	streams, streamsErr := loadStreams(current.Lookup)
	request := tokensRequest{
		replicas: streams.Replicas,
		tenant:   *tenant,
		file:     *file,
	}
	outputErr := outputChoice(current.Stdout, *file, *stdout)
	tenantErr := issueTenant(current.Lookup, &request, isSet(flags, flagTenant))
	return request, errors.Join(outputErr, tenantErr, streamsErr)
}

func isSet(flags *flag.FlagSet, name string) bool {
	set := false
	flags.Visit(func(visited *flag.Flag) {
		set = set || visited.Name == name
	})
	return set
}

func outputChoice(stdout *os.File, file string, toStdout bool) error {
	switch {
	case file != "" && toStdout:
		return errTwoOutputs
	case file != "":
		return nil
	case toStdout:
		return privateStdout(stdout)
	}
	return terminalOutput(stdout)
}

func terminalOutput(stdout *os.File) error {
	if !term.IsTerminal(int(stdout.Fd())) { //nolint:gosec // G115: a file descriptor fits an int
		return errNoOutput
	}
	return nil
}

func issueTenant(lookup lookupFunc, request *tokensRequest, flagged bool) error {
	if !flagged {
		reader := newEnvReader(lookup)
		request.tenant = reader.tenant(envTenant)
		return errors.Join(reader.errs...)
	}
	if err := record.ValidateTenant(request.tenant); err != nil {
		return fmt.Errorf("--%s: %w", flagTenant, err)
	}
	return nil
}

func runTokens(ctx context.Context, request *tokensRequest, action tokensAction, out *tokenOutput) error {
	options := request.nats.connOptions(roleLogger(out.stderr, slog.LevelWarn))
	dialCtx, cancel := context.WithTimeout(ctx, dialBudget(options))
	defer cancel()
	conn, err := natsjs.Dial(dialCtx, options)
	if err != nil {
		return fmt.Errorf("connect to NATS: %w", err)
	}
	actionErr := action.run(ctx, conn, request, out)
	return errors.Join(actionErr, closeTokens(ctx, conn, out))
}

func closeTokens(ctx context.Context, conn connCloser, out *tokenOutput) error {
	closeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), connCloseBudget)
	defer cancel()
	closeErr := conn.Close(closeCtx)
	if closeErr == nil {
		return nil
	}
	return out.note("close the NATS connection: %v", closeErr)
}

func initTokens(ctx context.Context, conn *natsjs.Conn, request *tokensRequest, out *tokenOutput) error {
	ctx, cancel := context.WithTimeout(ctx, tokenSettle)
	defer cancel()
	if err := tokens.ReconcileBucket(ctx, conn, request.replicas); err != nil {
		return fmt.Errorf("reconcile the token bucket: %w", err)
	}
	store, err := tokens.OpenStore(ctx, conn)
	if err != nil {
		return fmt.Errorf("open the token bucket: %w", err)
	}
	grants, err := store.List(ctx)
	if err != nil {
		return fmt.Errorf("list ingest tokens: %w", err)
	}
	return out.note("%s", bucketSummary(len(grants), store.ConfigNotes()))
}

func bucketSummary(grants int, kept []string) string {
	summary := fmt.Sprintf("token bucket ready, grants: %d", grants)
	if len(kept) == 0 {
		return summary
	}
	return summary + "; kept as found: " + strings.Join(kept, ", ")
}

func issueToken(ctx context.Context, conn *natsjs.Conn, request *tokensRequest, out *tokenOutput) error {
	setupCtx, cancel := context.WithTimeout(ctx, tokenSetupBudget)
	defer cancel()
	if err := tokens.CreateBucket(setupCtx, conn, request.replicas); err != nil {
		return fmt.Errorf("create the token bucket: %w", err)
	}
	admin, err := openAdmin(setupCtx, conn, out)
	if err != nil {
		return err
	}
	id, err := admin.Issue(ctx, request.tenant)
	if err != nil {
		return err
	}
	return out.note("ingest token %s for tenant %s is stored; %s", id, request.tenant, acceptedWithin)
}

func revokeToken(ctx context.Context, conn *natsjs.Conn, request *tokensRequest, out *tokenOutput) error {
	admin, err := openAdmin(ctx, conn, out)
	if err != nil {
		return err
	}
	revocation, err := admin.Revoke(ctx, request.id)
	if err != nil {
		return err
	}
	if revocation == tokenadmin.Absent {
		return out.note("ingest token %s %s", request.id, revocationAbsent)
	}
	return out.note("ingest token %s revoked; %s", request.id, revokedWithin)
}

func listTokens(ctx context.Context, conn *natsjs.Conn, _ *tokensRequest, out *tokenOutput) error {
	admin, err := openAdmin(ctx, conn, out)
	if err != nil {
		return err
	}
	grants, err := admin.List(ctx)
	if err != nil {
		return err
	}
	return out.list(grants)
}

func openAdmin(ctx context.Context, conn *natsjs.Conn, out *tokenOutput) (*tokenadmin.Admin, error) {
	openCtx, cancel := context.WithTimeout(ctx, tokenSetupBudget)
	defer cancel()
	store, err := tokens.OpenStore(openCtx, conn)
	if err != nil {
		return nil, fmt.Errorf("open the token bucket: %w", err)
	}
	return tokenadmin.NewAdmin(store, rand.Reader, systemClock{}, out, tokenadmin.Budget{
		Settle:       tokenSettle,
		Compensation: tokenCompensation,
	})
}
