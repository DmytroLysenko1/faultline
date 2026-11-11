package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"connectrpc.com/connect"

	"github.com/DmytroLysenko1/faultline/internal/cli"
	"github.com/DmytroLysenko1/faultline/internal/gen/proto/faultline/v1/faultlinev1connect"
)

const (
	usage        = "usage: faultctl graph export -o dot|json [--server URL] [--timeout 10s]"
	readMaxBytes = 192 << 20
	formatDOT    = "dot"
	formatJSON   = "json"
	exitOK       = 0
	exitFailure  = 1
	exitUsage    = 2
)

type lookupFunc func(name string) (string, bool)

type process struct {
	Args   []string
	Lookup lookupFunc
	Stdout io.Writer
	Stderr io.Writer
}

func realMain(current process) int {
	options, err := parseExport(current.Args, current.Lookup, renderers())
	if errors.Is(err, flag.ErrHelp) {
		return fail(current.Stdout, usage, exitOK)
	}
	if err != nil {
		return fail(current.Stderr, fmt.Sprintf("faultctl: %v\n%s", err, usage), exitUsage)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	context.AfterFunc(ctx, stop)
	if err := export(ctx, current.Stdout, &options); err != nil {
		return fail(current.Stderr, describe(err, options.Server), exitFailure)
	}
	return exitOK
}

func renderers() map[string]cli.Renderer {
	return map[string]cli.Renderer{
		formatDOT:  cli.DOT,
		formatJSON: cli.JSON,
	}
}

func export(ctx context.Context, stdout io.Writer, options *exportOptions) error {
	callCtx, cancel := context.WithTimeout(ctx, options.Timeout)
	defer cancel()
	transport := &http.Transport{
		DisableKeepAlives: true,
	}
	defer transport.CloseIdleConnections()
	client := faultlinev1connect.NewGraphServiceClient(&http.Client{
		Transport: transport,
	}, options.Server, connect.WithReadMaxBytes(options.ReadMaxBytes))
	return cli.Export(callCtx, client, options.Render, stdout)
}

func describe(err error, server string) string {
	if errors.Is(err, cli.ErrWrite) {
		return "faultctl: " + cli.Printable(err.Error())
	}
	return fmt.Sprintf("faultctl: server %s: %s", server, cli.Printable(err.Error()))
}

func fail(stderr io.Writer, message string, code int) int {
	if _, err := fmt.Fprintln(stderr, message); err != nil {
		return exitFailure
	}
	return code
}
