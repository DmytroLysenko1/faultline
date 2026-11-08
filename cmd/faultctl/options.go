package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"maps"
	"net/url"
	"slices"
	"strings"
	"time"

	"github.com/DmytroLysenko1/faultline/internal/cli"
)

const (
	commandGraph   = "graph"
	commandExport  = "export"
	envServer      = "FAULTCTL_SERVER"
	defaultServer  = "http://127.0.0.1:8080"
	defaultTimeout = 10 * time.Second
	maxTimeout     = 5 * time.Minute
	flagFormat     = "o"
	flagServer     = "server"
	flagTimeout    = "timeout"
	schemeHTTP     = "http"
	schemeHTTPS    = "https"
	formatSep      = "|"
)

var (
	errCommand     = errors.New("unknown command, want graph export")
	errArgument    = errors.New("unexpected positional argument")
	errFormat      = errors.New("unknown output format")
	errServer      = errors.New("invalid server, want http://host:port")
	errCredentials = errors.New("credentials in the server URL are not supported")
	errTimeout     = errors.New("timeout out of range")
)

type exportOptions struct {
	Server       string
	Timeout      time.Duration
	ReadMaxBytes int
	Render       cli.Renderer
}

type rawOptions struct {
	format  string
	server  string
	timeout time.Duration
}

func parseExport(args []string, lookup lookupFunc, renderers map[string]cli.Renderer) (exportOptions, error) {
	if !isExportCommand(args) {
		return exportOptions{}, errCommand
	}
	raw, err := parseFlags(args[len(exportCommand()):], lookup)
	if err != nil {
		return exportOptions{}, err
	}
	return raw.validated(renderers)
}

func exportCommand() []string {
	return []string{commandGraph, commandExport}
}

func isExportCommand(args []string) bool {
	command := exportCommand()
	return len(args) >= len(command) && slices.Equal(args[:len(command)], command)
}

func (raw rawOptions) validated(renderers map[string]cli.Renderer) (exportOptions, error) {
	render, err := rendererFor(raw.format, renderers)
	if err != nil {
		return exportOptions{}, err
	}
	if err := checkServer(raw.server); err != nil {
		return exportOptions{}, err
	}
	if err := checkTimeout(raw.timeout); err != nil {
		return exportOptions{}, err
	}
	return exportOptions{
		Server:       raw.server,
		Timeout:      raw.timeout,
		ReadMaxBytes: readMaxBytes,
		Render:       render,
	}, nil
}

func parseFlags(args []string, lookup lookupFunc) (rawOptions, error) {
	flags := flag.NewFlagSet(commandExport, flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	var raw rawOptions
	flags.StringVar(&raw.format, flagFormat, "", "")
	flags.StringVar(&raw.server, flagServer, serverDefault(lookup), "")
	flags.DurationVar(&raw.timeout, flagTimeout, defaultTimeout, "")
	if err := flags.Parse(args); err != nil {
		return rawOptions{}, err
	}
	if flags.NArg() != 0 {
		return rawOptions{}, errArgument
	}
	return raw, nil
}

func serverDefault(lookup lookupFunc) string {
	if server, set := lookup(envServer); set && server != "" {
		return server
	}
	return defaultServer
}

func rendererFor(format string, renderers map[string]cli.Renderer) (cli.Renderer, error) {
	render, found := renderers[format]
	if !found {
		known := slices.Sorted(maps.Keys(renderers))
		return nil, fmt.Errorf("-%s %q: %w, want %s", flagFormat, format, errFormat, strings.Join(known, formatSep))
	}
	return render, nil
}

func checkServer(raw string) error {
	parsed, err := url.Parse(raw)
	if err != nil {
		return fmt.Errorf("-%s: %w", flagServer, errServer)
	}
	if parsed.User != nil {
		return fmt.Errorf("-%s: %w", flagServer, errCredentials)
	}
	if !isPlainHTTP(parsed) {
		return fmt.Errorf("-%s: %w", flagServer, errServer)
	}
	return nil
}

func isPlainHTTP(parsed *url.URL) bool {
	schemes := []string{schemeHTTP, schemeHTTPS}
	bare := parsed.RawQuery == "" && !parsed.ForceQuery && parsed.Fragment == ""
	return slices.Contains(schemes, parsed.Scheme) && parsed.Host != "" && bare
}

func checkTimeout(timeout time.Duration) error {
	if timeout > 0 && timeout <= maxTimeout {
		return nil
	}
	return fmt.Errorf("-%s %s: %w, want > 0 and ≤ %s", flagTimeout, timeout, errTimeout, maxTimeout)
}
