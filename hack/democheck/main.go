package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"os"

	"github.com/DmytroLysenko1/faultline/test/demo/reference"
)

const (
	exitOK       = 0
	exitFailed   = 1
	exitBadInput = 2

	defaultRequired = "test/demo/otel-demo/required-edges.txt"
	defaultOptional = "test/demo/otel-demo/optional-edges.txt"
)

var errFailed = errors.New("demo graph did not pass")

type options struct {
	graph        string
	required     string
	optional     string
	requiredOnly bool
	red          bool
}

func main() {
	os.Exit(run(os.Args[1:], os.Stdin, os.Stdout, os.Stderr))
}

func run(args []string, stdin io.Reader, stdout, stderr io.Writer) int {
	parsed, err := parseOptions(args)
	if err != nil {
		return report(stderr, err, exitBadInput)
	}
	verdict, err := check(&parsed, stdin)
	if err != nil {
		return report(stderr, err, exitBadInput)
	}
	if _, err := io.WriteString(stdout, verdict.String()); err != nil {
		return report(stderr, err, exitBadInput)
	}
	if !verdict.Passed() {
		return report(stderr, errFailed, exitFailed)
	}
	return exitOK
}

func parseOptions(args []string) (options, error) {
	flags := flag.NewFlagSet("democheck", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	var parsed options
	flags.StringVar(&parsed.graph, "graph", "", "faultctl graph export -o json output")
	flags.StringVar(&parsed.required, "required", defaultRequired, "required edges")
	flags.StringVar(&parsed.optional, "optional", defaultOptional, "optional edges")
	flags.BoolVar(&parsed.requiredOnly, "required-only", false, "check only that every required edge is present")
	flags.BoolVar(&parsed.red, "red", false, "also check final RED windows and the step 2 accounting in pipeline.stats")
	if err := flags.Parse(args); err != nil {
		return options{}, err
	}
	if parsed.graph == "" {
		return options{}, errNoGraph
	}
	return parsed, nil
}

func check(parsed *options, stdin io.Reader) (verdict, error) {
	expected, err := reference.Load(parsed.required, parsed.optional)
	if err != nil {
		return verdict{}, err
	}
	shown, err := readGraph(parsed.graph)
	if err != nil {
		return verdict{}, err
	}
	result := expected.Check(shown.edges)
	finals := redFinals(parsed.red, &expected, &shown)
	if parsed.requiredOnly {
		built := requiredOnly(&expected, &shown, result)
		built.red = finals
		return built, nil
	}
	stats, err := lastStats(stdin)
	if err != nil {
		return verdict{}, err
	}
	built := newVerdict(&expected, &shown, result, &stats)
	built.red = finals.withStats(&stats)
	return built, nil
}

func report(stderr io.Writer, err error, code int) int {
	if _, writeErr := fmt.Fprintf(stderr, "democheck: %v\n", err); writeErr != nil {
		return exitBadInput
	}
	return code
}
