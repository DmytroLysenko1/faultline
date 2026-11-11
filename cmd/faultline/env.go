package main

import (
	"errors"
	"fmt"
	"log/slog"
	"net"
	"strconv"
	"strings"
	"time"
	"unicode"
)

const (
	durationExample = "10s"
	minPort         = 1
	maxPort         = 65535
	signs           = "+-"
	addressFormat   = "host:port"
)

var (
	errEmpty        = errors.New("empty value, unset the variable to use the default")
	errInvalid      = errors.New("invalid")
	errOutOfRange   = errors.New("out of range")
	errBelow        = errors.New("must not be below")
	errSameAddress  = errors.New("must differ from")
	errAddress      = errors.New("invalid address")
	errUnknownLevel = errors.New("unknown log level")
	errEmptyHost    = errors.New("host is empty")
	errPort         = errors.New("port must be 1 … 65535")
	errWhitespace   = errors.New("contains whitespace")
)

type lookupFunc func(name string) (string, bool)

type envReader struct {
	lookup lookupFunc
	errs   []error
	failed map[string]bool
}

func newEnvReader(lookup lookupFunc) *envReader {
	return &envReader{
		lookup: lookup,
		failed: map[string]bool{},
	}
}

func (r *envReader) fail(name string, err error) {
	r.failed[name] = true
	r.errs = append(r.errs, fmt.Errorf("%s: %w", name, err))
}

func (r *envReader) value(name string) (string, bool) {
	raw, set := r.lookup(name)
	if set && raw == "" {
		r.fail(name, errEmpty)
		return "", false
	}
	return raw, set
}

func (r *envReader) size(name string, fallback, low, high int64) int64 {
	raw, set := r.value(name)
	if !set {
		return fallback
	}
	parsed, err := parseSize(raw, configUnits())
	if err != nil {
		r.fail(name, fmt.Errorf("%w size %q (%w), want e.g. %s", errInvalid, raw, err, formatSize(fallback)))
		return fallback
	}
	return within(r, name, parsed, fallback, low, high, formatSize)
}

func (r *envReader) count(name string, fallback, high int64) int {
	raw, set := r.value(name)
	if !set {
		return int(fallback)
	}
	parsed, err := parseUnsigned(raw)
	if err != nil {
		r.fail(name, fmt.Errorf("%w integer %q (%w), want e.g. %d", errInvalid, raw, err, fallback))
		return int(fallback)
	}
	return int(within(r, name, parsed, fallback, minCount, high, formatCount))
}

func (r *envReader) duration(name string, fallback, low, high time.Duration) time.Duration {
	raw, set := r.value(name)
	if !set {
		return fallback
	}
	parsed, err := time.ParseDuration(raw)
	if err != nil || strings.ContainsAny(raw[:1], signs) {
		r.fail(name, fmt.Errorf("%w duration %q, want e.g. %s", errInvalid, raw, durationExample))
		return fallback
	}
	return within(r, name, parsed, fallback, low, high, time.Duration.String)
}

func (r *envReader) address(name, fallback string) string {
	raw, set := r.value(name)
	if !set {
		return fallback
	}
	return r.validAddress(name, raw, fallback)
}

func (r *envReader) endpoint(name string) string {
	raw, set := r.lookup(name)
	if !set || raw == "" {
		return ""
	}
	return r.validAddress(name, raw, "")
}

func (r *envReader) validAddress(name, raw, fallback string) string {
	if err := validateAddress(raw); err != nil {
		r.fail(name, fmt.Errorf("%w %q (%w), want %s", errAddress, raw, err, addressFormat))
		return fallback
	}
	return raw
}

func (r *envReader) level(name string, fallback slog.Level) slog.Level {
	raw, set := r.value(name)
	if !set {
		return fallback
	}
	parsed, found := levels()[raw]
	if !found {
		r.fail(name, fmt.Errorf("%w %q, want debug, info, warn or error", errUnknownLevel, raw))
		return fallback
	}
	return parsed
}

func (r *envReader) notBelow(name string, value time.Duration, floorName string, floor time.Duration) {
	if r.failed[name] || r.failed[floorName] || value >= floor {
		return
	}
	r.fail(name, fmt.Errorf("%s %w %s (%s)", value, errBelow, floorName, floor))
}

func (r *envReader) atLeast(name string, value int64, need string, floor int64) {
	if r.failed[name] || value >= floor {
		return
	}
	r.fail(name, fmt.Errorf("%d %w %s (%d)", value, errBelow, need, floor))
}

func (r *envReader) distinct(name, value, otherName, other string) {
	if r.failed[name] || r.failed[otherName] || value != other {
		return
	}
	r.fail(name, fmt.Errorf("%q %w %s", value, errSameAddress, otherName))
}

func within[T ~int64](r *envReader, name string, value, fallback, low, high T, format func(T) string) T {
	if value < low || value > high {
		r.fail(name, fmt.Errorf("%s %w, want %s … %s", format(value), errOutOfRange, format(low), format(high)))
		return fallback
	}
	return value
}

func formatCount(value int64) string {
	return strconv.FormatInt(value, 10)
}

func levels() map[string]slog.Level {
	return map[string]slog.Level{
		"debug": slog.LevelDebug,
		"info":  slog.LevelInfo,
		"warn":  slog.LevelWarn,
		"error": slog.LevelError,
	}
}

func validateAddress(raw string) error {
	if strings.ContainsFunc(raw, unicode.IsSpace) {
		return errWhitespace
	}
	host, port, err := net.SplitHostPort(raw)
	if err != nil {
		return err
	}
	if host == "" {
		return errEmptyHost
	}
	return validatePort(port)
}

func validatePort(port string) error {
	number, err := parseUnsigned(port)
	if err != nil || number < minPort || number > maxPort {
		return errPort
	}
	return nil
}
