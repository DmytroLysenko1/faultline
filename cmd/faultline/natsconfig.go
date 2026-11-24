package main

import (
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"os"
	"slices"
	"strings"
	"time"

	"github.com/samber/lo"

	"github.com/DmytroLysenko1/faultline/internal/infrastructure/natsjs"
	"github.com/DmytroLysenko1/faultline/internal/infrastructure/natsjs/record"
)

const (
	envNATSURL            = "NATS_URL"
	envNATSCredsFile      = "NATS_CREDS_FILE" //nolint:gosec // G101: the name of a variable that holds a file path
	envNATSTLSCAFile      = "NATS_TLS_CA_FILE"
	envNATSConnectTimeout = "NATS_CONNECT_TIMEOUT"
	envInstance           = "INSTANCE"
	envTenant             = "TENANT"
	envPartitions         = "PARTITIONS"
	envStreamReplicas     = "STREAM_REPLICAS"
	envStreamMaxBytes     = "STREAM_MAX_BYTES"
	envDuplicateWindow    = "STREAM_DUPLICATE_WINDOW"
	envStreamCompression  = "STREAM_COMPRESSION"
	envPublishTimeout     = "PUBLISH_TIMEOUT"

	defaultConnectTimeout  = 10 * time.Second
	minConnectTimeout      = time.Second
	maxConnectTimeout      = time.Minute
	natsDrainTimeout       = 300 * time.Millisecond
	defaultTenant          = "default"
	onlyPartitions         = 1
	defaultReplicas        = 1
	maxStreamReplicas      = 5
	defaultStreamMaxBytes  = 2 * gib
	minStreamMaxBytes      = gib
	maxStreamMaxBytes      = 256 * gib
	defaultDuplicateWindow = 5 * time.Minute
	maxDuplicateWindow     = 15 * time.Minute
	defaultPublishTimeout  = 2 * time.Second
	minPublishTimeout      = 200 * time.Millisecond
	maxPublishTimeout      = 3 * time.Second
	maxLabelBytes          = 63

	urlSeparator  = ","
	schemeSuffix  = "://"
	labelAlphabet = "abcdefghijklmnopqrstuvwxyz0123456789-"
	labelHyphen   = "-"
	hostDot       = "."
)

var (
	errRequired      = errors.New("required")
	errURLShape      = errors.New("must be nats://host:port or tls://host:port")
	errURLUserinfo   = errors.New("carries credentials, put them in " + envNATSCredsFile)
	errNotDNSLabel   = errors.New("is not a DNS label of at most 63 bytes")
	errDuplicateURL  = errors.New("repeats an earlier url")
	errUnknownChoice = errors.New("unknown value")
	errNoHostname    = errors.New("hostname unavailable, set " + envInstance)
)

type natsConfig struct {
	URLs           []string
	CredsFile      string
	TLSCAFile      string
	ConnectTimeout time.Duration
	Instance       string
}

func loadNATS(lookup lookupFunc, hostname func() (string, error)) (natsConfig, error) {
	reader := newEnvReader(lookup)
	cfg := natsConfig{
		URLs:           reader.natsURLs(envNATSURL),
		CredsFile:      reader.file(envNATSCredsFile),
		TLSCAFile:      reader.file(envNATSTLSCAFile),
		ConnectTimeout: reader.duration(envNATSConnectTimeout, defaultConnectTimeout, minConnectTimeout, maxConnectTimeout),
		Instance:       reader.instance(envInstance, hostname),
	}
	return cfg, errors.Join(reader.errs...)
}

func loadStreams(lookup lookupFunc) (natsjs.Topology, error) {
	reader := newEnvReader(lookup)
	topology := natsjs.Topology{
		Partitions:           reader.countWithin(envPartitions, onlyPartitions, onlyPartitions, onlyPartitions),
		Replicas:             reader.countWithin(envStreamReplicas, defaultReplicas, defaultReplicas, maxStreamReplicas),
		MaxBytesPerPartition: reader.size(envStreamMaxBytes, defaultStreamMaxBytes, minStreamMaxBytes, maxStreamMaxBytes),
		DuplicateWindow: reader.duration(
			envDuplicateWindow,
			defaultDuplicateWindow,
			defaultDuplicateWindow,
			maxDuplicateWindow,
		),
		Compression:   reader.compression(envStreamCompression),
		ConfigVersion: natsjs.StreamConfigVersion,
	}
	reader.valid(envStreamReplicas, topology.Validate())
	return topology, errors.Join(reader.errs...)
}

func loadPublish(lookup lookupFunc) (publishConfig, error) {
	reader := newEnvReader(lookup)
	cfg := publishConfig{
		Tenant:         reader.tenant(envTenant),
		PublishTimeout: reader.duration(envPublishTimeout, defaultPublishTimeout, minPublishTimeout, maxPublishTimeout),
	}
	return cfg, errors.Join(reader.errs...)
}

func (c *natsConfig) connOptions(logger *slog.Logger) *natsjs.ConnOptions {
	return &natsjs.ConnOptions{
		URLs:           c.URLs,
		CredsFile:      c.CredsFile,
		TLSCAFile:      c.TLSCAFile,
		Name:           c.Instance,
		ConnectTimeout: c.ConnectTimeout,
		DrainTimeout:   natsDrainTimeout,
		Logger:         logger,
	}
}

type publishConfig struct {
	Tenant         string
	PublishTimeout time.Duration
}

func (r *envReader) natsURLs(name string) []string {
	raw, set := r.value(name)
	if !set {
		r.require(name)
		return nil
	}
	urls := lo.Map(strings.Split(raw, urlSeparator), trimmed)
	r.checkNATSURLs(name, urls)
	return urls
}

func (r *envReader) checkNATSURLs(name string, urls []string) {
	for index, candidate := range urls {
		if err := validateNATSURL(candidate); err != nil {
			r.fail(name, fmt.Errorf("url %d %w", index+1, err))
		}
		if slices.Contains(urls[:index], candidate) {
			r.fail(name, fmt.Errorf("url %d %w", index+1, errDuplicateURL))
		}
	}
}

func (r *envReader) require(name string) {
	if r.failed[name] {
		return
	}
	r.fail(name, errRequired)
}

func (r *envReader) file(name string) string {
	path := r.path(name)
	if path == "" {
		return ""
	}
	if err := regularFile(path); err != nil {
		r.fail(name, fmt.Errorf("%s: %w", path, err))
		return ""
	}
	return path
}

func (r *envReader) instance(name string, hostname func() (string, error)) string {
	raw, set := r.value(name)
	if set {
		return r.label(name, raw)
	}
	if r.failed[name] {
		return ""
	}
	if hostname == nil {
		r.fail(name, errNoHostname)
		return ""
	}
	host, err := hostname()
	if err != nil {
		r.fail(name, fmt.Errorf("%w: %w", errNoHostname, err))
		return ""
	}
	first, _, _ := strings.Cut(host, hostDot)
	return r.label(name, strings.ToLower(first))
}

func (r *envReader) label(name, value string) string {
	if !isDNSLabel(value) {
		r.fail(name, fmt.Errorf("%q %w", value, errNotDNSLabel))
		return ""
	}
	return value
}

func (r *envReader) tenant(name string) string {
	raw, set := r.value(name)
	if !set {
		return defaultTenant
	}
	if err := record.ValidateTenant(raw); err != nil {
		r.fail(name, err)
		return defaultTenant
	}
	return raw
}

func (r *envReader) compression(name string) natsjs.Compression {
	raw, set := r.value(name)
	if !set {
		return natsjs.CompressionNone
	}
	chosen, known := compressions()[raw]
	if !known {
		r.fail(name, fmt.Errorf("%w %q, want none or s2", errUnknownChoice, raw))
		return natsjs.CompressionNone
	}
	return chosen
}

func compressions() map[string]natsjs.Compression {
	return map[string]natsjs.Compression{
		"none": natsjs.CompressionNone,
		"s2":   natsjs.CompressionS2,
	}
}

func isDNSLabel(value string) bool {
	return value != "" &&
		len(value) <= maxLabelBytes &&
		strings.Trim(value, labelAlphabet) == "" &&
		!strings.HasPrefix(value, labelHyphen) &&
		!strings.HasSuffix(value, labelHyphen)
}

func regularFile(path string) error {
	info, err := os.Stat(path)
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() {
		return fmt.Errorf("%w (%s)", errNotRegular, info.Mode().Type())
	}
	return nil
}

func validateNATSURL(raw string) error {
	parsed, err := url.Parse(raw)
	if err != nil {
		return errURLShape
	}
	if parsed.User != nil {
		return errURLUserinfo
	}
	return validateNATSShape(raw, parsed)
}

func trimmed(raw string, _ int) string {
	return strings.TrimSpace(raw)
}

func validateNATSShape(raw string, parsed *url.URL) error {
	if !slices.Contains(natsSchemes(), parsed.Scheme) || raw != parsed.Scheme+schemeSuffix+parsed.Host {
		return errURLShape
	}
	if err := validateAddress(parsed.Host); err != nil {
		return fmt.Errorf("%w (%w)", errURLShape, err)
	}
	return nil
}

func natsSchemes() []string {
	return []string{"nats", "tls"}
}
