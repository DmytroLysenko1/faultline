package natsjs

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/url"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/samber/lo"

	"github.com/DmytroLysenko1/faultline/internal/infrastructure/natsjs/record"
)

var ErrInvalidConnOptions = errors.New("invalid nats connection options")

const (
	rolePublish = "publish"
	roleFeed    = "feed"
	roleControl = "control"
	roleProbe   = "probe"

	unclassifiedAsync = "unclassified"
	maxAsyncClasses   = 16
	asyncNetPrefix    = "net "
	asyncRepeatPeriod = time.Minute

	noReconnectBuffer = -1
	reconnectForever  = -1
)

var roles = [...]string{rolePublish, roleFeed, roleControl}

type ConnOptions struct {
	URLs           []string
	CredsFile      string
	TLSCAFile      string
	Name           string
	ConnectTimeout time.Duration
	DrainTimeout   time.Duration
	Logger         *slog.Logger
}

func (o *ConnOptions) validate() error {
	if o == nil {
		return fmt.Errorf("%w: none given", ErrInvalidConnOptions)
	}
	if err := o.checkTargets(); err != nil {
		return err
	}
	return o.checkTimeouts()
}

func (o *ConnOptions) checkTargets() error {
	if len(o.URLs) == 0 || o.Name == "" {
		return fmt.Errorf("%w: urls and a name are required", ErrInvalidConnOptions)
	}
	return nil
}

func (o *ConnOptions) checkTimeouts() error {
	if o.ConnectTimeout <= 0 || o.DrainTimeout <= 0 {
		return fmt.Errorf("%w: connect timeout %s and drain timeout %s must be positive", ErrInvalidConnOptions,
			o.ConnectTimeout, o.DrainTimeout)
	}
	return nil
}

type Link struct {
	NATS        *nats.Conn
	Closed      <-chan struct{}
	Disconnects atomic.Uint64
	Reconnects  atomic.Uint64
	AsyncErrors atomic.Uint64
}

type Conn struct {
	Publish *Link
	Feed    *Link
	Control *Link
}

func Dial(ctx context.Context, options *ConnOptions) (*Conn, error) {
	if err := options.validate(); err != nil {
		return nil, err
	}
	links := make([]*Link, 0, len(roles))
	for _, role := range roles {
		link, err := connect(ctx, options, role)
		if err != nil {
			closeAll(links)
			return nil, err
		}
		links = append(links, link)
	}
	conn := &Conn{
		Publish: links[0],
		Feed:    links[1],
		Control: links[2],
	}
	if err := payloadsFit(links); err != nil {
		closeAll(links)
		return nil, err
	}
	return conn, nil
}

type URLFailure struct {
	Host   string
	Reason DialReason
	Err    error
}

func DialEach(ctx context.Context, options *ConnOptions) ([]URLFailure, error) {
	if err := options.validate(); err != nil {
		return nil, err
	}
	probed := make([]URLFailure, 0, len(options.URLs))
	for _, raw := range options.URLs {
		result, err := probe(ctx, options, raw)
		if err != nil {
			return nil, err
		}
		probed = append(probed, result)
	}
	return lo.Filter(probed, failed), nil
}

func probe(ctx context.Context, options *ConnOptions, raw string) (URLFailure, error) {
	err := connectOnce(ctx, options, raw)
	if ctxErr := ctx.Err(); ctxErr != nil {
		return URLFailure{}, ctxErr
	}
	reason, _ := Unreachable(err)
	return URLFailure{
		Host:   hostOf(raw),
		Reason: reason,
		Err:    err,
	}, nil
}

func connectOnce(ctx context.Context, options *ConnOptions, raw string) error {
	settings := append(
		baseOptions(options, roleProbe, options.ConnectTimeout),
		nats.IgnoreDiscoveredServers(),
		nats.ErrorHandler(ignoreAsyncError),
	)
	nc, err := connectWith(ctx, raw, settings)
	if err != nil {
		return err
	}
	nc.Close()
	return nil
}

func (f URLFailure) Waitable() bool {
	return f.Reason != ""
}

func failed(result URLFailure, _ int) bool {
	return result.Err != nil
}

func hostOf(raw string) string {
	parsed, err := url.Parse(raw)
	if err != nil {
		return ""
	}
	return parsed.Host
}

func payloadsFit(links []*Link) error {
	for index, link := range links {
		if maxPayload := link.NATS.MaxPayload(); !payloadFits(maxPayload) {
			return fmt.Errorf("%w: %s max_payload %d, need %d", ErrPayloadLimit, roles[index], maxPayload, PayloadNeed())
		}
	}
	return nil
}

func (c *Conn) Connected() bool {
	return c.Publish.Connected() &&
		c.Feed.Connected() &&
		c.Control.Connected()
}

func (l *Link) Connected() bool {
	return l.NATS.IsConnected()
}

func (c *Conn) Close(ctx context.Context) error {
	if err := c.Publish.NATS.Drain(); err != nil {
		c.Publish.NATS.Close()
	}
	c.Feed.NATS.Close()
	c.Control.NATS.Close()
	for _, link := range []*Link{c.Publish, c.Feed, c.Control} {
		if err := awaitClosed(ctx, link); err != nil {
			c.Publish.NATS.Close()
			return err
		}
	}
	return nil
}

func awaitClosed(ctx context.Context, link *Link) error {
	select {
	case <-link.Closed:
		return nil
	default:
	}
	select {
	case <-link.Closed:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("close nats connections: %w", ctx.Err())
	}
}

func connect(ctx context.Context, options *ConnOptions, role string) (*Link, error) {
	timeout, err := serveTimeout(ctx, options)
	if err != nil {
		return nil, fmt.Errorf("connect %s: %w", role, err)
	}
	closed := make(chan struct{})
	link := &Link{
		Closed: closed,
	}
	settings := append(baseOptions(options, role, timeout), handlers(loggerOf(options), role, link, closed)...)
	nc, err := connectWith(ctx, strings.Join(options.URLs, ","), settings)
	if err != nil {
		return nil, fmt.Errorf("connect %s: %w", role, err)
	}
	link.NATS = nc
	return link, nil
}

type dialed struct {
	nc  *nats.Conn
	err error
}

func connectWith(ctx context.Context, urls string, settings []nats.Option) (*nats.Conn, error) {
	result := make(chan dialed, 1)
	go func() {
		nc, err := nats.Connect(urls, settings...)
		result <- dialed{
			nc:  nc,
			err: err,
		}
	}()
	select {
	case got := <-result:
		return got.nc, got.err
	case <-ctx.Done():
		go closeLate(result)
		return nil, ctx.Err()
	}
}

func closeLate(result <-chan dialed) {
	if late := <-result; late.nc != nil {
		late.nc.Close()
	}
}

func serveTimeout(ctx context.Context, options *ConnOptions) (time.Duration, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	deadline, bounded := ctx.Deadline()
	if !bounded {
		return options.ConnectTimeout, nil
	}
	share := time.Until(deadline) / time.Duration(len(options.URLs))
	if share <= 0 {
		return 0, context.DeadlineExceeded
	}
	return min(options.ConnectTimeout, share), nil
}

func baseOptions(options *ConnOptions, role string, timeout time.Duration) []nats.Option {
	settings := []nats.Option{
		nats.Name(options.Name + "-" + role),
		nats.ReconnectBufSize(noReconnectBuffer),
		nats.MaxReconnects(reconnectForever),
		nats.Timeout(timeout),
		nats.DrainTimeout(options.DrainTimeout),
	}
	if options.CredsFile != "" {
		settings = append(settings, nats.UserCredentials(options.CredsFile))
	}
	if options.TLSCAFile != "" {
		settings = append(settings, nats.RootCAs(options.TLSCAFile))
	}
	return settings
}

func handlers(logger *slog.Logger, role string, link *Link, closed chan<- struct{}) []nats.Option {
	return []nats.Option{
		nats.DisconnectErrHandler(func(_ *nats.Conn, err error) {
			if err == nil {
				return
			}
			link.Disconnects.Add(1)
			logger.Warn("nats.disconnected", "connection", role, "error", err)
		}),
		nats.ReconnectHandler(func(nc *nats.Conn) {
			link.Reconnects.Add(1)
			logger.Info("nats.reconnected", "connection", role, "url", nc.ConnectedUrlRedacted())
		}),
		nats.ErrorHandler(newAsyncLog(logger, role, link).handle),
		nats.ClosedHandler(func(*nats.Conn) {
			logger.Info("nats.closed", "connection", role)
			close(closed)
		}),
	}
}

type asyncClass struct {
	warnedAt   time.Time
	suppressed uint64
}

type asyncKey struct {
	name     string
	sentinel bool
}

type asyncLog struct {
	logger  *slog.Logger
	role    string
	link    *Link
	lock    sync.Mutex
	classes map[string]*asyncClass
	textual int
}

func newAsyncLog(logger *slog.Logger, role string, link *Link) *asyncLog {
	return &asyncLog{
		logger:  logger,
		role:    role,
		link:    link,
		classes: map[string]*asyncClass{},
	}
}

func (l *asyncLog) handle(_ *nats.Conn, _ *nats.Subscription, err error) {
	total := l.link.AsyncErrors.Add(1)
	suppressed, due := l.due(asyncClassOf(err))
	if !due {
		return
	}
	l.logger.Warn("nats.error", "connection", l.role, "error", err, "suppressed", suppressed, "total", total)
}

func (l *asyncLog) due(key asyncKey) (uint64, bool) {
	l.lock.Lock()
	defer l.lock.Unlock()
	class := l.classOf(key)
	now := time.Now()
	if !class.warnedAt.IsZero() && now.Sub(class.warnedAt) < asyncRepeatPeriod {
		class.suppressed++
		return 0, false
	}
	suppressed := class.suppressed
	class.warnedAt = now
	class.suppressed = 0
	return suppressed, true
}

func (l *asyncLog) classOf(key asyncKey) *asyncClass {
	if class := l.classes[key.name]; class != nil {
		return class
	}
	name := l.nameOf(key)
	class := l.classes[name]
	if class == nil {
		class = &asyncClass{}
		l.classes[name] = class
	}
	return class
}

func (l *asyncLog) nameOf(key asyncKey) string {
	if key.sentinel {
		return key.name
	}
	if l.textual >= maxAsyncClasses {
		return unclassifiedAsync
	}
	l.textual++
	return key.name
}

func asyncClassOf(err error) asyncKey {
	classes := asyncErrorClasses()
	index := slices.IndexFunc(classes, func(target error) bool {
		return errors.Is(err, target)
	})
	if index >= 0 {
		return asyncKey{
			name:     classes[index].Error(),
			sentinel: true,
		}
	}
	if failure, found := errors.AsType[*net.OpError](err); found {
		return asyncKey{
			name: asyncNetPrefix + failure.Op,
		}
	}
	return asyncKey{
		name: err.Error(),
	}
}

func asyncErrorClasses() []error {
	return []error{
		nats.ErrPermissionViolation,
		nats.ErrAuthorization,
		nats.ErrAuthExpired,
		nats.ErrAuthRevoked,
		nats.ErrAccountAuthExpired,
		nats.ErrSlowConsumer,
		nats.ErrMaxSubscriptionsExceeded,
	}
}

func ignoreAsyncError(*nats.Conn, *nats.Subscription, error) {}

func closeAll(links []*Link) {
	for _, link := range links {
		link.NATS.Close()
	}
}

func loggerOf(options *ConnOptions) *slog.Logger {
	if options.Logger == nil {
		return slog.New(slog.DiscardHandler)
	}
	return options.Logger
}

func payloadFits(maxPayload int64) bool {
	return maxPayload >= PayloadNeed()
}

func PayloadNeed() int64 {
	return record.MaxRecordBytes + HeaderBudget
}
