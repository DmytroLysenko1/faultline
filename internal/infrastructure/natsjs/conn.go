package natsjs

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync/atomic"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/DmytroLysenko1/faultline/internal/infrastructure/natsjs/record"
)

var ErrInvalidConnOptions = errors.New("invalid nats connection options")

const (
	rolePublish = "publish"
	roleFeed    = "feed"
	roleControl = "control"

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
	if maxPayload := conn.Publish.NATS.MaxPayload(); !payloadFits(maxPayload) {
		closeAll(links)
		return nil, fmt.Errorf("%w: max_payload %d, need %d", ErrPayloadLimit, maxPayload, payloadNeed())
	}
	return conn, nil
}

func (c *Conn) Connected() bool {
	return c.Publish.NATS.IsConnected() &&
		c.Feed.NATS.IsConnected() &&
		c.Control.NATS.IsConnected()
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
	settings := append(
		baseOptions(options, role, timeout),
		handlers(loggerOf(options),
			role, link, closed)...,
	)
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
		nats.ClosedHandler(func(*nats.Conn) {
			logger.Info("nats.closed", "connection", role)
			close(closed)
		}),
	}
}
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
	return maxPayload >= payloadNeed()
}

func payloadNeed() int64 {
	return record.MaxRecordBytes + HeaderBudget
}
