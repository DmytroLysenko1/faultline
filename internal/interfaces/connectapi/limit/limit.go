package limit

import (
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"connectrpc.com/connect"
	"golang.org/x/sync/semaphore"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/protobuf/types/known/durationpb"
)

const (
	RetryDelay      = time.Second
	slotsPerRequest = 1
)

var ErrTooManyRequests = errors.New("too many concurrent requests")

type Handler struct {
	Next     http.Handler
	Slots    *semaphore.Weighted
	Deadline time.Duration
	Errors   *connect.ErrorWriter
	Retry    *connect.ErrorDetail
	Logger   *slog.Logger
}

func New(next http.Handler, maxConcurrent int64, deadline time.Duration, logger *slog.Logger) (*Handler, error) {
	retry, err := connect.NewErrorDetail(&errdetails.RetryInfo{
		RetryDelay: durationpb.New(RetryDelay),
	})
	if err != nil {
		return nil, fmt.Errorf("build retry info: %w", err)
	}
	return &Handler{
		Next:     next,
		Slots:    semaphore.NewWeighted(maxConcurrent),
		Deadline: deadline,
		Errors:   connect.NewErrorWriter(),
		Retry:    retry,
		Logger:   logger,
	}, nil
}

func (h *Handler) ServeHTTP(response http.ResponseWriter, request *http.Request) {
	if !h.Slots.TryAcquire(slotsPerRequest) {
		h.reject(response, request)
		return
	}
	defer h.Slots.Release(slotsPerRequest)
	h.boundIO(response, request)
	h.Next.ServeHTTP(response, request)
}

func (h *Handler) boundIO(response http.ResponseWriter, request *http.Request) {
	deadline := time.Now().Add(h.Deadline)
	controller := http.NewResponseController(response)
	err := errors.Join(controller.SetReadDeadline(deadline), controller.SetWriteDeadline(deadline))
	if err != nil {
		h.Logger.WarnContext(request.Context(), "api.io_deadline_unsupported",
			slog.String("procedure", request.URL.Path),
			slog.String("error", err.Error()),
		)
	}
}

func (h *Handler) reject(response http.ResponseWriter, request *http.Request) {
	h.Logger.WarnContext(request.Context(), "api.busy", slog.String("procedure", request.URL.Path))
	if err := h.Errors.Write(response, request, h.tooManyRequests()); err != nil {
		h.Logger.WarnContext(request.Context(), "api.reject_write_failed",
			slog.String("procedure", request.URL.Path),
			slog.String("error", err.Error()),
		)
	}
}

func (h *Handler) tooManyRequests() error {
	failure := connect.NewError(connect.CodeResourceExhausted, ErrTooManyRequests)
	failure.AddDetail(h.Retry)
	return failure
}
