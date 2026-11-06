package recovery

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"runtime/debug"

	"connectrpc.com/connect"
)

var ErrPanic = errors.New("internal error")

func Handler(logger *slog.Logger) func(context.Context, connect.Spec, http.Header, any) error {
	return func(ctx context.Context, spec connect.Spec, _ http.Header, recovered any) error {
		logger.ErrorContext(ctx, "api.panic",
			slog.String("procedure", spec.Procedure),
			slog.String("panic", fmt.Sprint(recovered)),
			slog.String("stack", string(debug.Stack())),
		)
		return connect.NewError(connect.CodeInternal, ErrPanic)
	}
}
