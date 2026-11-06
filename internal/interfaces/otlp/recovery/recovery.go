package recovery

import (
	"context"
	"fmt"
	"log/slog"
	"runtime/debug"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const internalMessage = "internal error"

func UnaryInterceptor(logger *slog.Logger) grpc.UnaryServerInterceptor {
	return func(
		ctx context.Context,
		request any,
		info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler,
	) (response any, err error) {
		defer func() {
			if recovered := recover(); recovered != nil {
				logPanic(ctx, logger, info.FullMethod, recovered)
				response, err = nil, status.Error(codes.Internal, internalMessage)
			}
		}()
		return handler(ctx, request)
	}
}

func logPanic(ctx context.Context, logger *slog.Logger, method string, recovered any) {
	logger.ErrorContext(ctx, "rpc.panic",
		"method", method,
		"panic", fmt.Sprint(recovered),
		"stack", string(debug.Stack()),
	)
}
