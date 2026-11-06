package timeout

import (
	"context"
	"time"

	"connectrpc.com/connect"
)

func Interceptor(budget time.Duration) connect.UnaryInterceptorFunc {
	return func(next connect.UnaryFunc) connect.UnaryFunc {
		return func(ctx context.Context, req connect.AnyRequest) (connect.AnyResponse, error) {
			bounded, cancel := context.WithTimeout(ctx, budget)
			defer cancel()
			return next(bounded, req)
		}
	}
}
