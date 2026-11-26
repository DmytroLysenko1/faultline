package otlp

import (
	"errors"
	"net"

	"golang.org/x/net/netutil"
	"google.golang.org/grpc"
	_ "google.golang.org/grpc/encoding/gzip"
	"google.golang.org/grpc/keepalive"

	"github.com/DmytroLysenko1/faultline/internal/interfaces/otlp/decodelimit"
	"github.com/DmytroLysenko1/faultline/internal/interfaces/otlp/recovery"
)

var errNoTap = errors.New("otlp server needs a token tap")

func NewServer(acceptor spanAcceptor, gate *Tap, options Options) *grpc.Server {
	if gate == nil {
		panic(errNoTap)
	}
	shared := newEvents(options)
	server := grpc.NewServer(serverOptions(shared, gate, options)...)
	receiver := &traceReceiver{
		acceptor: acceptor,
		events:   shared,
	}
	server.RegisterService(traceServiceDesc(guardedExport{
		gate:     gate,
		receiver: receiver,
	}), receiver)
	return server
}

func LimitListener(listener net.Listener) net.Listener {
	return netutil.LimitListener(listener, MaxConnections)
}

func serverOptions(shared *events, gate *Tap, options Options) []grpc.ServerOption {
	return []grpc.ServerOption{
		grpc.InTapHandle(gate.handle),
		grpc.ForceServerCodecV2(decodeCodec(shared)),
		grpc.MaxRecvMsgSize(MaxRecvMsgBytes),
		grpc.MaxHeaderListSize(MaxHeaderListBytes),
		grpc.MaxConcurrentStreams(MaxConcurrentStreams),
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
			MinTime:             KeepaliveMinTime,
			PermitWithoutStream: true,
		}),
		grpc.KeepaliveParams(serverParameters(options.Keepalive)),
		grpc.ConnectionTimeout(ConnectionTimeout),
		grpc.ChainUnaryInterceptor(recovery.UnaryInterceptor(options.Logger)),
	}
}

func decodeCodec(shared *events) *decodelimit.Codec {
	weigher := decodelimit.NewWeigher(decodelimit.Limits{
		MaxDecodedBytes: MaxDecodedBytes,
		MaxNestingDepth: MaxNestingDepth,
	})
	return decodelimit.NewCodec(weigher, shared.decodeRejected)
}

func serverParameters(settings Keepalive) keepalive.ServerParameters {
	return keepalive.ServerParameters{
		Time:                  settings.Time,
		Timeout:               settings.Timeout,
		MaxConnectionIdle:     settings.MaxConnectionIdle,
		MaxConnectionAge:      settings.MaxConnectionAge,
		MaxConnectionAgeGrace: settings.MaxConnectionAgeGrace,
	}
}
