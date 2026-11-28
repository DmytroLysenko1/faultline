package otlp

import (
	"errors"

	"google.golang.org/grpc"
	_ "google.golang.org/grpc/encoding/gzip"
	"google.golang.org/grpc/keepalive"

	"github.com/DmytroLysenko1/faultline/internal/interfaces/otlp/admission"
	"github.com/DmytroLysenko1/faultline/internal/interfaces/otlp/decodelimit"
	"github.com/DmytroLysenko1/faultline/internal/interfaces/otlp/recovery"
)

var (
	errNoTap          = errors.New("otlp server needs a token tap")
	errNoGate         = errors.New("otlp server needs an admission gate")
	errInvalidOptions = errors.New("invalid otlp server options")
)

type Guards struct {
	Tap  *Tap
	Gate *admission.Gate
}

func NewServer(acceptor spanAcceptor, guards Guards, options Options) *grpc.Server {
	if guards.Tap == nil {
		panic(errNoTap)
	}
	if guards.Gate == nil {
		panic(errNoGate)
	}
	if err := options.validate(); err != nil {
		panic(err)
	}
	shared := newEvents(options)
	server := grpc.NewServer(serverOptions(shared, guards.Tap, options)...)
	receiver := &traceReceiver{
		acceptor: acceptor,
		events:   shared,
	}
	server.RegisterService(traceServiceDesc(guardedExport{
		gate:      guards.Tap,
		admission: guards.Gate,
		receiver:  receiver,
		events:    shared,
		holdLimit: options.SlotHoldLimit,
	}), receiver)
	return server
}

func serverOptions(shared *events, gate *Tap, options Options) []grpc.ServerOption {
	return []grpc.ServerOption{
		grpc.InTapHandle(gate.handle),
		grpc.ForceServerCodecV2(decodeCodec(shared)),
		grpc.MaxRecvMsgSize(MaxRecvMsgBytes),
		grpc.MaxHeaderListSize(MaxHeaderListBytes),
		grpc.MaxConcurrentStreams(MaxConcurrentStreams),
		grpc.StaticStreamWindowSize(StreamWindowBytes),
		grpc.StaticConnWindowSize(ConnWindowBytes),
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
