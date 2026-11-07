package main

import (
	"context"
	"errors"
	"fmt"
	"net"

	"github.com/DmytroLysenko1/faultline/internal/interfaces/otlp"
)

const network = "tcp"

type listeners struct {
	OTLP net.Listener
	API  net.Listener
}

func listen(ctx context.Context, cfg *Config) (listeners, error) {
	var config net.ListenConfig
	ingestListener, err := config.Listen(ctx, network, cfg.OTLPAddr)
	if err != nil {
		return listeners{}, fmt.Errorf("listen otlp %s: %w", cfg.OTLPAddr, err)
	}
	apiListener, err := config.Listen(ctx, network, cfg.APIAddr)
	if err != nil {
		return listeners{}, errors.Join(fmt.Errorf("listen api %s: %w", cfg.APIAddr, err), ingestListener.Close())
	}

	return listeners{
		OTLP: otlp.LimitListener(ingestListener),
		API:  apiListener,
	}, nil
}
