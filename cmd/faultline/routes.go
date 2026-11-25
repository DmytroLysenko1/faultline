package main

import (
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"

	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/operation"
	"github.com/DmytroLysenko1/faultline/internal/infrastructure/routerules"
)

var errNotRegular = errors.New("not a regular file")

type routeRules struct {
	Rules *operation.Rules
	Hash  uint32
}

func loadRules(path string) (routeRules, error) {
	if path == "" {
		rules, err := operation.NewRules(nil)
		return routeRules{
			Rules: rules,
		}, err
	}
	data, err := readAtMost(path, routerules.MaxFileBytes)
	if err != nil {
		return routeRules{}, fmt.Errorf("%s %s: %w", envRouteRulesFile, path, err)
	}
	rules, err := routerules.Parse(data)
	if err != nil {
		return routeRules{}, fmt.Errorf("%s %s: %w", envRouteRulesFile, path, err)
	}
	return routeRules{
		Rules: rules,
		Hash:  crc32.Checksum(data, crc32.MakeTable(crc32.Castagnoli)),
	}, nil
}

func readAtMost(path string, limit int64) ([]byte, error) {
	info, err := os.Stat(path)
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() {
		return nil, fmt.Errorf("%w (%s)", errNotRegular, info.Mode().Type())
	}
	file, err := os.Open(path) //nolint:gosec // G304: the operator names this file in ROUTE_RULES_FILE
	if err != nil {
		return nil, err
	}
	data, readErr := io.ReadAll(io.LimitReader(file, limit+1))
	if err := errors.Join(readErr, file.Close()); err != nil {
		return nil, fmt.Errorf("read: %w", err)
	}
	return data, nil
}
