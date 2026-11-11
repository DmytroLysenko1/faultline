package main

import (
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/operation"
	"github.com/DmytroLysenko1/faultline/internal/infrastructure/routerules"
)

var errNotRegular = errors.New("not a regular file")

func loadRouteRules(path string) (*operation.Rules, error) {
	if path == "" {
		return operation.NewRules(nil)
	}
	data, err := readAtMost(path, routerules.MaxFileBytes)
	if err != nil {
		return nil, fmt.Errorf("%s %s: %w", envRouteRulesFile, path, err)
	}
	rules, err := routerules.Parse(data)
	if err != nil {
		return nil, fmt.Errorf("%s %s: %w", envRouteRulesFile, path, err)
	}
	return rules, nil
}

func readAtMost(path string, limit int64) ([]byte, error) {
	info, err := os.Stat(path)
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() {
		return nil, fmt.Errorf("%w (%s)", errNotRegular, info.Mode().Type())
	}
	file, err := os.Open(path) //nolint:gosec // G304: the operator names this file in FAULTLINE_ROUTE_RULES_FILE
	if err != nil {
		return nil, err
	}
	data, readErr := io.ReadAll(io.LimitReader(file, limit+1))
	if err := errors.Join(readErr, file.Close()); err != nil {
		return nil, fmt.Errorf("read: %w", err)
	}
	return data, nil
}
