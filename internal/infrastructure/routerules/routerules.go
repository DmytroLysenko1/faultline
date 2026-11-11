package routerules

import (
	"bytes"
	"errors"
	"fmt"
	"io"

	"go.yaml.in/yaml/v3"

	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/operation"
	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/resource"
)

const (
	MaxFileBytes    = 1 << 20
	supportedFormat = 1
	unknownLine     = 0
)

type LineError struct {
	Line int
	Err  error
}

func (e *LineError) Error() string {
	if e.Line == unknownLine {
		return e.Err.Error()
	}
	return fmt.Sprintf("line %d: %v", e.Line, e.Err)
}

func (e *LineError) Unwrap() error {
	return e.Err
}

type document struct {
	Version  int       `yaml:"version"`
	Services []service `yaml:"services"`
}

type service struct {
	Namespace string   `yaml:"namespace"`
	Name      string   `yaml:"name"`
	Routes    []string `yaml:"routes"`
}

func Parse(data []byte) (*operation.Rules, error) {
	if len(data) > MaxFileBytes {
		return nil, fmt.Errorf("%w: file is %d bytes, at most %d", operation.ErrInvalidRules, len(data), MaxFileBytes)
	}
	parsed, err := decodeStrict(data)
	if err != nil {
		return nil, err
	}
	rules, err := operation.NewRules(servicesOf(parsed))
	if ruleErr, ok := errors.AsType[*operation.RuleError](err); ok {
		return nil, &LineError{
			Line: lineOf(data, ruleErr),
			Err:  ruleErr,
		}
	}
	return rules, err
}

func decodeStrict(data []byte) (document, error) {
	var parsed document
	decoder := yaml.NewDecoder(bytes.NewReader(data))
	decoder.KnownFields(true)
	err := decoder.Decode(&parsed)
	if errors.Is(err, io.EOF) {
		return document{}, fmt.Errorf("%w: the file is empty", operation.ErrInvalidRules)
	}
	if err != nil {
		return document{}, fmt.Errorf("%w: %w", operation.ErrInvalidRules, err)
	}
	if err := decoder.Decode(&document{}); !errors.Is(err, io.EOF) {
		return document{}, fmt.Errorf("%w: more than one YAML document or trailing content: %w",
			operation.ErrInvalidRules, err)
	}
	if parsed.Version != supportedFormat {
		return document{}, &LineError{
			Line: versionLine(data),
			Err: fmt.Errorf("%w: version %d, want %d",
				operation.ErrInvalidRules, parsed.Version, supportedFormat),
		}
	}
	return parsed, nil
}

func servicesOf(parsed document) []operation.ServiceRoutes {
	services := make([]operation.ServiceRoutes, 0, len(parsed.Services))
	for _, entry := range parsed.Services {
		services = append(services, operation.ServiceRoutes{
			Service: resource.ServiceRef{
				Namespace: entry.Namespace,
				Name:      entry.Name,
			},
			Routes: entry.Routes,
		})
	}
	return services
}
