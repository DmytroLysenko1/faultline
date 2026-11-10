package routerules

import (
	"go.yaml.in/yaml/v3"

	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/operation"
)

const keyValuePairs = 2

func lineOf(data []byte, ruleErr *operation.RuleError) int {
	entry := serviceNode(data, ruleErr.Service)
	if entry == nil {
		return unknownLine
	}
	routes := valueOf(entry, "routes")
	if routes == nil || ruleErr.Route < 0 || ruleErr.Route >= len(routes.Content) {
		return entry.Line
	}
	return routes.Content[ruleErr.Route].Line
}

func versionLine(data []byte) int {
	if version := valueOf(rootMapping(data), "version"); version != nil {
		return version.Line
	}
	return unknownLine
}

func rootMapping(data []byte) *yaml.Node {
	var root yaml.Node
	if yaml.Unmarshal(data, &root) != nil || len(root.Content) == 0 {
		return &yaml.Node{}
	}
	return root.Content[0]
}

func serviceNode(data []byte, index int) *yaml.Node {
	services := valueOf(rootMapping(data), "services")
	if services == nil || index >= len(services.Content) {
		return nil
	}
	return services.Content[index]
}

func valueOf(mapping *yaml.Node, key string) *yaml.Node {
	for i := 0; i+1 < len(mapping.Content); i += keyValuePairs {
		if mapping.Content[i].Value == key {
			return mapping.Content[i+1]
		}
	}
	return nil
}
