package operation

import (
	"strings"

	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/resource"
)

type Normalizer struct {
	rules *Rules
}

func NewNormalizer(rules *Rules) *Normalizer {
	return &Normalizer{rules: rules}
}

func (n *Normalizer) Normalize(service resource.ServiceRef, facts *Facts) Operation {
	if facts.RPCMethod != "" {
		return rpcOperation(facts)
	}
	if facts.Route != "" {
		return n.httpOperation(service, facts.HTTPMethod, facts.Route)
	}
	if path, ok := facts.path(); ok {
		return n.httpOperation(service, facts.HTTPMethod, path)
	}
	return Operation{}
}

func rpcOperation(facts *Facts) Operation {
	var built name
	method := strings.TrimLeft(facts.RPCMethod, "/")
	if service := strings.TrimLeft(facts.RPCService, "/"); service != "" && !strings.Contains(method, "/") {
		built.writeString(service)
		built.writeBytes("/")
	}
	built.writeString(method)
	result := built.String()
	if result == OverflowName || result == "" {
		result = "/" + result
	}
	return Operation{Protocol: rpcProtocol(facts.RPCSystem), Name: result}
}

func (n *Normalizer) httpOperation(service resource.ServiceRef, method, path string) Operation {
	var built name
	built.writeBytes(string(canonicalMethod(method)))
	built.writeBytes(" ")
	split := splitPath(path)
	n.writePath(&built, service, split.items())
	return Operation{Protocol: ProtocolHTTP, Name: built.String()}
}

func (n *Normalizer) writePath(built *name, service resource.ServiceRef, items []string) {
	if template, ok := n.rules.match(service, items); ok {
		built.writeBytes(template)
		return
	}
	writeTemplate(built, items)
}

func writeTemplate(built *name, items []string) {
	if len(items) == 0 {
		built.writeBytes(rootPath)
		return
	}
	for _, segment := range items {
		if built.truncated {
			return
		}
		writeSegment(built, segment)
	}
}

func writeSegment(built *name, segment string) {
	built.writeBytes("/")
	if isIdentifier(segment) {
		built.writeBytes(identifier)
		return
	}
	built.writeString(segment)
}
