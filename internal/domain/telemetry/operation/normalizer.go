package operation

import (
	"strings"

	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/resource"
)

type Normalizer struct {
	rules *Rules
}

func NewNormalizer(rules *Rules) *Normalizer {
	return &Normalizer{
		rules: rules,
	}
}

func (n *Normalizer) Normalize(service resource.ServiceRef, facts *Facts) Operation {
	return n.NormalizeShared(service, facts, nil)
}

func (n *Normalizer) NormalizeShared(service resource.ServiceRef, facts *Facts, names Names) Operation {
	var built name
	protocol := n.write(&built, service, facts)
	if protocol == ProtocolUnspecified {
		return Operation{}
	}
	return Operation{
		Protocol: protocol,
		Name:     names.share(&built),
	}
}

func (n *Normalizer) write(built *name, service resource.ServiceRef, facts *Facts) Protocol {
	if facts.RPCMethod != "" {
		writeRPC(built, facts)
		return rpcProtocol(facts.RPCSystem)
	}
	if facts.Route != "" {
		n.writeHTTP(built, service, facts.HTTPMethod, facts.Route)
		return ProtocolHTTP
	}
	if path, ok := facts.path(); ok {
		n.writeHTTP(built, service, facts.HTTPMethod, path)
		return ProtocolHTTP
	}
	return ProtocolUnspecified
}

func writeRPC(built *name, facts *Facts) {
	method := strings.TrimLeft(facts.RPCMethod, "/")
	service := strings.TrimLeft(facts.RPCService, "/")
	if service == "" || strings.Contains(method, "/") {
		writeBareMethod(built, method)
		return
	}
	built.writeString(service)
	built.writeBytes("/")
	built.writeString(method)
}

func writeBareMethod(built *name, method string) {
	if method == "" || method == OverflowName {
		built.writeBytes("/")
	}
	built.writeString(method)
}

func (n *Normalizer) writeHTTP(built *name, service resource.ServiceRef, method, path string) {
	built.writeBytes(string(canonicalMethod(method)))
	built.writeBytes(" ")
	split := splitPath(path)
	n.writePath(built, service, split.items())
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
