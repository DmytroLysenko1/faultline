package graph

import (
	"cmp"
	"net/url"

	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/resource"
	"github.com/DmytroLysenko1/faultline/internal/domain/topology/call"
	"github.com/DmytroLysenko1/faultline/internal/domain/topology/handle"
)

const ServiceNodePrefix = "svc:"

type Node struct {
	ID                    string
	Service               resource.ServiceRef
	Handle                handle.Node
	UnansweredCalls       uint64
	UnansweredFailedCalls uint64
	Served                uint64
}

type Edge struct {
	Source      string
	Target      string
	Handle      handle.Edge
	Calls       uint64
	FailedCalls uint64
}

type Stats struct {
	GraphOverflow      uint64
	UnansweredOverflow uint64
	NodeOverflow       uint64
}

func NewNode(service resource.ServiceRef) Node {
	return Node{
		ID:      ServiceNodeID(service),
		Service: service,
	}
}

func (n *Node) RecordUnanswered(unanswered call.Unanswered) {
	n.UnansweredCalls++
	if unanswered.Failed {
		n.UnansweredFailedCalls++
	}
}

func (n *Node) RecordServed() {
	n.Served++
}

func (e *Edge) Record(matched *call.Call) {
	e.Calls++
	if matched.Failed {
		e.FailedCalls++
	}
}

func ServiceNodeID(service resource.ServiceRef) string {
	return ServiceNodePrefix + url.PathEscape(service.Namespace) + "/" + url.PathEscape(service.Name)
}

func CompareNodes(a, b *Node) int {
	return cmp.Compare(a.ID, b.ID)
}

func CompareEdges(a, b *Edge) int {
	return cmp.Or(cmp.Compare(a.Source, b.Source), cmp.Compare(a.Target, b.Target))
}
