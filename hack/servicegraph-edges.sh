#!/usr/bin/env bash
# Extract candidate reference edges from the servicegraph connector output (step 1, slice 0).
#
# Usage: hack/servicegraph-edges.sh <servicegraph.jsonl> <namespace>
# Prints one line per edge: "<source_id> <target_id> <requests> <failed>", sorted by id.
#
# Only plain service-to-service edges are kept, the same scope as faultline step 1:
# edges to virtual nodes, databases and messaging systems (connection_type set) are dropped.
# The connector reports cumulative counters, so the last data point of each edge is its total.
# Node ids follow the faultline contract: "svc:" + PathEscape(namespace) + "/" + PathEscape(name);
# the demo services have plain names, so no escaping is needed, and a name that would need it
# fails loudly instead of producing a wrong id.
set -euo pipefail

if (($# != 2)); then
	echo "usage: $0 <servicegraph.jsonl> <namespace>" >&2
	exit 2
fi
input="$1"
namespace="$2"

jq -r --arg ns "$namespace" '
	def attr($k): (map(select(.key == $k)) | first | .value.stringValue) // "";
	def node_id($name):
		if ($name | test("^[A-Za-z0-9._~-]+$")) then "svc:\($ns)/\($name)"
		else error("service name needs escaping: \($name)") end;

	.resourceMetrics[]?.scopeMetrics[]?.metrics[]?
	| select(.name == "traces_service_graph_request_total" or .name == "traces_service_graph_request_failed_total")
	| .name as $metric
	| .sum.dataPoints[]
	| (.attributes // []) as $a
	| select(($a | attr("connection_type")) == "")
	| {
		edge: "\(node_id($a | attr("client"))) \(node_id($a | attr("server")))",
		metric: $metric,
		time: (.timeUnixNano | tonumber),
		value: ((.asInt // .asDouble // 0) | tonumber)
	  }
' "$input" | jq -rs '
	group_by(.edge)
	| map({
		edge: .[0].edge,
		requests: ([.[] | select(.metric == "traces_service_graph_request_total")] | max_by(.time) | .value // 0),
		failed: ([.[] | select(.metric == "traces_service_graph_request_failed_total")] | max_by(.time) | .value // 0)
	  })
	| sort_by(.edge)[]
	| "\(.edge) \(.requests) \(.failed)"
'
