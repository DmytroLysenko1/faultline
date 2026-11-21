#!/usr/bin/env bash
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
