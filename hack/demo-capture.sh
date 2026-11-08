#!/usr/bin/env bash
# Step 1, slice 0: record real OTLP traffic of OTel Demo in kind.
#
# Output (raw, not versioned; the trimmed dump and the reference edges go to test/demo/otel-demo/):
#   $CAPTURE_DIR/traces.jsonl        one ptrace JSON line per collector trace export
#   $CAPTURE_DIR/servicegraph.jsonl  servicegraph connector metrics (the independent judge)
#   $CAPTURE_DIR/meta.env            chart version, namespace, capture window
#
# Env: CAPTURE_DURATION (seconds of traffic after the demo is ready, default 600),
#      CAPTURE_DIR (default tmp/capture).
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"
# shellcheck source=hack/versions.env
source hack/versions.env

# shellcheck source=hack/lib.sh
source hack/lib.sh
script=demo-capture

duration="${CAPTURE_DURATION:-600}"
capture_dir="${CAPTURE_DIR:-tmp/capture}"
min_docker_mem_mib=7168

reset_capture_files() {
	# A previous run may have left files on the node; the window must start after readiness.
	docker exec "$node" sh -c 'rm -f /var/lib/faultline-capture/*.jsonl'
	kubectl --context "$context" -n "$OTEL_DEMO_NAMESPACE" rollout restart daemonset/otel-collector-agent
	kubectl --context "$context" -n "$OTEL_DEMO_NAMESPACE" rollout status daemonset/otel-collector-agent --timeout=5m
}

record() {
	local started
	started="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
	log "recording ${duration}s of traffic"
	sleep "$duration"
	mkdir -p "$capture_dir"
	docker cp "$node:/var/lib/faultline-capture/." "$capture_dir/"
	cat >"$capture_dir/meta.env" <<EOF
OTEL_DEMO_CHART_VERSION=$OTEL_DEMO_CHART_VERSION
OTEL_DEMO_APP_VERSION=$OTEL_DEMO_APP_VERSION
OTEL_DEMO_NAMESPACE=$OTEL_DEMO_NAMESPACE
CAPTURE_STARTED=$started
CAPTURE_DURATION_SECONDS=$duration
EOF
	log "done: $(wc -l <"$capture_dir/traces.jsonl") trace exports in $capture_dir"
}

check_docker_memory "$min_docker_mem_mib"
ensure_cluster
install_demo deploy/dev/otel-demo-capture-values.yaml
wait_demo_ready
reset_capture_files
record
