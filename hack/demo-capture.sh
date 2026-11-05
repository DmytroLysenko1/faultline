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

cluster=faultline-demo
context="kind-${cluster}"
node="${cluster}-control-plane"
duration="${CAPTURE_DURATION:-600}"
capture_dir="${CAPTURE_DIR:-tmp/capture}"
min_docker_mem_gib=7

log() { printf '%s demo-capture: %s\n' "$(date -u +%H:%M:%S)" "$*" >&2; }

check_docker_memory() {
	local bytes gib
	bytes="$(docker info --format '{{.MemTotal}}')"
	gib=$((bytes / 1024 / 1024 / 1024))
	log "Docker VM memory: ${gib} GiB"
	if ((gib < min_docker_mem_gib)); then
		log "need at least ${min_docker_mem_gib} GiB for OTel Demo"
		exit 1
	fi
}

ensure_cluster() {
	if kind get clusters | grep -qx "$cluster"; then
		log "reusing kind cluster $cluster"
		return
	fi
	log "creating kind cluster $cluster (deploy/kind/kind-config-demo.yaml)"
	kind create cluster --name "$cluster" --config deploy/kind/kind-config-demo.yaml --wait 120s
}

install_demo() {
	log "installing OTel Demo chart $OTEL_DEMO_CHART_VERSION into namespace $OTEL_DEMO_NAMESPACE"
	helm repo add open-telemetry https://open-telemetry.github.io/opentelemetry-helm-charts >/dev/null 2>&1 || true
	helm repo update open-telemetry >/dev/null
	helm upgrade --install otel-demo open-telemetry/opentelemetry-demo \
		--kube-context "$context" \
		--version "$OTEL_DEMO_CHART_VERSION" \
		--namespace "$OTEL_DEMO_NAMESPACE" --create-namespace \
		-f deploy/dev/otel-demo-values.yaml \
		-f deploy/dev/otel-demo-capture-values.yaml
}

wait_ready() {
	log "waiting for all Deployments in $OTEL_DEMO_NAMESPACE (up to 15 min: ~25 images to pull)"
	kubectl --context "$context" -n "$OTEL_DEMO_NAMESPACE" wait \
		--for=condition=available deployment --all --timeout=15m
	kubectl --context "$context" -n "$OTEL_DEMO_NAMESPACE" rollout status daemonset/otel-collector-agent --timeout=5m
}

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

check_docker_memory
ensure_cluster
install_demo
wait_ready
reset_capture_files
record
