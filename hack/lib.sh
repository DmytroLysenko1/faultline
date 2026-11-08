#!/usr/bin/env bash
# Shared helpers of the demo scripts (design step 1, slice 7, D1).
# Source after hack/versions.env; the caller sets `script` (the log prefix).
# Every kubectl and helm call names the demo cluster's context: the developer's current
# context may point at a real cluster.

cluster=faultline-demo
context="kind-${cluster}"
node="${cluster}-control-plane"

log() { printf '%s %s: %s\n' "$(date -u +%H:%M:%S)" "${script:-demo}" "$*" >&2; }

fail() {
	log "$*"
	exit 1
}

check_docker_memory() {
	local minimum_mib="$1" bytes mib
	bytes="$(docker info --format '{{.MemTotal}}')" || fail "docker is not running"
	mib=$((bytes / 1024 / 1024))
	log "Docker VM memory: ${mib} MiB"
	((mib >= minimum_mib)) || fail "need at least ${minimum_mib} MiB of Docker VM memory"
}

cluster_matches_profile() {
	local image nodes
	image="$(docker inspect "$node" --format '{{.Config.Image}}' 2>/dev/null)" || return 1
	nodes="$(kind get nodes --name "$cluster" | wc -l | tr -d ' ')"
	[[ "$image" == "$KIND_NODE_IMAGE" && "$nodes" == 1 ]]
}

ensure_cluster() {
	if kind get clusters | grep -qx "$cluster"; then
		if cluster_matches_profile; then
			log "reusing kind cluster $cluster"
			return
		fi
		log "kind cluster $cluster does not match deploy/kind/kind-config-demo.yaml, recreating it"
		kind delete cluster --name "$cluster"
	fi
	log "creating kind cluster $cluster (deploy/kind/kind-config-demo.yaml)"
	kind create cluster --name "$cluster" --config deploy/kind/kind-config-demo.yaml --wait 120s
}

install_demo() {
	local overlay="$1"
	log "installing OTel Demo chart $OTEL_DEMO_CHART_VERSION into $OTEL_DEMO_NAMESPACE with $overlay"
	helm repo add open-telemetry https://open-telemetry.github.io/opentelemetry-helm-charts >/dev/null 2>&1 || true
	helm repo update open-telemetry >/dev/null
	helm upgrade --install otel-demo open-telemetry/opentelemetry-demo \
		--kube-context "$context" \
		--version "$OTEL_DEMO_CHART_VERSION" \
		--namespace "$OTEL_DEMO_NAMESPACE" --create-namespace \
		-f deploy/dev/otel-demo-values.yaml \
		-f "$overlay"
}

wait_demo_ready() {
	log "waiting for OTel Demo in $OTEL_DEMO_NAMESPACE (up to 15 min: ~25 images to pull)"
	kubectl --context "$context" -n "$OTEL_DEMO_NAMESPACE" wait \
		--for=condition=available deployment --all --timeout=15m
	kubectl --context "$context" -n "$OTEL_DEMO_NAMESPACE" rollout status daemonset/otel-collector-agent --timeout=5m
}
