#!/usr/bin/env bash
# Step 1 demo: OTel Demo in kind sends traces to faultline, and the graph faultline builds must
# match the reference edges (design step 1, «Демо и путь деплоя», slice 7, D5).
# With DEMO_STEP=2 (hack/demo-2.sh) democheck also waits for final RED windows on the required
# edges and checks the step 2 accounting in pipeline.stats (design step 2, «Демо make demo-2»).
#
# Env: DEMO_PORT (local port of the API port-forward, default 18080); DEMO_STEP (1 or 2, default 1).
# Exit: 0 — the graph passed hack/democheck; 1 — it did not, or a step failed.
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"
# shellcheck source=hack/versions.env
source hack/versions.env
# shellcheck source=hack/lib.sh
source hack/lib.sh
step="${DEMO_STEP:-1}"
script="demo-${step}"
check_flags=()
case "$step" in
1) ;;
2) check_flags=(-red) ;;
*) fail "DEMO_STEP must be 1 or 2, got $step" ;;
esac

port="${DEMO_PORT:-18080}"
server="http://127.0.0.1:${port}"
min_docker_mem_mib=7680
poll_every=10
poll_rounds=30
stats_wait_seconds=35
work=tmp/demo
graph="$work/graph.json"
faultctl=tmp/bin/faultctl
democheck=tmp/bin/democheck
forward_pid=""

stop_forward() {
	if [[ -n "$forward_pid" ]]; then
		kill "$forward_pid" 2>/dev/null || true
		wait "$forward_pid" 2>/dev/null || true
		forward_pid=""
	fi
}
trap stop_forward EXIT

build_tools() {
	log "building faultctl and democheck"
	mkdir -p tmp/bin "$work"
	rm -f "$work/required.txt"
	go build -o "$faultctl" ./cmd/faultctl
	go build -o "$democheck" ./hack/democheck
}

deploy_faultline() {
	log "building image faultline:dev and loading it into $cluster"
	make --no-print-directory image kind-load
	kubectl --context "$context" apply -k deploy/dev/skeleton
	kubectl --context "$context" -n faultline rollout restart deployment/faultline
	kubectl --context "$context" -n faultline rollout status deployment/faultline --timeout=2m
}

port_free() {
	! (exec 3<>"/dev/tcp/127.0.0.1/${port}") 2>/dev/null
}

start_forward() {
	kubectl --context "$context" -n faultline port-forward svc/faultline "${port}:8080" >"$work/port-forward.log" 2>&1 &
	forward_pid=$!
}

ensure_forward() {
	if [[ -z "$forward_pid" ]] || ! kill -0 "$forward_pid" 2>/dev/null; then
		log "port-forward is not running, starting it on $port"
		start_forward
	fi
}

wait_healthy() {
	local attempt
	for attempt in $(seq 30); do
		if curl -fsS "$server/healthz" >/dev/null 2>&1; then
			return
		fi
		ensure_forward
		sleep 1
	done
	fail "faultline did not answer /healthz through $server after $attempt s"
}

export_graph() {
	local fresh="$graph.tmp"
	if "$faultctl" graph export -o json --server "$server" >"$fresh"; then
		mv "$fresh" "$graph"
		return 0
	fi
	rm -f "$fresh"
	return 1
}

poll_required() {
	local round code
	for round in $(seq "$poll_rounds"); do
		ensure_forward
		if export_graph; then
			code=0
			"$democheck" -graph "$graph" -required-only ${check_flags[@]+"${check_flags[@]}"} >"$work/required.txt" 2>&1 ||
				code=$?
			case "$code" in
			0) log "every required edge is in the graph${check_flags[*]:+ with final RED} (round $round)" && return 0 ;;
			1) ;;
			*) cat "$work/required.txt" >&2 && fail "democheck rejected its input" ;;
			esac
		fi
		sleep "$poll_every"
	done
	check_restarts
	cat "$work/required.txt" >&2 || true
	fail "required edges${check_flags[*]:+ or their final RED} did not appear in $((poll_rounds * poll_every)) s"
}

faultline_log() {
	kubectl --context "$context" -n faultline logs deployment/faultline
}

wait_stats() {
	local second
	for second in $(seq "$stats_wait_seconds"); do
		if grep -q '"msg":"pipeline.stats"' < <(faultline_log); then
			return
		fi
		sleep 1
	done
	fail "no pipeline.stats record in the faultline log after $second s"
}

check_restarts() {
	local restarts
	restarts="$(kubectl --context "$context" -n faultline get pods \
		-l app.kubernetes.io/name=faultline -o jsonpath='{.items[0].status.containerStatuses[0].restartCount}')" ||
		fail "cannot read the restart count of the faultline pod"
	[[ "$restarts" == 0 ]] || fail "faultline restarted ${restarts:-?} times: the graph lost what it had"
}

final_check() {
	ensure_forward
	export_graph || fail "faultctl could not export the final graph"
	faultline_log >"$work/faultline.log" || fail "cannot read the faultline log"
	if ! "$democheck" -graph "$graph" ${check_flags[@]+"${check_flags[@]}"} <"$work/faultline.log"; then
		faultline_log | tail -50 >&2
		fail "the demo graph did not pass the check"
	fi
}

main() {
	hack/check-versions.sh
	check_docker_memory "$min_docker_mem_mib"
	port_free || fail "local port $port is busy: set DEMO_PORT to a free port"
	build_tools
	ensure_cluster
	deploy_faultline
	install_demo deploy/dev/otel-demo-faultline-values.yaml
	wait_demo_ready
	start_forward
	wait_healthy
	poll_required
	wait_stats
	check_restarts
	final_check
	log "demo passed; graph in $graph"
}

main "$@"
