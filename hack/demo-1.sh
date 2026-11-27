#!/usr/bin/env bash
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
token_secret=ingest-token
token_id_annotation=ingest-token-id
token_tenant=default
token_switch_seconds=30
init_attempts=60
tokens_failure='faultline tokens: '
nats_not_reached='faultline tokens: connect to NATS: '
issued_at=0
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
	kubectl --context "$context" -n faultline rollout status deployment/nats --timeout=2m
	kubectl --context "$context" -n faultline rollout restart deployment/faultline
	init_tokens
	kubectl --context "$context" -n faultline rollout status deployment/faultline --timeout=3m
}

faultline_tokens() {
	kubectl --context "$context" -n faultline exec deploy/faultline -- /faultline tokens "$@"
}

init_failed_for_good() {
	grep -qF "$tokens_failure" "$1" && ! grep -qF "$nats_not_reached" "$1"
}

init_tokens() {
	local attempt
	log "creating the token bucket (faultline waits in waiting_tokens until it exists)"
	for attempt in $(seq "$init_attempts"); do
		if faultline_tokens init >"$work/tokens-init.log" 2>&1; then
			cat "$work/tokens-init.log" >&2
			return
		fi
		if init_failed_for_good "$work/tokens-init.log"; then
			cat "$work/tokens-init.log" >&2
			fail "faultline tokens init failed"
		fi
		sleep 2
	done
	cat "$work/tokens-init.log" >&2
	fail "faultline tokens init did not succeed in $attempt attempts"
}

stored_token_id() {
	kubectl --context "$context" -n "$OTEL_DEMO_NAMESPACE" get secret "$token_secret" --ignore-not-found \
		-o "jsonpath={.metadata.annotations.${token_id_annotation}}"
}

store_token() {
	local token="$1" id="$2"
	kubectl --context "$context" create namespace "$OTEL_DEMO_NAMESPACE" --dry-run=client -o yaml |
		kubectl --context "$context" apply -f - >/dev/null
	printf '%s' "$token" |
		kubectl --context "$context" -n "$OTEL_DEMO_NAMESPACE" create secret generic "$token_secret" \
			--from-file=token=/dev/stdin --dry-run=client -o yaml |
		kubectl --context "$context" annotate --local -f - "${token_id_annotation}=${id}" -o yaml |
		kubectl --context "$context" apply --server-side --field-manager=demo-1 --force-conflicts -f -
}

issue_token() {
	local token id
	log "issuing an ingest token into the Secret $OTEL_DEMO_NAMESPACE/$token_secret"
	token="$(faultline_tokens issue --tenant "$token_tenant" --stdout 2>"$work/tokens-issue.log")" || {
		cat "$work/tokens-issue.log" >&2
		fail "faultline tokens issue failed"
	}
	issued_at=$SECONDS
	cat "$work/tokens-issue.log" >&2
	id="$(sed -n 's/^issuing ingest token \([a-z2-7]*\) for tenant .*/\1/p' "$work/tokens-issue.log")"
	[[ -n "$id" ]] || fail "no token id in the output of faultline tokens issue"
	store_token "$token" "$id"
}

wait_token_accepted() {
	local left=$((token_switch_seconds - (SECONDS - issued_at)))
	if ((left > 0)); then
		log "waiting ${left} s until every ingester accepts the new token"
		sleep "$left"
	fi
}

restart_collector() {
	log "restarting the collector: it reads the token only at start"
	kubectl --context "$context" -n "$OTEL_DEMO_NAMESPACE" rollout restart daemonset/otel-collector-agent
}

revoke_stale_tokens() {
	local current listed id tenant
	current="$(stored_token_id)"
	[[ -n "$current" ]] || fail "the Secret $OTEL_DEMO_NAMESPACE/$token_secret holds no token id: no token is revoked"
	listed="$(faultline_tokens list)" || {
		log "cannot list the ingest tokens: the next run revokes the stale ones"
		return
	}
	while IFS=$'\t' read -r id tenant _; do
		if [[ "$tenant" == "$token_tenant" && "$id" != "$current" ]]; then
			log "revoking the stale ingest token $id of tenant $token_tenant"
			faultline_tokens revoke "$id" </dev/null >&2 || log "the token $id is not revoked: the next run tries again"
		fi
	done <<<"$listed"
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

wait_ready() {
	local attempt
	for attempt in $(seq 30); do
		if curl -fsS "$server/readyz" >/dev/null 2>&1; then
			return
		fi
		ensure_forward
		sleep 1
	done
	fail "faultline was not ready on /readyz through $server after $attempt s"
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

rotate_token() {
	issue_token
	wait_token_accepted
	install_demo deploy/dev/otel-demo-faultline-values.yaml
	restart_collector
	wait_demo_ready
	revoke_stale_tokens
}

main() {
	hack/check-versions.sh
	check_docker_memory "$min_docker_mem_mib"
	port_free || fail "local port $port is busy: set DEMO_PORT to a free port"
	build_tools
	ensure_cluster
	deploy_faultline
	rotate_token
	start_forward
	wait_ready
	poll_required
	wait_stats
	check_restarts
	final_check
	log "demo passed; graph in $graph"
}

if [[ "${BASH_SOURCE[0]}" == "$0" ]]; then
	main "$@"
fi
