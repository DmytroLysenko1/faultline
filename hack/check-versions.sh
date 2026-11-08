#!/usr/bin/env bash
# Compares the installed demo tools with hack/versions.env (design step 1, slice 7, D5).
# A mismatch is a warning, not a failure: a developer's newer kubectl should not block the demo,
# but the difference stays visible next to any demo failure.
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck source=hack/versions.env
source "$repo_root/hack/versions.env"

warn() { printf 'check-versions: %s\n' "$*" >&2; }

compare() {
	local tool="$1" want="$2" have="$3"
	local exact="(^|[^0-9.])${want//./\\.}([^0-9.]|$)"
	if [[ ! "$have" =~ $exact ]]; then
		warn "$tool: have '${have:-not installed}', pinned $want"
	fi
}

compare kind "$KIND_VERSION" "$(kind version 2>/dev/null || true)"
compare kubectl "$KUBECTL_VERSION" "$(kubectl version --client 2>/dev/null | head -1 || true)"
compare helm "$HELM_VERSION" "$(helm version --short 2>/dev/null || true)"
