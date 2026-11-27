#!/usr/bin/env bash
set -euo pipefail
set -f

makefile="${1:?usage: ci-verify-check.sh <Makefile> <ci.yml>}"
workflow="${2:?usage: ci-verify-check.sh <Makefile> <ci.yml>}"

prerequisites="$(sed -n 's/^verify:[[:space:]]*\([^#]*\).*/\1/p' "$makefile")"
if [[ -z "${prerequisites// /}" ]]; then
	echo "ci-verify-check: no verify target in $makefile" >&2
	exit 1
fi

invoked="$(sed -n 's/^[[:space:]]*run:[[:space:]]*make[[:space:]]\{1,\}\(.*\)$/\1/p' "$workflow")"

status=0
for target in $prerequisites; do
	count=0
	for line in $invoked; do
		[[ "$line" == "$target" ]] && count=$((count + 1))
	done
	if ((count != 1)); then
		echo "ci-verify-check: verify prerequisite '$target' is run $count times in $workflow, want exactly 1" >&2
		status=1
	fi
done

groups="$(sed -n 's/^RACE_GROUPS[[:space:]]*:\{0,1\}=[[:space:]]*\([0-9]\{1,\}\).*/\1/p' "$makefile")"
if [[ -z "$groups" ]]; then
	echo "ci-verify-check: no RACE_GROUPS in $makefile" >&2
	exit 1
fi
want="$(awk -v groups="$groups" 'BEGIN { for (i = 1; i <= groups; i++) printf "%s%d", (i > 1 ? ", " : ""), i }')"
have="$(sed -n 's/^[[:space:]]*group:[[:space:]]*\[\(.*\)\][[:space:]]*$/\1/p' "$workflow")"
if [[ "$have" != "$want" ]]; then
	echo "ci-verify-check: the group matrix of $workflow is [$have], want [$want] (RACE_GROUPS=$groups in $makefile)" >&2
	status=1
fi
runs="$(grep -cF $'run: make verify-tests-group RACE_GROUP=${{ matrix.group }}' "$workflow" || true)"
if ((runs != 1)); then
	echo "ci-verify-check: $workflow must run 'make verify-tests-group RACE_GROUP=\${{ matrix.group }}' exactly once, found $runs" >&2
	status=1
fi
exit "$status"
