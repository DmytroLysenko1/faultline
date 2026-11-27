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
exit "$status"
