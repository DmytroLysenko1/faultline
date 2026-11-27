#!/usr/bin/env bash
set -euo pipefail

output="${1:?usage: test-durations.sh <durations.txt> <sharded package>...}"
shift
cd "$(git rev-parse --show-toplevel)"
module="$(go list -m)"
sharded="$(printf '%s\n' "$@" | jq -R . | jq -sc --arg module "$module/" 'map(sub("^\\./"; $module))')"

go test -race -count=1 -json ./... |
	jq -r --arg module "$module/" --argjson sharded "$sharded" '
		select(.Action == "pass" or .Action == "fail")
		| select(.Test == null or ((.Test | contains("/") | not) and (.Package | IN($sharded[]))))
		| "\(.Package | ltrimstr($module)) \(.Test // "-") \(.Elapsed)"
	' |
	sort >"$output"
