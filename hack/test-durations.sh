#!/usr/bin/env bash
set -euo pipefail

output="${1:?usage: test-durations.sh <durations.txt> <package>...}"
shift
cd "$(git rev-parse --show-toplevel)"
module="$(go list -m)"

go test -race -count=1 -json "$@" |
	jq -r --arg module "$module/" '
		select((.Action == "pass" or .Action == "fail") and .Test != null and (.Test | contains("/") | not))
		| "\(.Package | ltrimstr($module)) \(.Test) \(.Elapsed)"
	' |
	sort >"$output"
