#!/usr/bin/env bash
set -euo pipefail

output="${1:?usage: test-durations.sh <durations.txt> <sharded package>...}"
shift
cd "$(git rev-parse --show-toplevel)"
module="$(go list -m)"
sharded="$(printf '%s\n' "$@" | jq -R . | jq -sc --arg module "$module/" 'map(sub("^\\./"; $module))')"

go test -race -count=1 -json ./... |
	jq -sr --arg module "$module/" --argjson sharded "$sharded" '
		map(select((.Action == "pass" or .Action == "fail") and .Elapsed != null))
		| (map(select(.Test == null) | "\(.Package | ltrimstr($module)) - \(.Elapsed)"))
		+ (map(select(.Test != null and (.Package | IN($sharded[]))))
			| group_by([.Package, (.Test | split("/")[0])])
			| map("\(.[0].Package | ltrimstr($module)) \(.[0].Test | split("/")[0]) \(map(.Elapsed) | max)"))
		| .[]
	' |
	sort >"$output"
