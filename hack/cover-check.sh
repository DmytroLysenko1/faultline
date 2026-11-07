#!/usr/bin/env bash
# Per-package line-coverage gate (NFR-MAINT-001).
# Usage: hack/cover-check.sh <coverage.out>
# Thresholds: STRICT_PACKAGES >= COVER_STRICT_MIN, every other package >= COVER_MIN.
# Excluded: cmd/*/main.go, generated code (internal/gen) and test-support packages (*test, like net/http/httptest).
set -euo pipefail

profile="${1:?usage: cover-check.sh <coverage.out>}"
module="$(go list -m)"

: "${COVER_MIN:=70}"
: "${COVER_STRICT_MIN:=80}"
# Parsers, detectors, window engine, RCA (NFR-MAINT-001) plus the step-1 pairing core.
: "${STRICT_PACKAGES:=internal/domain/topology pkg/stream internal/domain/anomaly internal/domain/logtemplate internal/domain/incident internal/infrastructure/ebpf}"

if [[ ! -s "$profile" ]]; then
	echo "cover-check: $profile not found or empty, run 'make test-race' first" >&2
	exit 1
fi

awk -v module="$module/" -v min="$COVER_MIN" -v strict_min="$COVER_STRICT_MIN" -v strict="$STRICT_PACKAGES" '
BEGIN {
	split(strict, list, " ")
	for (i in list) strictset[list[i]] = 1
}
NR == 1 { next }
{
	split($1, location, ":")
	file = location[1]
	block = $1
	statements = $2
	count = $3
	sub("^" module, "", file)
	if (file ~ /^cmd\/[^\/]+\/main\.go$/ || file ~ /^internal\/gen\//) next
	package = file
	sub(/\/[^\/]+$/, "", package)
	if (package ~ /test$/) next
	if (!(block in seen)) {
		seen[block] = 1
		total[package] += statements
		blockpackage[block] = package
		blockstatements[block] = statements
	}
	if (count > 0) covered_block[block] = 1
}
END {
	for (block in covered_block) covered[blockpackage[block]] += blockstatements[block]
	failed = 0
	for (package in total) {
		threshold = min
		for (prefix in strictset) {
			if (package == prefix || index(package, prefix "/") == 1) threshold = strict_min
		}
		percent = total[package] == 0 ? 100 : 100 * covered[package] / total[package]
		status = percent + 1e-9 >= threshold ? "ok  " : "FAIL"
		if (status == "FAIL") failed = 1
		printf "%s %-60s %6.1f%% (min %d%%)\n", status, package, percent, threshold
	}
	exit failed
}
' "$profile" | sort -k2
