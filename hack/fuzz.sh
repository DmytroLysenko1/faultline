#!/usr/bin/env bash
# Runs every Fuzz* target of the module for FUZZTIME each (design step 2, «Команды»).
# go test -fuzz takes one target per run, so the script walks them one by one.
# The seed corpora already run as plain tests in make verify; this is the longer local search.
#
# Env: FUZZTIME (per target, default 60s).
# Exit: 0 — no target found a failure; 1 — a target failed (its input is saved under testdata/fuzz).
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

fuzztime="${FUZZTIME:-60s}"

while read -r file name; do
	package="./$(dirname "$file")"
	printf 'fuzz: %s %s for %s\n' "$package" "$name" "$fuzztime" >&2
	go test -run='^$' -fuzz="^${name}\$" -fuzztime="$fuzztime" "$package"
done < <(grep -rEo --include='*_test.go' '^func Fuzz[A-Za-z0-9_]+' . |
	sed -E 's|^\./||; s|:func | |' | sort)
