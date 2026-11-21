#!/usr/bin/env bash
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
