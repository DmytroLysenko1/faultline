#!/usr/bin/env bash
set -euo pipefail

groups="${1:?usage: cover-merge.sh <groups> <directory> <merged.out>}"
directory="${2:?usage: cover-merge.sh <groups> <directory> <merged.out>}"
merged="${3:?usage: cover-merge.sh <groups> <directory> <merged.out>}"

status=0
for index in $(seq 1 "$groups"); do
	profile="$(find "$directory" -name "coverage-$index.out" | head -n 1)"
	if [[ -z "$profile" || "$(head -n 1 "$profile")" != "mode: atomic" || "$(wc -l <"$profile")" -lt 2 ]]; then
		echo "cover-merge: no profile with blocks for group $index of $groups under $directory" >&2
		status=1
	fi
done
if ((status != 0)); then
	exit "$status"
fi

echo "mode: atomic" >"$merged"
for index in $(seq 1 "$groups"); do
	tail -n +2 "$(find "$directory" -name "coverage-$index.out" | head -n 1)" >>"$merged"
done
