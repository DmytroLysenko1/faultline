#!/usr/bin/env bash
set -euo pipefail

profile="${1:?usage: race-shards.sh <coverage.out>}"
: "${RACE_SHARDS:=./cmd/faultline:4}"
: "${TEST_DURATIONS:=hack/test-durations.txt}"
: "${DEFAULT_SECONDS:=1}"

cd "$(git rev-parse --show-toplevel)"
work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT

module="$(go list -m)"
sharded=()
for spec in $RACE_SHARDS; do
	sharded+=("${spec%%:*}")
done

rest=()
while IFS= read -r package; do
	skip=""
	for wanted in "${sharded[@]}"; do
		if [[ "$package" == "$module/${wanted#./}" ]]; then
			skip=yes
		fi
	done
	if [[ -z "$skip" ]]; then
		rest+=("$package")
	fi
done < <(go list ./...)

listed() {
	go test -race -list '.*' "$1" | grep -E '^(Test|Fuzz|Example)' || true
}

assign() {
	local package="$1" shards="$2"
	awk -v package="${package#./}" -v shards="$shards" -v fallback="$DEFAULT_SECONDS" -v durations="$TEST_DURATIONS" '
		BEGIN {
			while ((getline line < durations) > 0) {
				split(line, field, " ")
				if (field[1] == package) seconds[field[2]] = field[3]
			}
		}
		{
			name = $1
			cost = (name in seconds) ? seconds[name] : fallback
			order[++count] = name
			weight[name] = cost
		}
		END {
			for (i = 1; i <= count; i++)
				for (j = i + 1; j <= count; j++)
					if (weight[order[j]] > weight[order[i]]) {
						swap = order[i]; order[i] = order[j]; order[j] = swap
					}
			for (s = 1; s <= shards; s++) load[s] = 0
			for (i = 1; i <= count; i++) {
				lightest = 1
				for (s = 2; s <= shards; s++) if (load[s] < load[lightest]) lightest = s
				load[lightest] += weight[order[i]]
				members[lightest] = members[lightest] (members[lightest] == "" ? "" : "|") order[i]
			}
			for (s = 1; s <= shards; s++) if (members[s] != "") print s "\t" load[s] "\t" members[s]
		}
	'
}

pids=()
names=()
start() {
	local name="$1"
	shift
	go test -race -covermode=atomic -coverprofile="$work/$name.out" "$@" >"$work/$name.log" 2>&1 &
	pids+=("$!")
	names+=("$name")
}

if ((${#rest[@]} > 0)); then
	start rest "${rest[@]}"
fi
for spec in $RACE_SHARDS; do
	package="${spec%%:*}"
	shards="${spec##*:}"
	listing="$work/$(basename "$package").list"
	listed "$package" >"$listing"
	total="$(wc -l <"$listing" | tr -d ' ')"
	assigned=0
	while IFS=$'\t' read -r index load members; do
		assigned=$((assigned + $(tr '|' '\n' <<<"$members" | wc -l)))
		echo "race-shards: $package shard $index ~${load}s"
		start "$(basename "$package")-$index" -run "^(${members})\$" "$package"
	done < <(assign "$package" "$shards" <"$listing")
	if ((assigned != total)); then
		echo "race-shards: $package lists $total tests but the shards hold $assigned" >&2
		exit 1
	fi
done

failed=()
for index in "${!pids[@]}"; do
	if ! wait "${pids[$index]}"; then
		failed+=("${names[$index]}")
	fi
done
for name in "${names[@]}"; do
	cat "$work/$name.log"
done

echo "mode: atomic" >"$profile"
for name in "${names[@]}"; do
	if [[ -s "$work/$name.out" ]]; then
		tail -n +2 "$work/$name.out" >>"$profile"
	fi
done

if ((${#failed[@]} > 0)); then
	echo "race-shards: failed: ${failed[*]}" >&2
	exit 1
fi
