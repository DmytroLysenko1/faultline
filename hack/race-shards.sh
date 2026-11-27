#!/usr/bin/env bash
set -euo pipefail

profile="${1:?usage: race-shards.sh <coverage.out>}"
: "${RACE_SHARDS:=./cmd/faultline:4}"
: "${RACE_GROUP:=}"
: "${RACE_GROUPS:=1}"
: "${TEST_DURATIONS:=hack/test-durations.txt}"
: "${DEFAULT_SECONDS:=1}"

if [[ -n "$RACE_GROUP" ]]; then
	if ! [[ "$RACE_GROUP" =~ ^[0-9]+$ && "$RACE_GROUPS" =~ ^[0-9]+$ ]] || ((RACE_GROUP < 1 || RACE_GROUP > RACE_GROUPS)); then
		echo "race-shards: RACE_GROUP=$RACE_GROUP is not in 1..RACE_GROUPS=$RACE_GROUPS" >&2
		exit 1
	fi
fi

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
	local package="$1" output="$2"
	go test -race -list '.*' "$package" >"$output.raw"
	grep -E '^(Test|Fuzz|Example)' "$output.raw" >"$output" || true
	if [[ ! -s "$output" ]]; then
		echo "race-shards: $package lists no tests" >&2
		return 1
	fi
}

weigh() {
	awk -F'\t' -v fallback="$DEFAULT_SECONDS" -v durations="$TEST_DURATIONS" '
		BEGIN {
			while ((getline line < durations) > 0) {
				split(line, field, " ")
				seconds[field[1] " " field[2]] = field[3]
				if (field[2] == "-") wall[field[1]] = field[3]
				else tests[field[1]] += field[3]
			}
		}
		{
			key = $2 " " $3
			scale = 1
			if (($3 != "-") && ($2 in wall) && ($2 in tests) && wall[$2] > tests[$2]) scale = wall[$2] / tests[$2]
			print $1 "\t" ((key in seconds) ? seconds[key] * scale : fallback)
		}
	'
}

pack() {
	awk -F'\t' -v bins="$1" '
		{
			order[++count] = $1
			weight[$1] = $2 + 0
		}
		END {
			for (i = 1; i <= count; i++)
				for (j = i + 1; j <= count; j++)
					if (weight[order[j]] > weight[order[i]]) {
						swap = order[i]; order[i] = order[j]; order[j] = swap
					}
			for (b = 1; b <= bins; b++) load[b] = 0
			for (i = 1; i <= count; i++) {
				lightest = 1
				for (b = 2; b <= bins; b++) if (load[b] < load[lightest]) lightest = b
				load[lightest] += weight[order[i]]
				members[lightest] = members[lightest] (members[lightest] == "" ? "" : "|") order[i]
			}
			for (b = 1; b <= bins; b++) if (members[b] != "") print b "\t" load[b] "\t" members[b]
		}
	'
}

units="$work/units.tsv"
shards="$work/shards.tsv"
: >"$units"
: >"$shards"

for package in "${rest[@]}"; do
	printf 'rest:%s\t%s\t-\n' "${package#"$module/"}" "${package#"$module/"}"
done | weigh >>"$units"

listing_pids=()
for spec in $RACE_SHARDS; do
	listed "${spec%%:*}" "$work/$(basename "${spec%%:*}").list" &
	listing_pids+=("$!")
done
for pid in "${listing_pids[@]}"; do
	wait "$pid"
done

for spec in $RACE_SHARDS; do
	package="${spec%%:*}"
	count="${spec##*:}"
	listing="$work/$(basename "$package").list"
	total="$(wc -l <"$listing" | tr -d ' ')"
	assigned=0
	while IFS=$'\t' read -r index load members; do
		assigned=$((assigned + $(tr '|' '\n' <<<"$members" | wc -l)))
		printf 'shard:%s:%s\t%s\n' "${package#./}" "$index" "$load" >>"$units"
		printf 'shard:%s:%s\t%s\t%s\n' "${package#./}" "$index" "$package" "$members" >>"$shards"
	done < <(awk -v package="${package#./}" '{ print $1 "\t" package "\t" $1 }' "$listing" | weigh | pack "$count")
	if ((assigned != total)); then
		echo "race-shards: $package lists $total tests but the shards hold $assigned" >&2
		exit 1
	fi
done

selected="$work/selected.txt"
if [[ -z "$RACE_GROUP" ]]; then
	cut -f1 "$units" >"$selected"
else
	pack "$RACE_GROUPS" <"$units" >"$work/groups.tsv"
	if (($(wc -l <"$work/groups.tsv") != RACE_GROUPS)); then
		echo "race-shards: $(wc -l <"$work/groups.tsv" | tr -d ' ') of $RACE_GROUPS groups hold units, a group would run nothing" >&2
		exit 1
	fi
	grouped="$(cut -f3 "$work/groups.tsv" | tr '|' '\n' | wc -l | tr -d ' ')"
	if ((grouped != $(wc -l <"$units"))); then
		echo "race-shards: $(wc -l <"$units" | tr -d ' ') units but the groups hold $grouped" >&2
		exit 1
	fi
	while IFS=$'\t' read -r index load members; do
		echo "race-shards: group $index/$RACE_GROUPS ~${load}s"
		if [[ "$index" == "$RACE_GROUP" ]]; then
			tr '|' '\n' <<<"$members" >"$selected"
		fi
	done <"$work/groups.tsv"
fi

pids=()
names=()
start() {
	local name="$1"
	shift
	go test -race -covermode=atomic -coverprofile="$work/$name.out" "$@" >"$work/$name.log" 2>&1 &
	pids+=("$!")
	names+=("$name")
}

selected_rest=()
while IFS= read -r unit; do
	if [[ "$unit" == rest:* ]]; then
		selected_rest+=("$module/${unit#rest:}")
	fi
done <"$selected"
if ((${#selected_rest[@]} > 0)); then
	start rest "${selected_rest[@]}"
fi

while IFS=$'\t' read -r unit package members; do
	if grep -qxF "$unit" "$selected"; then
		echo "race-shards: $unit"
		start "$(basename "$package")-${unit##*:}" -run "^(${members})\$" "$package"
	fi
done <"$shards"

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
