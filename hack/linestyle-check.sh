#!/usr/bin/env bash
set -euo pipefail

base="${1:-HEAD}"
cd "$(git rev-parse --show-toplevel)"
if ! git rev-parse --verify --quiet "${base}^{commit}" >/dev/null; then
	if ! git rev-parse --verify --quiet "HEAD~1^{commit}" >/dev/null; then
		echo "linestyle: base $base is not a commit and HEAD has no parent" >&2
		exit 2
	fi
	echo "linestyle: base $base is not a commit, checking against HEAD~1" >&2
	base="HEAD~1"
fi

work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT

git diff --no-renames --name-only --diff-filter=AM "$base" -- '*.go' >"$work/changed.all"
git ls-files --others --exclude-standard -- '*.go' >"$work/added.all"
grep -v -e '^internal/gen/' -e '/testdata/' "$work/changed.all" >"$work/changed" || true
grep -v -e '^internal/gen/' -e '/testdata/' "$work/added.all" >"$work/added" || true

files=()
while IFS= read -r file; do
	files+=("$file")
done < <(cat "$work/changed" "$work/added")
if [[ ${#files[@]} -eq 0 ]]; then
	exit 0
fi

: >"$work/ranges"
if [[ -s "$work/changed" ]]; then
	changed=()
	while IFS= read -r file; do
		changed+=("$file")
	done <"$work/changed"
	git diff --no-renames -U0 "$base" -- "${changed[@]}" >"$work/diff"
	awk -v OFS='\t' '
		/^\+\+\+ b\// { file = substr($0, 7) }
		/^@@/ {
			split($3, hunk, ",")
			start = substr(hunk[1], 2)
			count = (hunk[2] == "") ? 1 : hunk[2]
			if (count > 0) print file, start, start + count - 1
		}' "$work/diff" >"$work/ranges"
fi
while IFS= read -r file; do
	printf '%s\t1\t999999999\n' "$file" >>"$work/ranges"
done <"$work/added"

status=0
go run ./hack/linestyle "${files[@]}" >"$work/report" 2>"$work/errors" || status=$?
if [[ $status -gt 1 ]] || { [[ $status -eq 1 ]] && [[ ! -s "$work/report" ]]; }; then
	cat "$work/errors" >&2
	exit 2
fi

awk -F'\t' 'NR == FNR { from[$1, NR] = $2; to[$1, NR] = $3; files[$1] = files[$1] " " NR; next }
	{
		split($0, parts, ":")
		path = parts[1]; line = parts[2] + 0
		count = split(files[path], ids, " ")
		for (i = 1; i <= count; i++) {
			if (line >= from[path, ids[i]] && line <= to[path, ids[i]]) { print; found = 1; break }
		}
	}
	END { exit found ? 1 : 0 }' "$work/ranges" "$work/report" || {
	echo "linestyle: fix the lines above — a call that fits in 128 goes on one line, a literal with named fields goes one field per line" >&2
	exit 1
}
