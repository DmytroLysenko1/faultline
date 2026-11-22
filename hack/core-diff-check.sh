#!/usr/bin/env bash
set -euo pipefail

base="${1:-v0.2.0}"
allow="${CORE_DIFF_ALLOW:-hack/core-diff.allow}"

if ! git rev-parse --verify --quiet "$base^{commit}" >/dev/null; then
	echo "core-diff-check: base $base is not a commit here; fetch the tags (fetch-depth: 0)" >&2
	exit 1
fi
if awk 'NF == 1 { found = 1 } END { exit !found }' "$allow"; then
	echo "core-diff-check: every line of $allow needs a path and a class" >&2
	exit 1
fi
touched="$(git diff --no-renames --name-only --diff-filter=AMD "$base" HEAD -- internal/domain internal/application pkg)"
changed="$(awk -F/ '
	/_test\.go$/ || /\/testdata\// { next }
	{ for (i = 2; i < NF; i++) if ($i == $(i - 1) "test") next; print }
' <<<"$touched")"
listed="$(awk 'NF { print $1 }' "$allow" | sort -u)"
unlisted="$(comm -23 <(printf '%s\n' "$changed" | sed '/^$/d' | sort -u) <(printf '%s\n' "$listed"))"

if [ -n "$unlisted" ]; then
	echo "core-diff-check: core files changed since $base with no line in $allow:" >&2
	while IFS= read -r file; do
		echo "  $file" >&2
	done <<<"$unlisted"
	exit 1
fi
echo "core-diff-check: every core change since $base is listed in $allow"
