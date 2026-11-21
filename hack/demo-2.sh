#!/usr/bin/env bash
set -euo pipefail

DEMO_STEP=2 exec "$(dirname "${BASH_SOURCE[0]}")/demo-1.sh" "$@"
