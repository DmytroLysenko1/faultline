#!/usr/bin/env bash
# Step 2 demo: the step 1 demo plus final RED windows and the step 2 accounting
# (design step 2, «Демо make demo-2»). Env: DEMO_PORT, as in hack/demo-1.sh.
set -euo pipefail

DEMO_STEP=2 exec "$(dirname "${BASH_SOURCE[0]}")/demo-1.sh" "$@"
