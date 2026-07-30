#!/usr/bin/env bash
# Generate the tape (if missing), build the app against the installed clink
# prefix, run the five detectors over the tape, and gate the alerts against
# the manifest's oracle. Exit 0 only when every expected alert fired, every
# negative control stayed quiet, and nothing else alerted.
#
#   scripts/get-clink.sh          # once (CLINK_SOURCE=... for a local engine)
#   scripts/run-detections.sh
#
#   REGEN=1   force tape regeneration
#   SEED=n    generator seed (default 7; implies regeneration when changed)

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PREFIX="${ROOT}/.clink/prefix"
SEED="${SEED:-7}"

if [[ ! -x "${PREFIX}/bin/clink" ]]; then
    echo "clink prefix missing - run scripts/get-clink.sh first" >&2
    exit 2
fi

if [[ "${REGEN:-0}" == "1" || ! -f "${ROOT}/data/events.ndjson" \
      || ! -f "${ROOT}/data/rules.ndjson" ]]; then
    python3 "${ROOT}/tools/csgen.py" --seed "${SEED}" --out "${ROOT}/data"
fi

cmake -S "${ROOT}/app" -B "${ROOT}/app/build" \
    -DCMAKE_PREFIX_PATH="${PREFIX}" > /dev/null
cmake --build "${ROOT}/app/build" --parallel 10 | tail -1

"${ROOT}/app/build/patterns_test"

mkdir -p "${ROOT}/out"
"${ROOT}/app/build/card_sentry" \
    --in "${ROOT}/data/events.ndjson" \
    --out "${ROOT}/out/alerts.ndjson"

python3 "${ROOT}/tools/check.py" \
    --manifest "${ROOT}/data/manifest.json" \
    --alerts "${ROOT}/out/alerts.ndjson"
