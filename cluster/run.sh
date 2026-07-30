#!/usr/bin/env bash
# Cluster scene: the five CEP detectors as a compiled job plugin on a real
# coordinator + two-worker cluster, gated against the same manifest oracle
# as the embedded run (restricted to the five patterns the plugin ships -
# the broadcast watchlist and the queryable risk profile are embedded-mode
# scenes).
#
# Flow:
#   1. compose up (clink-runtime image - MUST be built from the same clink
#      commit the plugin will compile against; the scene compiles the
#      plugin inside that very image, so they match by construction);
#   2. build card_sentry_job.so in-image against the baked SDK;
#   3. submit with clink_submit_job (also in-image: the submitter dlopens
#      the Linux .so, which a macOS host cannot);
#   4. gate out-cluster/alerts-*.ndjson against the manifest.
#
#   CLINK_IMAGE=...   override the runtime image (default clink-runtime:latest)
#   KEEP_UP=1         leave the cluster running after the gate
#   PAR=n             job parallelism (default 1)

set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT}"
COMPOSE="docker compose -f cluster/docker-compose.yml"

[[ -f data/events.ndjson && -f data/rules.ndjson ]] || python3 tools/csgen.py --out data

echo "== cluster up, fresh (coordinator + 2 workers)"
mkdir -p out-cluster cluster/jobs
rm -f out-cluster/alerts-*.ndjson out-cluster/otp-healthy.ndjson cluster/jobs/card_sentry_job.so
${COMPOSE} down -v --remove-orphans >/dev/null 2>&1 || true
${COMPOSE} up -d --wait

echo "== building the job plugin inside the runtime image (SDK + matching ABI)"
${COMPOSE} exec -T coordinator bash -c "
    set -euo pipefail
    cmake -S /mnt/card-sentry/app -B /tmp/job-build > /dev/null
    cmake --build /tmp/job-build --target card_sentry_job --parallel 4 | tail -1
    cp /tmp/job-build/card_sentry_job.so /jobs/card_sentry_job.so
"
echo "   ABI fingerprint check happens at submit; a mismatch means the image"
echo "   and checkout are at different clink commits - rebuild the image."

echo "== submitting card-sentry (plugin .so ships to every worker)"
${COMPOSE} exec -T \
    -e CS_EVENTS=/data/events.ndjson -e CS_OUT_DIR=/out \
    coordinator clink_submit_job \
    --job=/jobs/card_sentry_job.so \
    --coordinator-host=127.0.0.1 --coordinator-port=6123 \
    --name=card-sentry --wait-timeout-s=180

echo "== gating cluster alerts against the manifest oracle"
cat out-cluster/alerts-*.ndjson > out-cluster/alerts.ndjson 2>/dev/null || true
python3 tools/check.py \
    --manifest data/manifest.json \
    --alerts out-cluster/alerts.ndjson \
    --only-patterns card_testing,impossible_travel,account_takeover,otp_never_verified,structuring

if [[ "${KEEP_UP:-0}" != "1" ]]; then
    ${COMPOSE} down -v --remove-orphans >/dev/null 2>&1 || true
else
    echo "KEEP_UP=1: cluster left running - dashboard at http://localhost:8081"
fi
echo "cluster scene: PASS"
