#!/usr/bin/env bash
# Incident replay scene: every alert is evidence.
#
# The flight recorder captures what each operator consumed, per checkpoint
# epoch. This scene runs the detectors on the cluster with capture armed,
# then - offline, engine stopped - re-executes a captured epoch
# byte-identically and freezes one operator's epoch into a self-contained
# regression bundle. For fraud detection, "why did the detector fire at
# 09:17" is a compliance question; replay makes the answer reproducible,
# and --emit-test turns the incident into a permanent regression test.
#
#   CLINK_IMAGE=...   override the runtime image
#   DAYS=n            tape length (default 3 - small; replay wants epochs, not scale)
#   KEEP_UP=1         leave the cluster running after the scene

set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT}"
COMPOSE="docker compose -f cluster/docker-compose.yml"
DAYS="${DAYS:-3}"

[[ -f data/events.ndjson && -f data/rules.ndjson ]] || python3 tools/csgen.py --days "${DAYS}" --out data

echo "== cluster up, fresh (coordinator + 2 workers)"
mkdir -p cluster/jobs cluster/ckpt cluster/capture out-cluster
rm -rf cluster/ckpt/* cluster/capture/* out-cluster/alerts-*.ndjson
${COMPOSE} down -v --remove-orphans >/dev/null 2>&1 || true
${COMPOSE} up -d --wait coordinator worker1 worker2

echo "== building the job plugin inside the runtime image"
${COMPOSE} exec -T coordinator bash -c "
    set -euo pipefail
    cmake -S /mnt/card-sentry/app -B /tmp/job-build > /dev/null
    cmake --build /tmp/job-build --target card_sentry_job --parallel 4 | tail -1
    cp /tmp/job-build/card_sentry_job.so /jobs/card_sentry_job.so
"

echo "== submitting with the flight recorder armed (capture + checkpoints)"
${COMPOSE} exec -T \
    -e CS_EVENTS=/data/events.ndjson -e CS_OUT_DIR=/out \
    coordinator clink_submit_job \
    --job=/jobs/card_sentry_job.so \
    --coordinator-host=127.0.0.1 --coordinator-port=6123 \
    --name=card-sentry-replay \
    --state-backend=file:/ckpt/state --checkpoint-interval-ms=500 \
    --capture-dir=/capture --capture-records=2000000 \
    --wait-timeout-s=180

echo "== 1. what the recorder captured (per operator, per epoch)"
${COMPOSE} exec -T coordinator bash -c "clink capture-cat --dir=/capture 2>/dev/null | head -20" \
    || ls -R cluster/capture | head -30

echo "== 2. replay epoch 1 for a captured detector, verifying byte-identically"
# Pick a detector (cs_event -> cs_alert) that has a captured epoch; the
# recorder writes a spec sidecar for every operator but an epoch only for
# the ones it captured input for. Replay needs the plugin for its factories.
OP_DIR="$(python3 - <<'PY2'
import glob, json, os
chosen = ""
for sidecar in sorted(glob.glob("cluster/capture/op-*/subtask-*/op.json")):
    d = os.path.dirname(sidecar)
    if not glob.glob(os.path.join(d, "epoch-1.cap")):
        continue
    spec = json.load(open(sidecar))
    if spec.get("in_channel") == "cs_event" and spec.get("out_channel") == "cs_alert":
        chosen = os.path.dirname(d)
        break
    chosen = chosen or os.path.dirname(d)
print(chosen)
PY2
)"
if [[ -z "${OP_DIR}" ]]; then
    echo "FAIL: no capture produced" >&2
    ${COMPOSE} down -v --remove-orphans >/dev/null 2>&1 || true
    exit 1
fi
OP_ID="${OP_DIR##*/op-}"
echo "   replaying $(cat "${OP_DIR}"/subtask-*/op.json | python3 -c 'import sys,json;d=json.load(sys.stdin);print(d["op_type"], d["in_channel"], "->", d["out_channel"])') (op ${OP_ID})"
${COMPOSE} exec -T coordinator clink replay \
    --capture-dir=/capture --checkpoint-dir=/ckpt/state \
    --epoch=1 --op="${OP_ID}" --verify \
    --plugin=/jobs/card_sentry_job.so
echo "   verify: OK (replay emissions are byte-identical to the live run)"

echo "== 3. freeze that operator's epoch into a regression bundle"
# The bundle is written inside the container and copied out: emitting it
# straight onto the bind mount fails on Docker Desktop's file sharing,
# which refuses the permission copy std::filesystem::copy_file makes.
${COMPOSE} exec -T coordinator rm -rf /tmp/cs-bundle
${COMPOSE} exec -T coordinator clink replay \
    --capture-dir=/capture --checkpoint-dir=/ckpt/state \
    --epoch=1 --op="${OP_ID}" --emit-test=/tmp/cs-bundle \
    --plugin=/jobs/card_sentry_job.so
${COMPOSE} exec -T coordinator ls -1 /tmp/cs-bundle
rm -rf cluster/capture/bundle
${COMPOSE} cp coordinator:/tmp/cs-bundle cluster/capture/bundle >/dev/null 2>&1 \
    && echo "   bundle copied to cluster/capture/bundle" || true

if [[ "${KEEP_UP:-0}" != "1" ]]; then
    ${COMPOSE} down -v --remove-orphans >/dev/null 2>&1 || true
fi
echo "replay scene: the incident replays offline, deterministically, and is frozen as a test."
