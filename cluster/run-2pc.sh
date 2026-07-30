#!/usr/bin/env bash
# 2PC kill-a-worker scene: kill the detector mid-run, the case table
# doesn't lie.
#
# The five detectors run on the cluster with periodic checkpoints, and
# every alert lands in Postgres through clink's exactly-once two-phase
# commit sink (PREPARE TRANSACTION at the barrier, COMMIT PREPARED when
# the checkpoint is globally durable, orphaned prepared transactions
# rolled back on recovery). Mid-run, a worker is killed -9. Compose
# restarts the process, the coordinator redeploys from the last
# checkpoint, sources rewind to their snapshotted offsets, and the job
# completes. The gate: the case table holds EXACTLY the manifest's
# alerts - no duplicates from the replay, no gaps from the crash.
#
# The file sinks are at-least-once by design and WILL carry duplicates
# after a restore; the exactly-once contract - and the gate - is the
# Postgres table.
#
# A larger tape (more days of noise; the campaigns and therefore the
# expected alerts are unchanged) keeps the job running long enough to
# kill a worker mid-flight, but not so large that the post-restart
# re-drain is slow. The workers are configured with generous slots
# (docker-compose.yml) so ONE survivor can host the whole job after a
# kill - the coordinator re-places every subtask on the survivor
# immediately, without waiting for the killed worker to return. That is
# the "survive a worker loss" demo; a job with no spare capacity would
# instead wedge waiting for the dead worker.
#
#   CLINK_IMAGE=...   override the runtime image
#   DAYS=n            tape length (default 90, ~0.8M events)
#   KILL_AFTER=s      seconds to wait before the kill (default 6)
#   KEEP_UP=1         leave the cluster running after the gate

set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT}"
COMPOSE="docker compose -f cluster/docker-compose.yml"
DAYS="${DAYS:-90}"
KILL_AFTER="${KILL_AFTER:-6}"
DSN="host=postgres port=5432 dbname=sentry user=sentry password=sentry"

echo "== generating the ${DAYS}-day tape (same campaigns, more noise)"
if [[ ! -f data/events-2pc.ndjson ]]; then
    python3 tools/csgen.py --days "${DAYS}" --out data-2pc
    cp data-2pc/events.ndjson data/events-2pc.ndjson
    cp data-2pc/rules.ndjson data/rules-2pc.ndjson
    cp data-2pc/manifest.json data/manifest-2pc.json
fi

echo "== cluster up, fresh (coordinator + 2 workers + postgres)"
mkdir -p out-cluster cluster/jobs cluster/ckpt
rm -rf cluster/ckpt/* out-cluster/alerts-*.ndjson out-cluster/otp-healthy.ndjson
${COMPOSE} down -v --remove-orphans >/dev/null 2>&1 || true
${COMPOSE} up -d --wait

echo "== creating the case table"
${COMPOSE} exec -T postgres psql -U sentry -d sentry -q -c \
    "DROP TABLE IF EXISTS alerts;
     CREATE TABLE alerts (pattern text, entity_kind text, entity_id bigint,
                          ts bigint, detail text);"

echo "== building the job plugin inside the runtime image"
${COMPOSE} exec -T coordinator bash -c "
    set -euo pipefail
    cmake -S /mnt/card-sentry/app -B /tmp/job-build > /dev/null
    cmake --build /tmp/job-build --target card_sentry_job --parallel 4 | tail -1
    cp /tmp/job-build/card_sentry_job.so /jobs/card_sentry_job.so
"

echo "== submitting with checkpoints every 1000 ms${NO_PG:+ (NO_PG: 2PC sink OFF)} ${NO_PG:-+ the 2PC case sink}"
# NO_PG=1 drops the 2PC sink (CS_PG_DSN empty) to isolate the EOS final
# checkpoint from the committing sink.
PG_ENV=(-e "CS_PG_DSN=${DSN}" -e CS_PG_TABLE=alerts)
if [[ "${NO_PG:-0}" == "1" ]]; then PG_ENV=(-e CS_PG_DSN=); fi
${COMPOSE} exec -T \
    -e CS_EVENTS=/data/events-2pc.ndjson -e CS_OUT_DIR=/out \
    "${PG_ENV[@]}" \
    coordinator clink_submit_job \
    --job=/jobs/card_sentry_job.so \
    --coordinator-host=127.0.0.1 --coordinator-port=6123 \
    --name=card-sentry-2pc \
    --state-backend=file:/ckpt/state --checkpoint-interval-ms=1000 \
    --max-restarts-on-worker-loss=3 \
    --wait-timeout-s=600 > /tmp/cs-2pc-submit.log 2>&1 &
SUBMIT_PID=$!

if [[ "${NO_KILL:-0}" == "1" ]]; then
    echo "== NO_KILL=1: baseline run, no worker kill (proves 2PC commit on a clean run)"
else
    sleep "${KILL_AFTER}"
    STATE="$(curl -fsS localhost:8081/api/v1/jobs 2>/dev/null || echo '{}')"
    if echo "${STATE}" | grep -q '"completion_signalled":true'; then
        echo "FAIL: the job completed before the kill - raise DAYS" >&2
        exit 1
    fi
    echo "== killing worker2 mid-run (job state: ${STATE})"
    docker kill -s KILL cluster-worker2-1 >/dev/null
    echo "   worker2 killed; compose restarts it, the coordinator redeploys"
fi

wait "${SUBMIT_PID}" && true
SUBMIT_RC=$?
echo "== submit finished (rc=${SUBMIT_RC}):"
tail -1 /tmp/cs-2pc-submit.log
grep -q "ok=1" /tmp/cs-2pc-submit.log || {
    echo "FAIL: job did not complete cleanly after the kill" >&2
    exit 1
}

echo "== gating the case table against the manifest (exactly-once)"
${COMPOSE} exec -T postgres psql -U sentry -d sentry -tA -F'|' -c \
    "SELECT pattern, count(*) FROM alerts GROUP BY pattern ORDER BY pattern;" \
    > /tmp/cs-2pc-counts.txt
python3 - /tmp/cs-2pc-counts.txt data/manifest-2pc.json <<'PY'
import json, sys

rows = [l.split("|") for l in open(sys.argv[1]).read().strip().splitlines() if l]
got = {p: int(n) for p, n in rows}

cep = {"card_testing", "impossible_travel", "account_takeover",
       "otp_never_verified", "structuring"}
manifest = json.load(open(sys.argv[2]))
want = {}
for e in manifest["expected_alerts"]:
    if e["pattern"] in cep:
        want[e["pattern"]] = want.get(e["pattern"], 0) + 1

print(f"case table: {sum(got.values())} rows, expected {sum(want.values())}")
for p in sorted(want):
    print(f"  {p}: {got.get(p, 0)} (expected {want[p]})")

if got != want:
    print("\nFAIL: the case table diverges from the manifest - "
          "a duplicate means broken exactly-once, a gap means lost alerts")
    sys.exit(1)
print("\nOK: killed a worker mid-run; the case table holds exactly the "
      "manifest's alerts - no duplicates, no gaps.")
PY

if [[ "${KEEP_UP:-0}" != "1" ]]; then
    ${COMPOSE} down -v --remove-orphans >/dev/null 2>&1 || true
else
    echo "KEEP_UP=1: cluster left running - dashboard at http://localhost:8081"
fi
echo "2pc scene: PASS"
