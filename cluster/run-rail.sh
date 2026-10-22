#!/usr/bin/env bash
# The payment rail: Kafka in, two stores out, a worker killed mid-flight.
#
# The five detectors run on the cluster against a Kafka topic instead of a
# file. Every alert lands in TWO places under one checkpoint: the Postgres
# case table through PREPARE TRANSACTION / COMMIT PREPARED, and a Kafka
# alerts topic inside a broker transaction. The feed is produced the way a
# real rail delivers it - in batches, by an at-least-once producer that
# re-sends one batch it had already delivered - and part-way through, once
# the case table shows alerts committing, a worker is killed -9 while more
# batches are still to come.
#
# What the gates prove, each from outside the engine:
#
#   1. the case table holds EXACTLY the manifest's alerts: no duplicate
#      from the replay after the kill, no gap from the crash, and nothing
#      from the retried batch (the idempotency guard dropped it);
#   2. the alerts topic, read with isolation.level=read_committed, holds
#      the same exact set. Read uncommitted, the log also shows what the
#      killed worker's aborted transactions left behind - records a
#      transactional consumer never sees;
#   3. the Kafka source resumed from its checkpointed offsets: the batches
#      produced after the kill are detected and committed by the
#      redeployed job, not lost and not reprocessed from the start.
#
# GUARD=0 removes the idempotency guard and is the scene's negative control:
# the retried batch then reaches the detectors twice and the oracle MUST
# fail (the script expects that and passes when it does). TRACE=1 records
# the protocol trace of every process and, after the run, checks it against
# clink's exactly-once specification (cluster/validate-trace.sh).
#
#   CLINK_IMAGE=...   override the runtime image
#   DAYS=n            tape length (default 90, ~0.8M events; same campaigns
#                     as the 3-day tape, spread over 90 days of noise)
#   CHUNKS=n          producer batches (default 16)
#   DUP_CHUNK=k       the batch the producer re-sends (default 2)
#   DUP_AFTER_KILL=1  re-send it only once the coordinator has restored the
#                     job after the kill, instead of right after batch k+1:
#                     every id in it was then checkpointed long before the
#                     restore point, so the guard's restored state alone
#                     decides whether the retry is dropped
#   KILL_AT=n         kill worker2 once the case table holds n alerts (default 3)
#   GUARD=0           negative control (see above)
#   GUARD_TTL_DAYS=n  the guard's event-time TTL (default 30). The producer's
#                     replay horizon here is one batch, 90/CHUNKS days of
#                     EVENT time, so the TTL must cover it; GUARD_TTL_DAYS=1
#                     is the horizon control - the re-sent batch is older
#                     than the TTL by the time it arrives, the guard has
#                     forgotten its ids, and the oracle fails
#   GUARD_DEBUG=1     the guard logs its restored state and every replay it
#                     admits (worker log, kept in out-cluster/rail-compose.log)
#   KAFKA_ALERTS=0    no transactional Kafka alert sink: Postgres is the only
#                     exactly-once store (gate 2 is skipped)
#   TRACE=1           record the protocol trace and run it through the
#                     specification's checker (reported, not gated). Needs
#                     KAFKA_ALERTS=0: the trace module models one sink family
#                     per job (Kafka's commit-confirmed protocol, or the
#                     recoverable family Postgres belongs to), and this job
#                     mixes both when the Kafka alert sink is on
#   KEEP_UP=1         leave the cluster running after the gate

set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT}"
COMPOSE="docker compose -f cluster/docker-compose.yml --profile rail"
DAYS="${DAYS:-90}"
CHUNKS="${CHUNKS:-16}"
DUP_CHUNK="${DUP_CHUNK:-2}"
DUP_AFTER_KILL="${DUP_AFTER_KILL:-0}"
KILL_AT="${KILL_AT:-3}"
GUARD="${GUARD:-1}"
GUARD_TTL_DAYS="${GUARD_TTL_DAYS:-30}"
GUARD_TTL_MS=$((GUARD_TTL_DAYS * 86400000))
KAFKA_ALERTS="${KAFKA_ALERTS:-1}"
TRACE="${TRACE:-0}"
if [[ "${TRACE}" == "1" && "${KAFKA_ALERTS}" != "0" ]]; then
    echo "TRACE=1 needs KAFKA_ALERTS=0: the exactly-once specification's trace module models one" >&2
    echo "sink family per job, and with the Kafka alert sink on this job mixes the Kafka and the" >&2
    echo "recoverable (Postgres) families. Run: TRACE=1 KAFKA_ALERTS=0 cluster/run-rail.sh" >&2
    exit 2
fi
DSN="host=postgres port=5432 dbname=sentry user=sentry password=sentry"
PATTERNS="card_testing,impossible_travel,account_takeover,otp_never_verified,structuring"
COORD="http://localhost:8081"
EVENTS_TOPIC="payments.events"
ALERTS_TOPIC="fraud.alerts"
KAFKA_BIN="/opt/kafka/bin"

say() { printf '\n== %s\n' "$*"; }
kafka() { ${COMPOSE} exec -T kafka "$@"; }
psql_() { ${COMPOSE} exec -T postgres psql -U sentry -d sentry -tA "$@"; }
case_rows() { psql_ -c "SELECT count(*) FROM alerts" 2>/dev/null | tr -d '[:space:]' || echo 0; }
job_status() { curl -fsS "${COORD}/api/v1/jobs" 2>/dev/null | grep -o '"status":"[A-Z_]*"' | head -1 || true; }
job_running() { [[ "$(job_status)" == '"status":"RUNNING"' ]]; }

# poll <seconds> <what> <command...>: retry until the command succeeds or
# the deadline passes; every wait is for a condition, never a duration.
poll() {
    local deadline=$(( $(date +%s) + $1 )) what=$2
    shift 2
    until "$@" >/dev/null 2>&1; do
        if [[ "$(date +%s)" -ge "${deadline}" ]]; then
            echo "FAIL: gave up waiting for ${what}" >&2
            return 1
        fi
        sleep 1
    done
}

diagnostics() {
    echo
    echo "run-rail.sh: FAILED - diagnostics follow" >&2
    ${COMPOSE} ps -a || true
    echo "----- jobs"; curl -s "${COORD}/api/v1/jobs" || true; echo
    echo "----- coordinator (recovery lines)"
    ${COMPOSE} logs --no-color coordinator 2>&1 | grep -iE 'lost|restart|restor|redeploy|recover|error' | cut -c1-220 | tail -20 || true
    echo "----- worker2 (tail)"; ${COMPOSE} logs --no-color --tail 30 worker2 2>&1 | cut -c1-220 || true
    echo "----- case table"; psql_ -c "SELECT pattern, count(*) FROM alerts GROUP BY pattern ORDER BY pattern" || true
    echo "----- prepared transactions"; psql_ -c "SELECT gid FROM pg_prepared_xacts" || true
}

cleanup() {
    local rc=$?
    if [[ "${rc}" -ne 0 ]]; then diagnostics; fi
    # Every run leaves its process logs behind for forensics, pass or fail.
    ${COMPOSE} logs --no-color > out-cluster/rail-compose.log 2>&1 || true
    if [[ "${KEEP_UP:-0}" == "1" ]]; then
        echo "KEEP_UP=1: cluster left running - dashboard at http://localhost:8081"
    else
        ${COMPOSE} down -v --remove-orphans >/dev/null 2>&1 || true
    fi
    exit "${rc}"
}
trap cleanup EXIT

say "the ${DAYS}-day tape and the producer's feed"
if [[ ! -f data-rail/manifest.json || "$(python3 -c "import json;print(json.load(open('data-rail/manifest.json'))['days'])")" != "${DAYS}" ]]; then
    python3 tools/csgen.py --days "${DAYS}" --out data-rail
fi
python3 tools/railfeed.py --tape data-rail/events.ndjson --out data-rail/feed \
    --chunks "${CHUNKS}" --dup-chunk "${DUP_CHUNK}"
EXPECTED="$(python3 - "${PATTERNS}" <<'PY'
import json, sys
want = set(sys.argv[1].split(","))
m = json.load(open("data-rail/manifest.json"))
print(sum(1 for e in m["expected_alerts"] if e["pattern"] in want))
PY
)"
echo "manifest: ${EXPECTED} alerts expected from the five shipped detectors"

say "cluster up, fresh (coordinator + 2 workers + postgres + kafka)"
mkdir -p out-cluster cluster/jobs cluster/ckpt cluster/trace
rm -rf cluster/ckpt/* cluster/trace/* out-cluster/alerts-*.ndjson out-cluster/otp-healthy.ndjson out-cluster/rail-* out-cluster/restore-point-*
if [[ "${TRACE}" == "1" ]]; then export CS_TRACE_DIR=/trace; else export CS_TRACE_DIR=; fi
${COMPOSE} down -v --remove-orphans >/dev/null 2>&1 || true
${COMPOSE} up -d --wait coordinator worker1 worker2 postgres kafka

say "topics and the case table"
for t in "${EVENTS_TOPIC}" "${ALERTS_TOPIC}"; do
    kafka "${KAFKA_BIN}/kafka-topics.sh" --bootstrap-server localhost:9092 --create \
        --if-not-exists --topic "${t}" --partitions 1 --replication-factor 1 >/dev/null
done
psql_ -q -c "SET client_min_messages TO WARNING;
             DROP TABLE IF EXISTS alerts;
             CREATE TABLE alerts (pattern text, entity_kind text, entity_id bigint,
                                  ts bigint, detail text);"

say "building the job plugin inside the runtime image"
${COMPOSE} exec -T coordinator bash -c "
    set -euo pipefail
    cmake -S /mnt/card-sentry/app -B /tmp/job-build > /dev/null
    cmake --build /tmp/job-build --target card_sentry_job --parallel 4 | tail -1
    cp /tmp/job-build/card_sentry_job.so /jobs/card_sentry_job.so
"

say "submitting: kafka source, idempotency guard $([[ "${GUARD}" == "1" ]] && echo "ON (TTL ${GUARD_TTL_DAYS} days of event time)" || echo OFF), postgres 2PC$([[ "${KAFKA_ALERTS}" == "1" ]] && echo " + kafka 2PC") sinks, checkpoints every 1000 ms"
ALERTS_ENV=(-e "CS_KAFKA_ALERTS_TOPIC=${ALERTS_TOPIC}")
if [[ "${KAFKA_ALERTS}" != "1" ]]; then ALERTS_ENV=(-e CS_KAFKA_ALERTS_TOPIC=); fi
# The job is unbounded (a topic has no end), so the submit's completion
# wait is told to return almost at once; the job keeps running and the
# reply carries its id.
SUBMIT_OUT="$(${COMPOSE} exec -T \
    -e CS_SOURCE=kafka -e CS_KAFKA_BROKERS=kafka:9092 -e "CS_KAFKA_TOPIC=${EVENTS_TOPIC}" \
    "${ALERTS_ENV[@]}" \
    -e "CS_PG_DSN=${DSN}" -e CS_PG_TABLE=alerts -e CS_OUT_DIR=/out \
    -e "CS_GUARD=${GUARD}" -e "CS_GUARD_TTL_MS=${GUARD_TTL_MS}" -e "CS_GUARD_DEBUG=${GUARD_DEBUG:-0}" \
    coordinator clink_submit_job \
    --job=/jobs/card_sentry_job.so \
    --coordinator-host=127.0.0.1 --coordinator-port=6123 \
    --name=card-sentry-rail \
    --state-backend=file:/ckpt/state --checkpoint-interval-ms=1000 \
    --max-restarts-on-worker-loss=3 \
    --wait-timeout-s=2 2>&1 || true)"
JOB_ID="$(echo "${SUBMIT_OUT}" | grep -o '"job_id":[0-9]*' | head -1 | cut -d: -f2)"
if [[ -z "${JOB_ID}" || "${JOB_ID}" == "0" ]]; then
    echo "FAIL: the job was not accepted" >&2
    echo "${SUBMIT_OUT}" >&2
    exit 1
fi
poll 60 "job ${JOB_ID} to be RUNNING" job_running
echo "job ${JOB_ID} accepted and RUNNING ($(curl -fsS "${COORD}/api/v1/jobs" | grep -o '"expected_completion":[0-9]*' | head -1 | cut -d: -f2) subtasks); the submit's completion wait was not waited for - a topic has no end"
# The coordinator names the job's weakest sink: the diagnostic file sinks
# are at-least-once by design, which is why the line below does not read
# EXACTLY_ONCE. The two stores the gates read are the transactional ones.
${COMPOSE} logs --no-color coordinator 2>&1 | grep -o "job delivery guarantee: .*" | tail -1 || true

resend_batch() {
    echo "   batch ${DUP_CHUNK} re-sent: the producer timed out on its ack and retried"
    kafka sh -c "${KAFKA_BIN}/kafka-console-producer.sh --bootstrap-server localhost:9092 \
        --topic ${EVENTS_TOPIC} < /tape/feed/retry.ndjson" >> out-cluster/rail-producer.log 2>&1
}

if [[ "${DUP_AFTER_KILL}" == "1" ]]; then
    say "producing the feed in ${CHUNKS} batches; batch ${DUP_CHUNK} is re-sent once the job has been restored after the kill"
else
    say "producing the feed in ${CHUNKS} batches; batch ${DUP_CHUNK} is re-sent after batch $((DUP_CHUNK + 1))"
fi
KILLED=0
PRODUCED=0
for chunk in data-rail/feed/chunk-*.ndjson; do
    name="$(basename "${chunk}")"
    stem="${name%.ndjson}"
    idx=$((10#${stem#chunk-}))
    kafka sh -c "${KAFKA_BIN}/kafka-console-producer.sh --bootstrap-server localhost:9092 \
        --topic ${EVENTS_TOPIC} < /tape/feed/${name}" >> out-cluster/rail-producer.log 2>&1
    PRODUCED=$((PRODUCED + 1))
    if [[ "${DUP_AFTER_KILL}" != "1" && "${idx}" -eq $((DUP_CHUNK + 1)) ]]; then
        resend_batch
    fi
    rows="$(case_rows)"
    echo "   batch ${idx} produced; case table: ${rows} alert(s) committed"
    if [[ "${KILLED}" -eq 0 && "${rows}" -ge "${KILL_AT}" && "${idx}" -ge "${DUP_CHUNK}" ]]; then
        remaining=$((CHUNKS - PRODUCED))
        say "killing worker2 -9 now: ${rows} alerts committed, ${remaining} batches still to come"
        prepared="$(psql_ -c "SELECT count(*) FROM pg_prepared_xacts" | tr -d '[:space:]')"
        echo "   prepared (in-doubt) transactions in postgres at the kill: ${prepared}"
        docker kill -s KILL cluster-worker2-1 >/dev/null
        KILLED=1
        restored() { ${COMPOSE} logs --no-color coordinator 2>&1 | grep -q "restore point:"; }
        if poll 60 "the coordinator to restore the job" restored; then
            ${COMPOSE} logs --no-color coordinator 2>&1 | grep -oE "worker lost: .*|restore point: .*" | sed 's/^/   coordinator: /' | tail -2
            echo "   worker2 killed; compose restarts it, the coordinator rolled the job back to that checkpoint and redeployed on the survivor"
            # Keep the restore point's snapshot files for forensics before
            # retention sweeps them (clink state-cat / state-query read them).
            rp="$(${COMPOSE} logs --no-color coordinator 2>&1 | grep -oE "restore point: checkpoint [0-9]+" | tail -1 | grep -oE "[0-9]+$")"
            if [[ -n "${rp}" ]]; then
                mkdir -p "out-cluster/restore-point-${rp}"
                for d in cluster/ckpt/state/v1/*/; do
                    sub="$(basename "${d}")"
                    if ls "${d}"checkpoint-"${rp}".snap >/dev/null 2>&1; then
                        mkdir -p "out-cluster/restore-point-${rp}/${sub}"
                        cp "${d}"checkpoint-"${rp}".snap* "out-cluster/restore-point-${rp}/${sub}/" 2>/dev/null || true
                    fi
                done
                echo "   restore point ${rp}'s snapshots kept under out-cluster/restore-point-${rp}/"
            fi
        else
            echo "   worker2 killed; no restore line from the coordinator within 60 s (the gates below still decide)"
        fi
        if [[ "${DUP_AFTER_KILL}" == "1" ]]; then
            # Give the redeployed job a moment to open before the retry lands.
            sleep 5
            resend_batch
        fi
    fi
done
if [[ "${KILLED}" -eq 0 ]]; then
    echo "FAIL: the case table never reached KILL_AT=${KILL_AT} before the feed ended - lower KILL_AT" >&2
    exit 1
fi

say "waiting for the case table to converge on the manifest"
rows_reached() { [[ "$(case_rows)" -ge "${EXPECTED}" ]]; }
if [[ "${GUARD}" == "1" ]]; then
    poll 240 "${EXPECTED} alerts in the case table" rows_reached
else
    # Without the guard the retried batch reaches the detectors twice; give
    # the false positives time to commit rather than waiting for a count.
    poll 240 "at least ${EXPECTED} alerts in the case table" rows_reached
fi
# A duplicate or a straggler would commit within a checkpoint interval or
# two of the last batch; wait a few intervals, then read the final state.
sleep 6
echo "case table: $(case_rows) rows after settling"
after="$(psql_ -c "SELECT count(*) FROM pg_prepared_xacts" | tr -d '[:space:]')"
echo "prepared (in-doubt) transactions in postgres now: ${after}"

say "gate 1: the case table against the manifest (exactly-once into postgres)"
psql_ -c "SELECT row_to_json(a) FROM alerts a" > out-cluster/rail-case-table.ndjson
set +e
python3 tools/check.py --manifest data-rail/manifest.json \
    --alerts out-cluster/rail-case-table.ndjson --only-patterns "${PATTERNS}"
PG_RC=$?
set -e

KAFKA_RC=0
if [[ "${KAFKA_ALERTS}" == "1" ]]; then
    say "gate 2: the alerts topic, read_committed, against the manifest (exactly-once into kafka)"
    kafka "${KAFKA_BIN}/kafka-console-consumer.sh" --bootstrap-server localhost:9092 \
        --topic "${ALERTS_TOPIC}" --from-beginning --isolation-level read_committed \
        --timeout-ms 8000 > out-cluster/rail-alerts-read-committed.ndjson 2>/dev/null || true
    kafka "${KAFKA_BIN}/kafka-console-consumer.sh" --bootstrap-server localhost:9092 \
        --topic "${ALERTS_TOPIC}" --from-beginning --isolation-level read_uncommitted \
        --timeout-ms 8000 > out-cluster/rail-alerts-read-uncommitted.ndjson 2>/dev/null || true
    set +e
    python3 tools/check.py --manifest data-rail/manifest.json \
        --alerts out-cluster/rail-alerts-read-committed.ndjson --only-patterns "${PATTERNS}"
    KAFKA_RC=$?
    set -e
    committed="$(grep -c . out-cluster/rail-alerts-read-committed.ndjson || true)"
    uncommitted="$(grep -c . out-cluster/rail-alerts-read-uncommitted.ndjson || true)"
    echo "alerts topic: ${uncommitted} records on the log, ${committed} visible to a read_committed consumer"
    if [[ "${uncommitted}" -gt "${committed}" ]]; then
        echo "   the difference is what the killed worker's aborted transactions left on the log: a transactional consumer never sees them"
    fi
else
    say "gate 2 skipped: KAFKA_ALERTS=0, the case table is the only exactly-once store in this run"
fi

say "the diagnostics side"
diag_lines="$(cat out-cluster/alerts-*.ndjson 2>/dev/null | grep -c . || true)"
echo "file sinks: ${diag_lines} alert lines; case table: $(case_rows) rows. The files are at-least-once"
echo "   diagnostics a restore may truncate or duplicate; they are not part of the contract."
if [[ "${GUARD_DEBUG:-0}" == "1" ]]; then
    echo "the guard, from the worker logs (open = what the restored seen-set held):"
    ${COMPOSE} logs --no-color 2>&1 | grep -h "cs.guard\] open" | sed 's/^/   /' | cut -c1-200 || true
fi

say "the input side"
end_offset="$(kafka "${KAFKA_BIN}/kafka-get-offsets.sh" --bootstrap-server localhost:9092 \
    --topic "${EVENTS_TOPIC}" 2>/dev/null | awk -F: '{print $3}' | tr -d '[:space:]')"
python3 - "${end_offset}" <<'PY'
import json, sys
plan = json.load(open("data-rail/feed/feed.json"))
print(f"events topic: {sys.argv[1]} records for {plan['distinct_events']} distinct events "
      f"({plan['duplicate_records']} duplicates from the retried batch)")
PY
${COMPOSE} logs --no-color coordinator 2>&1 | grep -iE 'worker lost|restor|redeploy' | cut -c1-200 | tail -4 || true

if [[ "${TRACE}" == "1" ]]; then
    say "stopping the job so every process flushes its protocol trace"
    ${COMPOSE} exec -T coordinator clink_cancel_job --coordinator-host=127.0.0.1 \
        --coordinator-port=6123 --job-id="${JOB_ID}" >/dev/null 2>&1 || true
    sleep 3
    ${COMPOSE} stop >/dev/null 2>&1 || true
fi

say "verdict"
if [[ "${GUARD}" == "1" ]]; then
    if [[ "${PG_RC}" -ne 0 || "${KAFKA_RC}" -ne 0 ]]; then
        echo "FAIL: exactly-once did not hold (postgres rc=${PG_RC}, kafka rc=${KAFKA_RC})" >&2
        exit 1
    fi
    if [[ "${KAFKA_ALERTS}" == "1" ]]; then
        echo "OK: a worker died mid-flight and a batch was delivered twice; the case table and the"
        echo "    read_committed alerts topic both hold exactly the manifest's alerts."
    else
        echo "OK: a worker died mid-flight and a batch was delivered twice; the case table holds"
        echo "    exactly the manifest's alerts."
    fi
else
    if [[ "${PG_RC}" -eq 0 && "${KAFKA_RC}" -eq 0 ]]; then
        echo "FAIL: without the guard the retried batch should have produced false positives, and did not" >&2
        exit 1
    fi
    echo "OK (negative control): without the idempotency guard the retried batch reached the detectors"
    echo "    twice and the oracle failed, as it must. Exactly-once delivery did not, and cannot, dedupe the input."
fi

if [[ "${TRACE}" == "1" ]]; then
    # Keep the run's trace where the next scene's start-up wipe cannot reach it.
    rm -rf out-cluster/rail-trace && cp -R cluster/trace out-cluster/rail-trace
    say "the recorded run against the exactly-once specification (copy kept under out-cluster/rail-trace)"
    # Reported, not gated: the gates above are the scene's verdict. At clink
    # v0.11.0 the specification models every prepare as staging a
    # transaction, so a sink that received nothing in an interval (most
    # intervals, for a feed with nine alerts in ninety days) and therefore
    # declines its commit is the first step the checker cannot explain. The
    # checker stopping exactly there, and naming the event, is the point.
    set +e
    cluster/validate-trace.sh cluster/trace
    TRACE_RC=$?
    set -e
    if [[ "${TRACE_RC}" -ne 0 ]]; then
        echo "trace validation: the checker stopped where the specification's scope ends (see README,"
        echo "   'The run, checked against the protocol's specification'); the gates above stand."
    fi
fi
echo "rail scene: PASS"
