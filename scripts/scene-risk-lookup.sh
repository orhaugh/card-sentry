#!/usr/bin/env bash
# Queryable state scene: run the detectors, then read the per-card risk
# profile over the queryable-state HTTP surface - the same routes a
# cluster worker exposes - and gate the served JSON against an expectation
# computed INDEPENDENTLY from the tape by Python.
#
# Verifies three things:
#   1. point lookup: profiles of a campaign card and a noise card match
#      the tape-derived expectation field for field;
#   2. missing keys 404;
#   3. the bounded scan route returns entries (state-as-table over HTTP).
#
# Requires: scripts/get-clink.sh done, tape + app built (run-detections.sh
# does both; this scene rebuilds cheaply if needed).

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PORT="${PORT:-7071}"
BASE="http://127.0.0.1:${PORT}/api/v1/queryable_state"
JSON_BASE="${BASE}/op/cs/subtask/0/json/risk_profile"

if [[ ! -f "${ROOT}/data/events.ndjson" || ! -f "${ROOT}/data/rules.ndjson" ]]; then
    python3 "${ROOT}/tools/csgen.py" --out "${ROOT}/data"
fi
cmake -S "${ROOT}/app" -B "${ROOT}/app/build" \
    -DCMAKE_PREFIX_PATH="${ROOT}/.clink/prefix" > /dev/null
cmake --build "${ROOT}/app/build" --parallel 10 | tail -1

mkdir -p "${ROOT}/out"
"${ROOT}/app/build/card_sentry" \
    --in "${ROOT}/data/events.ndjson" \
    --rules "${ROOT}/data/rules.ndjson" \
    --out "${ROOT}/out/alerts.ndjson" \
    --serve-state "${PORT}" --hold 30 &
APP_PID=$!
trap 'kill ${APP_PID} 2>/dev/null || true; wait ${APP_PID} 2>/dev/null || true' EXIT

# Wait for the serving window (the tape drains first, then the server binds).
for _ in $(seq 1 100); do
    if curl -fsS "${BASE}" > /dev/null 2>&1; then
        break
    fi
    sleep 0.3
done

python3 - "$JSON_BASE" "${ROOT}/data/events.ndjson" <<'PY'
import json, sys, urllib.request, urllib.error

json_base, tape = sys.argv[1], sys.argv[2]

# Independent expectation: fold the tape's auths per card with the same
# documented definitions the operator uses (file order = arrival order;
# last_country follows the max-ts auth, later arrival winning ties).
profiles = {}
for line in open(tape):
    e = json.loads(line)
    if e["type"] != "auth":
        continue
    p = profiles.setdefault(e["card"], {
        "auths": 0, "declines": 0, "total": 0.0, "max_amount": 0.0,
        "last_ts": 0, "last_country": ""})
    p["auths"] += 1
    if not e["approved"]:
        p["declines"] += 1
    p["total"] += e["amount"]
    p["max_amount"] = max(p["max_amount"], e["amount"])
    if e["ts"] >= p["last_ts"]:
        p["last_ts"] = e["ts"]
        p["last_country"] = e["country"]

def get(url):
    try:
        with urllib.request.urlopen(url) as r:
            return r.status, json.loads(r.read())
    except urllib.error.HTTPError as ex:
        return ex.code, None

failures = []

# 1. Point lookups: the card-testing campaign card and one noise card.
for card in (9001, 1001):
    want = profiles[card]
    status, body = get(f"{json_base}?key={card}")
    if status != 200:
        failures.append(f"card {card}: HTTP {status}")
        continue
    got = body["value"]
    checks = [
        ("auths", got["auths"] == want["auths"]),
        ("declines", got["declines"] == want["declines"]),
        ("total", abs(got["total"] - want["total"]) < 0.01),
        ("max_amount", abs(got["max_amount"] - want["max_amount"]) < 0.01),
        ("last_ts", got["last_ts"] == want["last_ts"]),
        ("last_country", got["last_country"] == want["last_country"]),
    ]
    for name, ok in checks:
        if not ok:
            failures.append(f"card {card} {name}: served {got[name]!r} "
                            f"vs tape {want[name]!r}")
    print(f"card {card}: served profile matches the tape "
          f"(auths={got['auths']}, declines={got['declines']}, "
          f"score={got['score']})")

# 2. A key that never authed must 404.
status, _ = get(f"{json_base}?key=424242")
if status != 404:
    failures.append(f"missing key: expected 404, got {status}")
else:
    print("missing key: 404 as specified")

# 3. The bounded scan returns entries.
status, body = get(f"{json_base}/scan?limit=5")
if status != 200 or not body["entries"]:
    failures.append(f"scan: HTTP {status}, entries "
                    f"{len(body['entries']) if body else 'n/a'}")
else:
    print(f"scan: {len(body['entries'])} profiles returned"
          f"{' (truncated)' if body['truncated'] else ''}")

if failures:
    print("\nFAIL:")
    for f in failures:
        print(f"  - {f}")
    sys.exit(1)
print("\nOK: queryable state serves the risk profiles the tape predicts.")
PY

kill ${APP_PID} 2>/dev/null || true
wait ${APP_PID} 2>/dev/null || true
trap - EXIT
echo "scene-risk-lookup: PASS"
