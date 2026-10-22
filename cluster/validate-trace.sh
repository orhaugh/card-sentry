#!/usr/bin/env bash
# Check a recorded run against clink's exactly-once specification.
#
# With CLINK_PROTOCOL_TRACE_DIR set, every clink process appends one line
# per protocol step it takes - barrier delivered, transaction prepared,
# checkpoint completed, commit delivered, worker lost, job redeployed - to
# its own file. clink ships the protocol as a TLA+ specification
# (formal/ExactlyOnce.tla) and a trace module that constrains it to a
# recorded run; TLC then accepts the run only if some path through the
# specification produces every recorded step in order. A run the model
# cannot explain is a defect in one of the two, and the first unexplained
# step is named.
#
# This is the strongest statement the scene can make about the kill: not
# "the counts matched this time", but "the steps the engine took under this
# failure are steps the protocol allows".
#
#   cluster/validate-trace.sh cluster/trace
#
# Needs a Java 11+ runtime (TLC) and the clink source at the pinned release
# for formal/ and the validation script. scripts/get-clink.sh leaves that
# source under .clink/src when it built from source; otherwise this script
# fetches just those two directories at the pinned tag. CLINK_SPEC_SOURCE
# names another clink checkout to take the specification from, for a model
# newer than the pinned release's: the empty-interval extension this scene
# prompted landed on clink main after v0.11.0.

set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TRACE_DIR="$(cd "${1:-${ROOT}/cluster/trace}" && pwd)"
CLINK_VERSION="${CLINK_VERSION:-$(grep -o 'CLINK_VERSION:-v[0-9.]*' "${ROOT}/scripts/get-clink.sh" | head -1 | cut -d- -f2)}"
CLINK_REPO="${CLINK_REPO:-https://github.com/orhaugh/clink}"
SRC="${CLINK_SPEC_SOURCE:-${ROOT}/.clink/src}"

if ! command -v java >/dev/null 2>&1; then
    echo "validate-trace: java not found - TLC needs a Java 11+ runtime" >&2
    exit 2
fi
if ! ls "${TRACE_DIR}"/*.ndjson >/dev/null 2>&1; then
    echo "validate-trace: no trace files under ${TRACE_DIR} (run the scene with TRACE=1)" >&2
    exit 2
fi

if [[ ! -f "${SRC}/formal/ExactlyOnce.tla" || ! -x "${SRC}/scripts/formal-check.sh" ]]; then
    # The SDK fast path installs binaries only; fetch the specification and
    # its checker at the same tag the binaries were built from.
    SRC="${ROOT}/.clink/spec-${CLINK_VERSION}"
    if [[ ! -f "${SRC}/formal/ExactlyOnce.tla" ]]; then
        echo "validate-trace: fetching formal/ and scripts/ of clink ${CLINK_VERSION}"
        rm -rf "${SRC}"
        git clone --quiet --depth 1 --branch "${CLINK_VERSION}" --filter=blob:none --sparse \
            "${CLINK_REPO}" "${SRC}"
        git -C "${SRC}" sparse-checkout set --quiet formal scripts
    fi
fi

# One sink family per trace: the specification shares its trace module
# between the Kafka family (commit-confirmed: receipts, confirmations) and
# the recoverable family (Postgres, files, objects) through one constant,
# so a run whose sinks span both cannot be judged as one job.
fams="$(grep -ho '"family":"[a-z]*"' "${TRACE_DIR}"/*.ndjson | sort -u | tr '\n' ' ')"
if [[ "${fams}" == *recoverable* && "${fams}" == *kafka* ]]; then
    echo "validate-trace: the run mixes sink families (${fams}); the specification's trace module" >&2
    echo "models one family per job. Record a run with one family (run-rail.sh KAFKA_ALERTS=0)." >&2
    exit 2
fi

n="$(cat "${TRACE_DIR}"/*.ndjson | wc -l | tr -d ' ')"
echo "trace: ${n} protocol events from $(ls "${TRACE_DIR}"/*.ndjson | wc -l | tr -d ' ') process files"
grep -ho '"event":"[A-Za-z]*"' "${TRACE_DIR}"/*.ndjson | sort | uniq -c | sort -rn | awk '{printf "   %s x%s\n", $2, $1}' | sed 's/"event"://;s/"//g'

# One job per run directory: the scene records one job, so the merge takes
# it; the checker fetches the pinned TLC jars (checksum-verified) on first
# use and caches them.
"${SRC}/scripts/formal-check.sh" --trace "${TRACE_DIR}"
echo "validate-trace: the recorded run is a behaviour of the exactly-once specification"
