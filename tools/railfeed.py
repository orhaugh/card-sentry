#!/usr/bin/env python3
"""Prepare the payment-rail feed: the tape in producer batches, plus one
batch the producer "retries".

    tools/railfeed.py --tape data-rail/events.ndjson --out data-rail/feed \
                      [--chunks 16] [--dup-chunk 2]

Writes chunk-NN.ndjson (the tape split into --chunks consecutive batches,
arrival order preserved) and retry.ndjson, an exact copy of batch
--dup-chunk. The rail scene produces the batches in order and, after the
batch that follows --dup-chunk, produces retry.ndjson as well: the
at-least-once producer that re-sends a batch it had already delivered.
Every record in it is therefore already on the topic, under the same id,
and the idempotency guard is what keeps the detectors from counting it
twice. feed.json records the counts the scene reports.
"""

import argparse
import json
import os


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--tape", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--chunks", type=int, default=16)
    ap.add_argument("--dup-chunk", type=int, default=2)
    args = ap.parse_args()

    with open(args.tape) as f:
        lines = f.readlines()
    total = len(lines)
    if args.chunks < 2 or not (0 <= args.dup_chunk < args.chunks):
        raise SystemExit("need chunks >= 2 and 0 <= dup-chunk < chunks")
    os.makedirs(args.out, exist_ok=True)
    for old in os.listdir(args.out):
        os.remove(os.path.join(args.out, old))

    per = -(-total // args.chunks)  # ceiling division
    sizes = []
    for i in range(args.chunks):
        part = lines[i * per:(i + 1) * per]
        sizes.append(len(part))
        with open(os.path.join(args.out, f"chunk-{i:02d}.ndjson"), "w") as f:
            f.writelines(part)
    dup = lines[args.dup_chunk * per:(args.dup_chunk + 1) * per]
    with open(os.path.join(args.out, "retry.ndjson"), "w") as f:
        f.writelines(dup)

    plan = {
        "tape": args.tape,
        "chunks": args.chunks,
        "chunk_sizes": sizes,
        "dup_chunk": args.dup_chunk,
        "distinct_events": total,
        "duplicate_records": len(dup),
        "records_on_topic": total + len(dup),
    }
    with open(os.path.join(args.out, "feed.json"), "w") as f:
        json.dump(plan, f, indent=1)
    print(f"feed: {args.chunks} batches of ~{per} records, batch {args.dup_chunk} "
          f"retried ({len(dup)} duplicate records): {total + len(dup)} records "
          f"on the topic for {total} distinct events")


if __name__ == "__main__":
    main()
