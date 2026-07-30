#!/usr/bin/env python3
"""card-sentry oracle: compare the detector's alerts against the manifest.

The generator's noise invariants guarantee zero false positives by
construction, so the comparison is exact and total:

  * every expected alert (pattern, entity) appears exactly once;
  * negative-control campaigns produced no alert;
  * NO alert exists for any entity the manifest does not predict - an
    unexpected alert is a detector or engine bug, never noise.

Exit 0 when everything matches; 1 with a diff otherwise.

Usage:
    tools/check.py [--manifest data/manifest.json] [--alerts out/alerts.ndjson]
"""

import argparse
import json
import sys
from collections import Counter


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--manifest", default="data/manifest.json")
    ap.add_argument("--alerts", default="out/alerts.ndjson")
    ap.add_argument("--only-patterns", default="",
                    help="comma-separated pattern subset to gate (deployments "
                         "that ship a subset of the detectors, e.g. the "
                         "cluster plugin); default: every pattern")
    args = ap.parse_args()

    only = {p for p in args.only_patterns.split(",") if p}

    with open(args.manifest) as f:
        manifest = json.load(f)
    if only:
        manifest["expected_alerts"] = [
            e for e in manifest["expected_alerts"] if e["pattern"] in only]
        manifest["campaigns"] = [
            c for c in manifest["campaigns"] if c["pattern"] in only]

    actual = Counter()
    lines = 0
    with open(args.alerts) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            lines += 1
            a = json.loads(line)
            actual[(a["pattern"], a["entity_kind"], a["entity_id"])] += 1

    expected = Counter(
        (e["pattern"], e["entity_kind"], e["entity_id"])
        for e in manifest["expected_alerts"]
    )

    failures = []

    for key, want in sorted(expected.items()):
        got = actual.get(key, 0)
        if got != want:
            failures.append(f"expected {want} alert(s) for {key}, got {got}")

    for c in manifest["campaigns"]:
        if c["expect"] != "none":
            continue
        key = (c["pattern"], c["entity_kind"], c["entity_id"])
        got = actual.get(key, 0)
        if got != 0:
            failures.append(
                f"negative control {key} ({c['detail']}) raised {got} alert(s)"
            )

    unexpected = actual - expected
    for key, n in sorted(unexpected.items()):
        failures.append(f"unexpected alert x{n}: {key} (noise must never alert)")

    print(f"alerts: {lines} written, {sum(expected.values())} expected")
    per_pattern = Counter(k[0] for k in actual.elements())
    for pattern in sorted(per_pattern):
        print(f"  {pattern}: {per_pattern[pattern]}")

    if failures:
        print(f"\nFAIL ({len(failures)}):")
        for msg in failures:
            print(f"  - {msg}")
        return 1
    print("\nOK: every expected alert fired, every control stayed quiet, "
          "zero false positives.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
