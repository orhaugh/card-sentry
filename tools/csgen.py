#!/usr/bin/env python3
"""card-sentry tape generator.

Writes a deterministic synthetic card-fraud event tape:

    data/events.ndjson   one JSON event per line, in ARRIVAL order
    data/manifest.json   seed, volumes, injected campaigns, expected alerts

The tape is card-payment and account activity for a small fleet: card
authorisations (approved and declined), account logins, password changes,
OTP request/verify pairs, and transfers. Event time (`ts`) and arrival
order disagree: most events arrive within 3 seconds of their timestamp,
some up to 40 seconds late, so correct pattern evaluation needs event-time
watermarks, not file order.

Fraud campaigns are injected on dedicated entities (cards 9xxx, accounts
8xxx) and recorded in the manifest together with the exact alerts the
detector must raise. Negative controls - sequences that look close to
fraud but must NOT alert - are recorded with expect "none".

A second file, data/rules.ndjson, carries the dynamic watchlist rules the
broadcast-state detector consumes: merchant watchlists and per-merchant
amount caps, each EFFECTIVE-DATED via `activate_at` (event time). Alert
outcomes therefore depend only on event times, never on the race between
the rules stream and the event stream; the file ends with an end_of_rules
marker that seals the rule set (the detector buffers events until then).
Rule campaigns use dedicated merchants that never appear in noise.

Noise invariants (what makes the oracle exact):

  * noise cards never emit 3+ sub-2.00 declined auths in any 10-minute
    window: noise declines are isolated (>= 45 min apart per card) and
    always >= 8.00;
  * noise cards stay in their home city for the whole tape, and successive
    auths on one card are >= 10 minutes apart, so apparent travel speed
    stays far below the 900 km/h impossible-travel threshold;
  * noise accounts never exceed 2 login failures in any 30-minute window
    (fail runs are 1-2 events, at most one run per 2 hours);
  * noise password changes are always preceded by an OTP verify within
    the prior 3 minutes, and noise accounts never reach 3 login failures
    beforehand anyway;
  * noise OTP requests are always verified 45-120 seconds later - past
    the 40-second lateness cap, so the verify's ARRIVAL can never precede
    its request's (the CEP matcher consumes events in arrival order);
  * noise transfers avoid the 800.00-999.99 structuring band, except at
    most two band transfers per account per tape, which stay below both
    the 3-event and the 3000.00 cumulative structuring thresholds.

Any alert on a non-campaign entity is therefore a detector or engine bug,
and the checker treats it as a failure.

Usage:
    tools/csgen.py [--seed 7] [--days 3] [--out data]
"""

import argparse
import json
import math
import os
import random

# ---------------------------------------------------------------------------
# Detection thresholds. Mirrored in app/src/patterns.hpp; the generator only
# uses them to steer noise WELL clear of the decision boundaries.
# ---------------------------------------------------------------------------
PROBE_MAX = 2.00          # card testing: probe auths are declined and below this
STRIKE_MIN = 250.00       # card testing: the strike is approved and above this
TRAVEL_KMH = 900.0        # impossible travel: apparent speed threshold
DRAIN_MIN = 1500.00       # account takeover: the draining transfer
STRUCT_LO, STRUCT_HI = 800.00, 1000.00   # structuring band [lo, hi)
STRUCT_TOTAL = 3000.00    # structuring: cumulative band total that trips
OTP_WINDOW_S = 300        # OTP must verify within this many seconds

BASE_TS = 1_753_920_000_000  # 2025-07-31 00:00:00 UTC, fixed epoch for the tape

CITIES = [
    ("London", 51.5074, -0.1278, "GB"),
    ("Manchester", 53.4808, -2.2426, "GB"),
    ("Paris", 48.8566, 2.3522, "FR"),
    ("Berlin", 52.5200, 13.4050, "DE"),
    ("Madrid", 40.4168, -3.7038, "ES"),
    ("Milan", 45.4642, 9.1900, "IT"),
    ("Amsterdam", 52.3676, 4.9041, "NL"),
    ("Dublin", 53.3498, -6.2603, "IE"),
    ("Lisbon", 38.7223, -9.1393, "PT"),
    ("Vienna", 48.2082, 16.3738, "AT"),
    ("Copenhagen", 55.6761, 12.5683, "DK"),
    ("Prague", 50.0755, 14.4378, "CZ"),
]

MERCHANTS = [
    "corner-espresso", "metro-tickets", "green-grocer", "petrol-24",
    "book-nook", "street-noodles", "cinema-plex", "pharmacy-one",
    "hardware-house", "flower-cart", "deli-fresh", "news-kiosk",
]

SINGAPORE = ("Singapore", 1.3521, 103.8198, "SG")

# Merchants reserved for the dynamic-rules campaigns; never used by noise.
RULE_MERCHANTS = ["grey-imports", "night-bazaar", "grand-casino", "pop-up-vintage"]


def event(ts, etype, *, card=0, account=0, amount=0.0, approved=0,
          present=0, lat=0.0, lon=0.0, merchant="", country=""):
    return {
        "ts": int(ts),
        "type": etype,
        "card": int(card),
        "account": int(account),
        "amount": round(float(amount), 2),
        "approved": int(approved),
        "present": int(present),
        "lat": round(float(lat), 6),
        "lon": round(float(lon), 6),
        "merchant": merchant,
        "country": country,
    }


class Tape:
    """Collects (event, lateness_ms) pairs; assigns arrival and ids at write."""

    def __init__(self, rng):
        self.rng = rng
        self.rows = []

    def add(self, ev, max_lateness_ms=None):
        r = self.rng.random()
        if max_lateness_ms is not None:
            late = self.rng.uniform(0, max_lateness_ms)
        elif r < 0.90:
            late = self.rng.uniform(0, 3_000)
        elif r < 0.99:
            late = self.rng.uniform(3_000, 8_000)
        else:
            late = self.rng.uniform(8_000, 40_000)
        self.rows.append((ev["ts"] + int(late), ev))

    def write(self, path):
        self.rows.sort(key=lambda p: (p[0], p[1]["ts"], p[1]["type"],
                                      p[1]["card"], p[1]["account"]))
        with open(path, "w") as f:
            for i, (_, ev) in enumerate(self.rows):
                out = {"id": i + 1}
                out.update(ev)
                f.write(json.dumps(out, separators=(",", ":")) + "\n")
        return len(self.rows)


def jitter_pos(rng, city):
    _, lat, lon, country = city
    return (lat + rng.uniform(-0.15, 0.15), lon + rng.uniform(-0.15, 0.15),
            country)


# ---------------------------------------------------------------------------
# Noise
# ---------------------------------------------------------------------------

def gen_card_noise(rng, tape, card_id, span_ms):
    """One noise card: home-city auths, >=10 min apart, occasional isolated
    declines that are never probe-sized."""
    city = rng.choice(CITIES)
    t = BASE_TS + rng.uniform(0, 3_600_000)
    last_decline = -10**18
    while t < BASE_TS + span_ms:
        lat, lon, country = jitter_pos(rng, city)
        amount = round(rng.uniform(8.0, 420.0), 2)
        approved = 1
        # Isolated declines, >= 45 minutes since the previous one on this card.
        if rng.random() < 0.03 and t - last_decline >= 2_700_000:
            approved = 0
            last_decline = t
        tape.add(event(t, "auth", card=card_id, amount=amount,
                       approved=approved, present=int(rng.random() < 0.6),
                       lat=lat, lon=lon, merchant=rng.choice(MERCHANTS),
                       country=country))
        # >= 10 minutes between auths keeps apparent speed far below the
        # impossible-travel threshold given <= ~47 km of in-city jitter.
        t += rng.uniform(600_000, 5_400_000)


def gen_account_noise(rng, tape, account_id, span_ms):
    """One noise account: logins (fail runs capped at 2, one run per 2 h),
    OTP pairs always verified quickly, occasional verified password change,
    transfers outside the structuring band (<= 2 band exceptions)."""
    t = BASE_TS + rng.uniform(0, 3_600_000)
    last_fail_run = -10**18
    band_transfers = 0
    while t < BASE_TS + span_ms:
        r = rng.random()
        if r < 0.40:  # login activity
            if rng.random() < 0.15 and t - last_fail_run >= 7_200_000:
                n_fails = rng.choice([1, 2])
                last_fail_run = t
                for _ in range(n_fails):
                    tape.add(event(t, "login_fail", account=account_id))
                    t += rng.uniform(5_000, 25_000)
            tape.add(event(t, "login_ok", account=account_id))
        elif r < 0.65:  # OTP pair, always verified within 45-120 s. The
            # gap must exceed the tape's 40 s lateness cap: the CEP matcher
            # consumes events in ARRIVAL order, so a verify whose event-time
            # gap is inside the reordering band could arrive before its
            # request and the request would (correctly, per the arrival
            # order) time out.
            tape.add(event(t, "otp_request", account=account_id))
            t += rng.uniform(45_000, 120_000)
            tape.add(event(t, "otp_verify", account=account_id))
        elif r < 0.72:  # verified password change (same 45 s floor)
            tape.add(event(t, "otp_request", account=account_id))
            t += rng.uniform(45_000, 90_000)
            tape.add(event(t, "otp_verify", account=account_id))
            t += rng.uniform(30_000, 150_000)
            tape.add(event(t, "password_change", account=account_id))
        else:  # transfer
            if band_transfers < 2 and rng.random() < 0.04:
                amount = rng.uniform(STRUCT_LO, STRUCT_HI - 0.01)
                band_transfers += 1
            elif rng.random() < 0.5:
                amount = rng.uniform(40.0, 700.0)
            else:
                amount = rng.uniform(1_200.0, 5_200.0)
            tape.add(event(t, "transfer", account=account_id, amount=amount))
        t += rng.uniform(1_800_000, 14_400_000)


# ---------------------------------------------------------------------------
# Campaigns. Campaign events use tight lateness (<= 1 s) and generous gaps
# (>= 45 s) so their per-entity arrival order equals their event-time order.
# ---------------------------------------------------------------------------

def c_card_testing(rng, tape, card_id, start):
    city = rng.choice(CITIES)
    lat, lon, country = jitter_pos(rng, city)
    t = start
    probes = []
    events = []
    for _ in range(rng.choice([5, 6, 7])):
        amount = round(rng.uniform(0.50, 1.90), 2)
        probes.append(amount)
        ev = event(t, "auth", card=card_id, amount=amount, approved=0,
                   present=0, lat=lat, lon=lon, merchant="web-donations",
                   country=country)
        tape.add(ev, max_lateness_ms=1_000)
        events.append(ev["ts"])
        t += rng.uniform(60_000, 90_000)
    strike = round(rng.uniform(480.0, 520.0), 2)
    ev = event(t, "auth", card=card_id, amount=strike, approved=1, present=0,
               lat=lat, lon=lon, merchant="lux-electronics", country=country)
    tape.add(ev, max_lateness_ms=1_000)
    return {"pattern": "card_testing", "entity_kind": "card",
            "entity_id": card_id, "expect": "alert",
            "detail": f"{len(probes)} probes then {strike:.2f} strike"}


def c_impossible_travel(rng, tape, card_id, start, second_city=SINGAPORE,
                        gap_min=45, expect="alert"):
    a_city = CITIES[0]  # London
    lat, lon, country = a_city[1], a_city[2], a_city[3]
    ev1 = event(start, "auth", card=card_id, amount=rng.uniform(40, 90),
                approved=1, present=1, lat=lat, lon=lon,
                merchant="corner-espresso", country=country)
    tape.add(ev1, max_lateness_ms=1_000)
    t2 = start + gap_min * 60_000
    _, lat2, lon2, country2 = second_city
    ev2 = event(t2, "auth", card=card_id, amount=rng.uniform(60, 120),
                approved=1, present=1, lat=lat2, lon=lon2,
                merchant="harbour-dutyfree", country=country2)
    tape.add(ev2, max_lateness_ms=1_000)
    return {"pattern": "impossible_travel", "entity_kind": "card",
            "entity_id": card_id, "expect": expect,
            "detail": f"London then {second_city[0]} {gap_min} min later"}


def c_account_takeover(rng, tape, account_id, start, with_otp=False):
    t = start
    for _ in range(rng.choice([4, 5])):
        tape.add(event(t, "login_fail", account=account_id),
                 max_lateness_ms=1_000)
        t += rng.uniform(45_000, 70_000)
    if with_otp:  # the negative control: a verified recovery, not a takeover
        tape.add(event(t, "otp_request", account=account_id),
                 max_lateness_ms=1_000)
        t += 60_000
        tape.add(event(t, "otp_verify", account=account_id),
                 max_lateness_ms=1_000)
        t += 60_000
    t += 120_000
    tape.add(event(t, "password_change", account=account_id),
             max_lateness_ms=1_000)
    t += 180_000
    tape.add(event(t, "transfer", account=account_id,
                   amount=round(rng.uniform(2_200, 2_600), 2)),
             max_lateness_ms=1_000)
    return {"pattern": "account_takeover", "entity_kind": "account",
            "entity_id": account_id,
            "expect": "none" if with_otp else "alert",
            "detail": "fails -> password change -> drain"
                      + (" (verified recovery)" if with_otp else "")}


def c_otp_timeout(rng, tape, account_id, start, verified_after_s=None):
    tape.add(event(start, "otp_request", account=account_id),
             max_lateness_ms=1_000)
    if verified_after_s is not None:
        tape.add(event(start + verified_after_s * 1_000, "otp_verify",
                       account=account_id), max_lateness_ms=1_000)
        return {"pattern": "otp_never_verified", "entity_kind": "account",
                "entity_id": account_id, "expect": "none",
                "detail": f"verified after {verified_after_s} s"}
    # A later, normally-verified pair on the same account proves the
    # detector times out the one lost request, not the account.
    t2 = start + 40 * 60_000
    tape.add(event(t2, "otp_request", account=account_id),
             max_lateness_ms=1_000)
    tape.add(event(t2 + 45_000, "otp_verify", account=account_id),
             max_lateness_ms=1_000)
    return {"pattern": "otp_never_verified", "entity_kind": "account",
            "entity_id": account_id, "expect": "alert",
            "detail": "request never verified"}


def c_structuring(rng, tape, account_id, start, amounts, gap_h=(2, 5),
                  expect="alert"):
    t = start
    for a in amounts:
        tape.add(event(t, "transfer", account=account_id, amount=a),
                 max_lateness_ms=1_000)
        t += rng.uniform(gap_h[0] * 3_600_000, gap_h[1] * 3_600_000)
    return {"pattern": "structuring", "entity_kind": "account",
            "entity_id": account_id, "expect": expect,
            "detail": f"band transfers {amounts}"}


def c_rule_merchant(rng, tape, rules, card_id, merchant, kind, activate_at,
                    auth_times_amounts, cap=0.0):
    """A dynamic-rule campaign: one effective-dated rule plus a card's auths
    at that merchant. Expected alerts follow from event times (and the cap)
    alone, so the stream race is irrelevant."""
    rules.append({"kind": kind, "merchant": merchant,
                  "activate_at": int(activate_at), "cap": round(cap, 2)})
    city = rng.choice(CITIES)
    pattern = ("watchlist_hit" if kind == "merchant_watchlist"
               else "cap_exceeded")
    hits = 0
    for t, amount in auth_times_amounts:
        lat, lon, country = jitter_pos(rng, city)
        tape.add(event(t, "auth", card=card_id, amount=amount, approved=1,
                       present=1, lat=lat, lon=lon, merchant=merchant,
                       country=country))
        if t >= activate_at and (kind == "merchant_watchlist" or amount > cap):
            hits += 1
    return {"pattern": pattern, "entity_kind": "card", "entity_id": card_id,
            "expect": "alert" if hits else "none", "alerts": hits,
            "detail": f"{merchant} {kind} from t+{activate_at - BASE_TS} ms, "
                      f"{len(auth_times_amounts)} auths, {hits} expected"}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--seed", type=int, default=7)
    ap.add_argument("--days", type=int, default=3)
    ap.add_argument("--cards", type=int, default=220)
    ap.add_argument("--accounts", type=int, default=160)
    ap.add_argument("--out", default="data")
    args = ap.parse_args()

    rng = random.Random(args.seed)
    span_ms = args.days * 86_400_000
    tape = Tape(rng)

    for i in range(args.cards):
        gen_card_noise(rng, tape, 1_000 + i, span_ms)
    for i in range(args.accounts):
        gen_account_noise(rng, tape, 2_000 + i, span_ms)

    day = 86_400_000
    rules = []
    campaigns = [
        c_card_testing(rng, tape, 9_001, BASE_TS + int(0.4 * day)),
        c_card_testing(rng, tape, 9_002, BASE_TS + int(1.6 * day)),
        c_impossible_travel(rng, tape, 9_101, BASE_TS + int(0.7 * day)),
        c_impossible_travel(rng, tape, 9_102, BASE_TS + int(2.2 * day)),
        c_impossible_travel(rng, tape, 9_103, BASE_TS + int(1.1 * day),
                            second_city=CITIES[2], gap_min=50, expect="none"),
        c_account_takeover(rng, tape, 8_001, BASE_TS + int(0.9 * day)),
        c_account_takeover(rng, tape, 8_002, BASE_TS + int(1.9 * day),
                           with_otp=True),
        c_otp_timeout(rng, tape, 8_101, BASE_TS + int(0.5 * day)),
        c_otp_timeout(rng, tape, 8_102, BASE_TS + int(1.3 * day)),
        c_otp_timeout(rng, tape, 8_103, BASE_TS + int(2.5 * day)),
        c_otp_timeout(rng, tape, 8_104, BASE_TS + int(1.7 * day),
                      verified_after_s=290),
        c_structuring(rng, tape, 8_201, BASE_TS + int(0.6 * day),
                      [850.00, 920.00, 880.00, 940.00]),
        c_structuring(rng, tape, 8_202, BASE_TS + int(0.3 * day),
                      [870.00, 910.00], gap_h=(26, 28), expect="none"),
        # Dynamic rules: a watchlist live from the start, a watchlist that
        # activates mid-tape (the same card's auths flip from silent to
        # alerting at the boundary), an amount cap, and a control whose
        # rule activates only after its auths.
        c_rule_merchant(rng, tape, rules, 9_301, "grey-imports",
                        "merchant_watchlist", BASE_TS,
                        [(BASE_TS + int(0.8 * day), 95.00),
                         (BASE_TS + int(1.5 * day), 140.00)]),
        c_rule_merchant(rng, tape, rules, 9_302, "night-bazaar",
                        "merchant_watchlist", BASE_TS + int(1.5 * day),
                        [(BASE_TS + int(0.6 * day), 60.00),
                         (BASE_TS + int(1.0 * day), 75.00),
                         (BASE_TS + int(1.9 * day), 82.00),
                         (BASE_TS + int(2.4 * day), 66.00)]),
        c_rule_merchant(rng, tape, rules, 9_303, "grand-casino",
                        "merchant_cap", BASE_TS,
                        [(BASE_TS + int(0.5 * day), 320.00),
                         (BASE_TS + int(1.1 * day), 480.00),
                         (BASE_TS + int(1.7 * day), 650.00),
                         (BASE_TS + int(2.3 * day), 720.00)], cap=500.00),
        c_rule_merchant(rng, tape, rules, 9_304, "pop-up-vintage",
                        "merchant_watchlist", BASE_TS + int(2.9 * day),
                        [(BASE_TS + int(0.5 * day), 45.00),
                         (BASE_TS + int(1.2 * day), 58.00)]),
    ]

    os.makedirs(args.out, exist_ok=True)
    n = tape.write(os.path.join(args.out, "events.ndjson"))

    with open(os.path.join(args.out, "rules.ndjson"), "w") as f:
        for r in rules:
            f.write(json.dumps(r, separators=(",", ":")) + "\n")
        f.write(json.dumps({"kind": "end_of_rules", "merchant": "",
                            "activate_at": 0, "cap": 0.0},
                           separators=(",", ":")) + "\n")

    expected = []
    for c in campaigns:
        count = c.get("alerts", 1 if c["expect"] == "alert" else 0)
        expected.extend(
            [{"pattern": c["pattern"], "entity_kind": c["entity_kind"],
              "entity_id": c["entity_id"]}] * count
        )
    manifest = {
        "seed": args.seed,
        "days": args.days,
        "base_ts": BASE_TS,
        "events": n,
        "thresholds": {
            "probe_max": PROBE_MAX, "strike_min": STRIKE_MIN,
            "travel_kmh": TRAVEL_KMH, "drain_min": DRAIN_MIN,
            "struct_band": [STRUCT_LO, STRUCT_HI],
            "struct_total": STRUCT_TOTAL, "otp_window_s": OTP_WINDOW_S,
        },
        "campaigns": campaigns,
        "expected_alerts": expected,
    }
    with open(os.path.join(args.out, "manifest.json"), "w") as f:
        json.dump(manifest, f, indent=2)
        f.write("\n")

    print(f"wrote {n} events, {len(campaigns)} campaigns "
          f"({len(expected)} expected alerts) to {args.out}/")


if __name__ == "__main__":
    main()
