#pragma once
// The decision path: one post-authorisation risk decision per card
// authorisation.
//
// A payments platform owes every authorisation exactly one decision, and a
// downstream service acts on it: a block puts the card on hold, a review
// opens a case, a clear closes the loop. Delivered twice, a block holds the
// card twice and a review opens two cases; delivered never, a fraud
// continues. So the decision stream is where the three delivery tiers a
// store can offer are easiest to see side by side, which is what the rail
// scene does with it: the same stream fans out to an append table
// (at-least-once), an upsert table keyed by the authorisation's id
// (effectively-once: a replay rewrites the same rows) and a two-phase-commit
// table (exactly-once: rows appear when their checkpoint completes).
//
// The decision itself is deliberately simple and deterministic: the card's
// declines within a window of event time, scored as the served profile
// scores a lifetime (risk_profile.hpp), plus the last fix for an
// impossible-travel check. The contract under test is the delivery, not
// the model. `decided_at` is the
// operator's wall clock at emission; the scene measures each store's
// visibility latency against it from Postgres commit timestamps.
#include <algorithm>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <optional>
#include <string>

#include <clink/core/codec.hpp>
#include <clink/operators/process_function.hpp>
#include <clink/state/keyed_state.hpp>

#include "events.hpp"
#include "risk_profile.hpp"

namespace cs {

struct Decision {
    std::int64_t id{0};    // the authorisation's event id: the decision's key
    std::int64_t ts{0};    // event time of the authorisation
    std::int64_t card{0};
    double amount{0.0};
    std::string decision;  // "clear" | "review" | "block"
    std::string reason;
    int risk{0};                 // the card's score after this authorisation
    std::int64_t decided_at{0};  // wall clock, ms since epoch, at emission
};

inline std::string decision_to_json(const Decision& d) {
    return "{\"id\":" + std::to_string(d.id) + ",\"ts\":" + std::to_string(d.ts) +
           ",\"card\":" + std::to_string(d.card) + ",\"amount\":" + detail::fmt2(d.amount) +
           ",\"decision\":\"" + d.decision + "\",\"reason\":\"" + d.reason +
           "\",\"risk\":" + std::to_string(d.risk) +
           ",\"decided_at\":" + std::to_string(d.decided_at) + "}";
}

// Codec<Decision>: the decision crosses a network channel between the
// operator and its sinks on the cluster. Same framing as the other types.
namespace detail {

inline void encode_decision_into(const Decision& d, clink::Codec<Decision>::Bytes& out) {
    put_i64(out, d.id);
    put_i64(out, d.ts);
    put_i64(out, d.card);
    put_f64(out, d.amount);
    put_str(out, d.decision);
    put_str(out, d.reason);
    put_i64(out, d.risk);
    put_i64(out, d.decided_at);
}

}  // namespace detail

inline clink::Codec<Decision> decision_codec() {
    return clink::Codec<Decision>{
        .encode =
            [](const Decision& d) {
                clink::Codec<Decision>::Bytes out;
                detail::encode_decision_into(d, out);
                return out;
            },
        .decode = [](clink::Codec<Decision>::BytesView b) -> std::optional<Decision> {
            detail::Cursor c{b};
            Decision d;
            std::int64_t risk = 0;
            if (!c.take_i64(d.id) || !c.take_i64(d.ts) || !c.take_i64(d.card) ||
                !c.take_f64(d.amount) || !c.take_str(d.decision) || !c.take_str(d.reason) ||
                !c.take_i64(risk) || !c.take_i64(d.decided_at) || c.at != b.size()) {
                return std::nullopt;
            }
            d.risk = static_cast<int>(risk);
            return d;
        },
        .encode_into =
            [](const Decision& d, clink::Codec<Decision>::Bytes& out) {
                detail::encode_decision_into(d, out);
            }};
}

// The policy's thresholds ride the job spec as operator params, so every
// process that builds the operator builds the same one. Declines count
// within a window of event time, not for the card's lifetime: a card that
// was probed last month is not a card being probed now.
struct DecisionPolicy {
    std::int64_t window_ms{3'600'000};  // event time over which declines count
    int review_at{50};                  // two declines in the window review an approval
    int block_at{100};                  // four block it
    double review_amount{400.0};        // a card-not-present approval this large is reviewed
    double max_speed_kmh{900.0};        // faster than this from the last fix is impossible travel
    double min_jump_km{100.0};          // below this the speed check does not apply
};

// Per-card state: the decline window and the last authorisation's position.
struct CardWindow {
    std::int64_t window_start{0};  // event time of the window's first decline
    std::int64_t declines{0};      // declines since window_start
    std::int64_t last_ts{0};       // last authorisation's event time, 0 = none yet
    double lat{0.0};
    double lon{0.0};
};

inline clink::Codec<CardWindow> card_window_codec() {
    return clink::Codec<CardWindow>{
        .encode =
            [](const CardWindow& w) {
                clink::Codec<CardWindow>::Bytes out;
                detail::put_i64(out, w.window_start);
                detail::put_i64(out, w.declines);
                detail::put_i64(out, w.last_ts);
                detail::put_f64(out, w.lat);
                detail::put_f64(out, w.lon);
                return out;
            },
        .decode = [](clink::Codec<CardWindow>::BytesView b) -> std::optional<CardWindow> {
            detail::Cursor c{b};
            CardWindow w;
            if (!c.take_i64(w.window_start) || !c.take_i64(w.declines) || !c.take_i64(w.last_ts) ||
                !c.take_f64(w.lat) || !c.take_f64(w.lon) || c.at != b.size()) {
                return std::nullopt;
            }
            return w;
        }};
}

// The windowed score: declines dominate, one large ticket adds, as in the
// served profile's lifetime score (risk_profile.hpp) but over the window.
inline int window_score(const CardWindow& w, double amount) {
    const auto declines = static_cast<int>(std::min<std::int64_t>(w.declines, 4));
    return std::min(100, declines * 25 + (amount >= 400.0 ? 15 : 0));
}

inline constexpr const char* kCardWindowSlot = "card_window";

class DecisionFn final : public clink::KeyedProcessFunction<std::int64_t, Event, Decision> {
public:
    explicit DecisionFn(DecisionPolicy policy = {}) : policy_(policy) {}

    void open(clink::RuntimeContext& ctx) override {
        window_.emplace(ctx.keyed_state<std::int64_t, CardWindow>(
            kCardWindowSlot, clink::int64_codec(), card_window_codec()));
    }

    void process_element(const Event& e,
                         clink::ProcessFunctionContext<Decision>& ctx,
                         clink::Collector<Decision>& out) override {
        if (e.type != EventType::Auth) {
            return;
        }
        const auto key = current_key();
        CardWindow w = window_->get(key).value_or(CardWindow{});

        // A decline opens the window or extends its count; a window older
        // than the policy allows is over, whatever it held.
        if (w.declines > 0 && e.ts - w.window_start > policy_.window_ms) {
            w.declines = 0;
            w.window_start = 0;
        }
        if (!e.approved) {
            if (w.declines == 0) {
                w.window_start = e.ts;
            }
            w.declines += 1;
        }
        const int score = window_score(w, e.amount);

        Decision d;
        d.id = e.id;
        d.ts = e.ts;
        d.card = e.card;
        d.amount = e.amount;
        d.risk = score;

        if (!e.approved) {
            d.decision = "clear";
            d.reason = "declined upstream";
        } else if (const auto travel = impossible_travel(w, e); travel.has_value()) {
            d.decision = "block";
            d.reason = *travel;
        } else if (score >= policy_.block_at) {
            d.decision = "block";
            d.reason = "risk " + std::to_string(score) + ": " + std::to_string(w.declines) +
                       " declines in the window";
        } else if (score >= policy_.review_at) {
            d.decision = "review";
            d.reason = "risk " + std::to_string(score) + ": " + std::to_string(w.declines) +
                       " declines in the window";
        } else if (!e.present && e.amount >= policy_.review_amount) {
            d.decision = "review";
            d.reason = "card not present for " + detail::fmt2(e.amount);
        } else {
            d.decision = "clear";
            d.reason = "risk " + std::to_string(score);
        }
        if (w.last_ts == 0 || e.ts >= w.last_ts) {
            w.last_ts = e.ts;
            w.lat = e.lat;
            w.lon = e.lon;
        }
        window_->put(key, w);
        d.decided_at = std::chrono::duration_cast<std::chrono::milliseconds>(
                           std::chrono::system_clock::now().time_since_epoch())
                           .count();

        // Event time rides through, as it does through the guard.
        if (const auto ts = ctx.timestamp(); ts.has_value()) {
            out.collect_with_timestamp(d, *ts);
        } else {
            out.collect(d);
        }
    }

private:
    std::optional<std::string> impossible_travel(const CardWindow& w, const Event& e) const {
        if (w.last_ts == 0 || e.ts <= w.last_ts) {
            return std::nullopt;
        }
        const double km = haversine_km(w.lat, w.lon, e.lat, e.lon);
        if (km < policy_.min_jump_km) {
            return std::nullopt;
        }
        const double hours = static_cast<double>(e.ts - w.last_ts) / 3'600'000.0;
        const double kmh = km / hours;
        if (kmh <= policy_.max_speed_kmh) {
            return std::nullopt;
        }
        return "impossible travel: " + detail::fmt2(km) + " km in " +
               detail::fmt2(hours * 60.0) + " min";
    }

    DecisionPolicy policy_;
    std::optional<clink::KeyedState<std::int64_t, CardWindow>> window_;
};

}  // namespace cs
