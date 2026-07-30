#pragma once

// The per-card risk profile: a keyed, stateful operator that folds every
// authorisation into a compact per-card profile, and serves that profile
// over clink's queryable-state HTTP surface while the state is live.
//
// The profile is engine state, not a side table: it rides a KeyedState
// slot (checkpointable, restorable, inspectable by the test harness), and
// external readers reach it through the queryable-state registry - a JSON
// lookup (key in, JSON document out) plus a bounded JSON scan that lets a
// client rank the currently riskiest cards without the engine exporting
// anything.
//
// The score is a deliberately simple, documented illustration - declines
// and a large-ticket marker - not a claim of a risk model.

#include <algorithm>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include <clink/core/codec.hpp>
#include <clink/operators/process_function.hpp>
#include <clink/queryable_state/registry.hpp>
#include <clink/state/keyed_state.hpp>

#include "events.hpp"

namespace cs {

struct RiskProfile {
    std::int64_t auths{0};     // every auth event seen (approved or not)
    std::int64_t declines{0};  // the declined subset
    double total{0.0};         // sum of all auth amounts
    double max_amount{0.0};
    std::int64_t last_ts{0};
    std::string last_country;
};

inline int risk_score(const RiskProfile& p) {
    // Illustrative, deterministic: declines dominate, one large ticket adds.
    const auto declines = static_cast<int>(std::min<std::int64_t>(p.declines, 4));
    return std::min(100, declines * 25 + (p.max_amount >= 400.0 ? 15 : 0));
}

namespace detail {

inline void encode_profile_into(const RiskProfile& p, clink::Codec<RiskProfile>::Bytes& out) {
    put_i64(out, p.auths);
    put_i64(out, p.declines);
    put_f64(out, p.total);
    put_f64(out, p.max_amount);
    put_i64(out, p.last_ts);
    put_str(out, p.last_country);
}

}  // namespace detail

inline clink::Codec<RiskProfile> profile_codec() {
    return clink::Codec<RiskProfile>{
        .encode =
            [](const RiskProfile& p) {
                clink::Codec<RiskProfile>::Bytes out;
                detail::encode_profile_into(p, out);
                return out;
            },
        .decode = [](clink::Codec<RiskProfile>::BytesView b) -> std::optional<RiskProfile> {
            detail::Cursor c{b};
            RiskProfile p;
            if (!c.take_i64(p.auths) || !c.take_i64(p.declines) || !c.take_f64(p.total) ||
                !c.take_f64(p.max_amount) || !c.take_i64(p.last_ts) ||
                !c.take_str(p.last_country) || c.at != b.size()) {
                return std::nullopt;
            }
            return p;
        },
        .encode_into =
            [](const RiskProfile& p, clink::Codec<RiskProfile>::Bytes& out) {
                detail::encode_profile_into(p, out);
            }};
}

inline std::string profile_to_json(std::int64_t card, const RiskProfile& p) {
    return "{\"card\":" + std::to_string(card) + ",\"auths\":" + std::to_string(p.auths) +
           ",\"declines\":" + std::to_string(p.declines) + ",\"total\":" + detail::fmt2(p.total) +
           ",\"max_amount\":" + detail::fmt2(p.max_amount) +
           ",\"last_ts\":" + std::to_string(p.last_ts) + ",\"last_country\":\"" + p.last_country +
           "\",\"score\":" + std::to_string(risk_score(p)) + "}";
}

// Queryable-state addressing: the app serves under role "cs", subtask 0,
// slot "risk_profile" - i.e. GET
//   /api/v1/queryable_state/op/cs/subtask/0/json/risk_profile?key=<card>
inline constexpr const char* kProfileRole = "cs";
inline constexpr const char* kProfileSlot = "risk_profile";

class RiskProfileFn final : public clink::KeyedProcessFunction<std::int64_t, Event, Alert> {
public:
    // With a registry the profile becomes externally queryable; without
    // one (tests) it is plain keyed state.
    explicit RiskProfileFn(clink::queryable_state::Registry* registry = nullptr)
        : registry_(registry) {}

    void open(clink::RuntimeContext& ctx) override {
        profile_.emplace(ctx.keyed_state<std::int64_t, RiskProfile>(
            "profile", clink::int64_codec(), profile_codec()));
        if (registry_ == nullptr) {
            return;
        }
        auto state = std::make_shared<clink::KeyedState<std::int64_t, RiskProfile>>(*profile_);
        const auto slot = clink::queryable_state::compose_subtask_slot(kProfileRole, 0,
                                                                       kProfileSlot);
        registry_->register_json_slot(
            slot, [state](const std::string& key) -> std::optional<std::string> {
                std::int64_t card = 0;
                try {
                    card = std::stoll(key);
                } catch (...) {
                    return std::nullopt;
                }
                const auto p = state->get(card);
                if (!p.has_value()) {
                    return std::nullopt;
                }
                return profile_to_json(card, *p);
            });
        registry_->register_json_scan(
            slot, [state](std::size_t limit) -> clink::queryable_state::JsonScanResult {
                clink::queryable_state::JsonScanResult out;
                state->scan([&](std::int64_t card, const RiskProfile& p) {
                    if (out.entries.size() < limit) {
                        out.entries.emplace_back(std::to_string(card),
                                                 profile_to_json(card, p));
                    } else {
                        out.truncated = true;
                    }
                });
                return out;
            });
    }

    void process_element(const Event& e,
                         clink::ProcessFunctionContext<Alert>& /*ctx*/,
                         clink::Collector<Alert>& /*out*/) override {
        if (e.type != EventType::Auth) {
            return;
        }
        const auto key = current_key();
        RiskProfile p = profile_->get(key).value_or(RiskProfile{});
        p.auths += 1;
        if (!e.approved) {
            p.declines += 1;
        }
        p.total += e.amount;
        p.max_amount = std::max(p.max_amount, e.amount);
        if (e.ts >= p.last_ts) {
            p.last_ts = e.ts;
            p.last_country = e.country;
        }
        profile_->put(key, p);
    }

private:
    clink::queryable_state::Registry* registry_;
    std::optional<clink::KeyedState<std::int64_t, RiskProfile>> profile_;
};

}  // namespace cs
