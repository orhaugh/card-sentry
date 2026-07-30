#pragma once

// Schema evolution of the per-card risk profile: v1 (app/src/risk_profile.hpp,
// six fields) gains a seventh, first_ts (the first-seen auth timestamp), in
// v2. This is the change every long-lived stateful job eventually makes -
// add a field to state that is already persisted in savepoints - and the
// thing that breaks a naive restore: the old bytes have no first_ts.
//
// clink's answer is a registered migration keyed on the state type's name
// plus a version bump. This header supplies:
//   * RiskProfileV2  - the evolved value + its codec
//   * SchemaVersionTrait<RiskProfile>=1, <RiskProfileV2>=2  - the versions
//   * register_risk_profile_migration()  - the 1->2 migration function
//
// The migration backfills first_ts from last_ts: v1 never recorded a
// first-seen time, so the safest non-fabricating default is "the only
// timestamp we have". A real migration documents such choices; this one
// does so here.

#include <cstdint>
#include <span>
#include <string>
#include <vector>

#include <clink/core/codec.hpp>
#include <clink/state/schema_version.hpp>

#include "risk_profile.hpp"

namespace cs {

struct RiskProfileV2 {
    std::int64_t auths{0};
    std::int64_t declines{0};
    double total{0.0};
    double max_amount{0.0};
    std::int64_t last_ts{0};
    std::string last_country;
    std::int64_t first_ts{0};  // NEW in v2
};

inline clink::Codec<RiskProfileV2> profile_v2_codec() {
    return clink::Codec<RiskProfileV2>{
        .encode =
            [](const RiskProfileV2& p) {
                clink::Codec<RiskProfileV2>::Bytes out;
                detail::put_i64(out, p.auths);
                detail::put_i64(out, p.declines);
                detail::put_f64(out, p.total);
                detail::put_f64(out, p.max_amount);
                detail::put_i64(out, p.last_ts);
                detail::put_str(out, p.last_country);
                detail::put_i64(out, p.first_ts);
                return out;
            },
        .decode = [](clink::Codec<RiskProfileV2>::BytesView b) -> std::optional<RiskProfileV2> {
            detail::Cursor c{b};
            RiskProfileV2 p;
            if (!c.take_i64(p.auths) || !c.take_i64(p.declines) || !c.take_f64(p.total) ||
                !c.take_f64(p.max_amount) || !c.take_i64(p.last_ts) ||
                !c.take_str(p.last_country) || !c.take_i64(p.first_ts) || c.at != b.size()) {
                return std::nullopt;
            }
            return p;
        }};
}

// The state-type tag the migration + version map key on. Convention:
// the qualified "{op_uid}.{slot}" form so one operator's slot is
// addressable independently.
inline constexpr const char* kRiskProfileStateType = "cs-risk-profile.profile";

// Register the v1 -> v2 migration in the given registry (the process
// global by default). Idempotent per (type, from) - a second call
// replaces the edge.
inline void register_risk_profile_migration(
    clink::StateMigrationRegistry& reg = clink::StateMigrationRegistry::global()) {
    reg.register_migration(
        kRiskProfileStateType, /*from=*/1, /*to=*/2,
        [](std::span<const std::byte> in) -> std::vector<std::byte> {
            const auto v1 = profile_codec().decode(in);
            if (!v1.has_value()) {
                throw std::runtime_error("risk-profile migration 1->2: undecodable v1 state");
            }
            RiskProfileV2 v2;
            v2.auths = v1->auths;
            v2.declines = v1->declines;
            v2.total = v1->total;
            v2.max_amount = v1->max_amount;
            v2.last_ts = v1->last_ts;
            v2.last_country = v1->last_country;
            // Backfill: v1 never recorded a first-seen time; the only
            // timestamp we hold is last_ts, so use it (documented choice,
            // not a fabricated value).
            v2.first_ts = v1->last_ts;
            return profile_v2_codec().encode(v2);
        });
}

}  // namespace cs

// Version traits: the persisted risk profile is v1; the evolved value is v2.
// check_restore_compatibility keys on these against a savepoint's stamp.
namespace clink {
template <>
struct SchemaVersionTrait<cs::RiskProfile> {
    static constexpr std::uint32_t value = 1;
};
template <>
struct SchemaVersionTrait<cs::RiskProfileV2> {
    static constexpr std::uint32_t value = 2;
};
}  // namespace clink
