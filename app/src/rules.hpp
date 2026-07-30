#pragma once

// The dynamic-rule model for the watchlist detector: rule records parsed
// from data/rules.ndjson, and the RuleSet the BroadcastProcessFunction
// keeps in broadcast state.
//
// Rules are EFFECTIVE-DATED: `activate_at` is event time, and the detector
// compares it against each event's own timestamp. Alert outcomes therefore
// depend only on event times, never on the race between the rules stream
// and the event stream. The file ends with an end_of_rules marker that
// seals the set; the detector buffers events until it sees it.

#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include <clink/core/codec.hpp>

#include "events.hpp"

namespace cs {

enum class RuleKind : std::uint8_t {
    MerchantWatchlist = 0,
    MerchantCap = 1,
    EndOfRules = 2,
};

struct Rule {
    RuleKind kind{RuleKind::MerchantWatchlist};
    std::string merchant;
    std::int64_t activate_at{0};  // event time; rule applies to events at/after this
    double cap{0.0};              // MerchantCap only
};

// The broadcast-state value: every rule seen so far, plus the sealed flag
// set by the end_of_rules marker.
struct RuleSet {
    std::vector<Rule> rules;
    bool sealed{false};
};

inline std::optional<Rule> parse_rule(std::string_view line) {
    const auto kind = detail::raw_value(line, "kind");
    const auto merchant = detail::raw_value(line, "merchant");
    const auto activate_at = detail::i64_field(line, "activate_at");
    const auto cap = detail::f64_field(line, "cap");
    if (!kind || !merchant || !activate_at || !cap) {
        return std::nullopt;
    }
    Rule r;
    if (*kind == "merchant_watchlist") {
        r.kind = RuleKind::MerchantWatchlist;
    } else if (*kind == "merchant_cap") {
        r.kind = RuleKind::MerchantCap;
    } else if (*kind == "end_of_rules") {
        r.kind = RuleKind::EndOfRules;
    } else {
        return std::nullopt;
    }
    r.merchant = std::string(*merchant);
    r.activate_at = *activate_at;
    r.cap = *cap;
    return r;
}

// Codec<RuleSet>: sealed flag, count, then per rule (kind, activate_at,
// cap, merchant). Broadcast state persists through this at checkpoints.
namespace detail {

inline void encode_rule_set_into(const RuleSet& rs, clink::Codec<RuleSet>::Bytes& out) {
    out.push_back(static_cast<std::byte>(rs.sealed ? 1 : 0));
    const auto count = static_cast<std::uint32_t>(rs.rules.size());
    for (int i = 0; i < 4; ++i) {
        out.push_back(static_cast<std::byte>((count >> (i * 8)) & 0xFF));
    }
    for (const auto& r : rs.rules) {
        out.push_back(static_cast<std::byte>(r.kind));
        put_i64(out, r.activate_at);
        put_f64(out, r.cap);
        put_str(out, r.merchant);
    }
}

}  // namespace detail

inline clink::Codec<RuleSet> rule_set_codec() {
    return clink::Codec<RuleSet>{
        .encode =
            [](const RuleSet& rs) {
                clink::Codec<RuleSet>::Bytes out;
                detail::encode_rule_set_into(rs, out);
                return out;
            },
        .decode = [](clink::Codec<RuleSet>::BytesView b) -> std::optional<RuleSet> {
            detail::Cursor c{b};
            RuleSet rs;
            std::uint8_t sealed = 0;
            if (!c.take_u8(sealed)) {
                return std::nullopt;
            }
            rs.sealed = sealed != 0;
            if (c.at + 4 > b.size()) {
                return std::nullopt;
            }
            std::uint32_t count = 0;
            for (int i = 0; i < 4; ++i) {
                count |= static_cast<std::uint32_t>(static_cast<unsigned char>(b[c.at + i]))
                         << (i * 8);
            }
            c.at += 4;
            rs.rules.reserve(count);
            for (std::uint32_t i = 0; i < count; ++i) {
                Rule r;
                std::uint8_t kind = 0;
                if (!c.take_u8(kind) || !c.take_i64(r.activate_at) || !c.take_f64(r.cap) ||
                    !c.take_str(r.merchant)) {
                    return std::nullopt;
                }
                r.kind = static_cast<RuleKind>(kind);
                rs.rules.push_back(std::move(r));
            }
            if (c.at != b.size()) {
                return std::nullopt;
            }
            return rs;
        },
        .encode_into =
            [](const RuleSet& rs, clink::Codec<RuleSet>::Bytes& out) {
                detail::encode_rule_set_into(rs, out);
            }};
}

}  // namespace cs
