#pragma once

// card-sentry event model: the tape's event type, a strict parser for the
// generator's ndjson lines, a byte codec for the type (the CEP operator
// persists partial matches through it), and the alert type the detectors
// emit.

#include <bit>
#include <cctype>
#include <charconv>
#include <cmath>
#include <cstdint>
#include <cstdlib>
#include <optional>
#include <string>
#include <string_view>

#include <clink/core/codec.hpp>

namespace cs {

enum class EventType : std::uint8_t {
    Auth = 0,
    Transfer = 1,
    LoginFail = 2,
    LoginOk = 3,
    PasswordChange = 4,
    OtpRequest = 5,
    OtpVerify = 6,
    Unknown = 255,
};

struct Event {
    std::int64_t id{0};
    std::int64_t ts{0};  // event time, ms since epoch
    EventType type{EventType::Unknown};
    std::int64_t card{0};     // 0 when not a card event
    std::int64_t account{0};  // 0 when not an account event
    double amount{0.0};
    bool approved{false};
    bool present{false};  // card-present authorisation
    double lat{0.0};
    double lon{0.0};
    std::string merchant;
    std::string country;
};

inline EventType event_type_from(std::string_view s) {
    if (s == "auth") return EventType::Auth;
    if (s == "transfer") return EventType::Transfer;
    if (s == "login_fail") return EventType::LoginFail;
    if (s == "login_ok") return EventType::LoginOk;
    if (s == "password_change") return EventType::PasswordChange;
    if (s == "otp_request") return EventType::OtpRequest;
    if (s == "otp_verify") return EventType::OtpVerify;
    return EventType::Unknown;
}

// ---------------------------------------------------------------------------
// ndjson parsing. The generator writes flat one-line objects with plain
// string values (no escapes), so a key-scan parser is exact for this tape;
// any missing key or malformed number yields nullopt and the record is
// dropped upstream of the detectors.
// ---------------------------------------------------------------------------

namespace detail {

inline std::optional<std::string_view> raw_value(std::string_view line, std::string_view key) {
    // Finds `"key":` and returns the value slice up to the next ',' or '}'
    // (strings return the slice inside their quotes).
    const std::string needle = "\"" + std::string(key) + "\":";
    const auto at = line.find(needle);
    if (at == std::string_view::npos) {
        return std::nullopt;
    }
    auto rest = line.substr(at + needle.size());
    if (rest.empty()) {
        return std::nullopt;
    }
    if (rest.front() == '"') {
        rest.remove_prefix(1);
        const auto end = rest.find('"');
        if (end == std::string_view::npos) {
            return std::nullopt;
        }
        return rest.substr(0, end);
    }
    const auto end = rest.find_first_of(",}");
    if (end == std::string_view::npos) {
        return std::nullopt;
    }
    return rest.substr(0, end);
}

inline std::optional<std::int64_t> i64_field(std::string_view line, std::string_view key) {
    const auto raw = raw_value(line, key);
    if (!raw.has_value()) {
        return std::nullopt;
    }
    std::int64_t v = 0;
    const auto [ptr, ec] = std::from_chars(raw->data(), raw->data() + raw->size(), v);
    if (ec != std::errc{} || ptr != raw->data() + raw->size()) {
        return std::nullopt;
    }
    return v;
}

inline std::optional<double> f64_field(std::string_view line, std::string_view key) {
    const auto raw = raw_value(line, key);
    if (!raw.has_value() || raw->empty()) {
        return std::nullopt;
    }
    // strtod over from_chars<double>: portable across standard libraries.
    std::string buf(*raw);
    char* end = nullptr;
    const double v = std::strtod(buf.c_str(), &end);
    if (end != buf.c_str() + buf.size()) {
        return std::nullopt;
    }
    return v;
}

}  // namespace detail

inline std::optional<Event> parse_event(std::string_view line) {
    Event e;
    const auto id = detail::i64_field(line, "id");
    const auto ts = detail::i64_field(line, "ts");
    const auto type = detail::raw_value(line, "type");
    const auto card = detail::i64_field(line, "card");
    const auto account = detail::i64_field(line, "account");
    const auto amount = detail::f64_field(line, "amount");
    const auto approved = detail::i64_field(line, "approved");
    const auto present = detail::i64_field(line, "present");
    const auto lat = detail::f64_field(line, "lat");
    const auto lon = detail::f64_field(line, "lon");
    const auto merchant = detail::raw_value(line, "merchant");
    const auto country = detail::raw_value(line, "country");
    if (!id || !ts || !type || !card || !account || !amount || !approved || !present || !lat ||
        !lon || !merchant || !country) {
        return std::nullopt;
    }
    e.id = *id;
    e.ts = *ts;
    e.type = event_type_from(*type);
    if (e.type == EventType::Unknown) {
        return std::nullopt;
    }
    e.card = *card;
    e.account = *account;
    e.amount = *amount;
    e.approved = *approved != 0;
    e.present = *present != 0;
    e.lat = *lat;
    e.lon = *lon;
    e.merchant = std::string(*merchant);
    e.country = std::string(*country);
    return e;
}

// ---------------------------------------------------------------------------
// Codec<Event>: fixed field order, little-endian scalars, length-prefixed
// strings. The CEP operator persists in-flight partial matches through this
// codec, so it must round-trip every field exactly.
// ---------------------------------------------------------------------------

namespace detail {

inline void put_u64(clink::Codec<Event>::Bytes& out, std::uint64_t v) {
    for (int i = 0; i < 8; ++i) {
        out.push_back(static_cast<std::byte>((v >> (i * 8)) & 0xFF));
    }
}

inline void put_i64(clink::Codec<Event>::Bytes& out, std::int64_t v) {
    put_u64(out, static_cast<std::uint64_t>(v));
}

inline void put_f64(clink::Codec<Event>::Bytes& out, double v) {
    put_u64(out, std::bit_cast<std::uint64_t>(v));
}

inline void put_str(clink::Codec<Event>::Bytes& out, const std::string& s) {
    const auto len = static_cast<std::uint32_t>(s.size());
    for (int i = 0; i < 4; ++i) {
        out.push_back(static_cast<std::byte>((len >> (i * 8)) & 0xFF));
    }
    for (const char c : s) {
        out.push_back(static_cast<std::byte>(c));
    }
}

struct Cursor {
    clink::Codec<Event>::BytesView buf;
    std::size_t at{0};

    bool take_u64(std::uint64_t& v) {
        if (at + 8 > buf.size()) {
            return false;
        }
        v = 0;
        for (int i = 0; i < 8; ++i) {
            v |= static_cast<std::uint64_t>(static_cast<unsigned char>(buf[at + i])) << (i * 8);
        }
        at += 8;
        return true;
    }
    bool take_i64(std::int64_t& v) {
        std::uint64_t u = 0;
        if (!take_u64(u)) {
            return false;
        }
        v = static_cast<std::int64_t>(u);
        return true;
    }
    bool take_f64(double& v) {
        std::uint64_t u = 0;
        if (!take_u64(u)) {
            return false;
        }
        v = std::bit_cast<double>(u);
        return true;
    }
    bool take_u8(std::uint8_t& v) {
        if (at + 1 > buf.size()) {
            return false;
        }
        v = static_cast<std::uint8_t>(buf[at]);
        at += 1;
        return true;
    }
    bool take_str(std::string& s) {
        if (at + 4 > buf.size()) {
            return false;
        }
        std::uint32_t len = 0;
        for (int i = 0; i < 4; ++i) {
            len |= static_cast<std::uint32_t>(static_cast<unsigned char>(buf[at + i])) << (i * 8);
        }
        at += 4;
        if (at + len > buf.size()) {
            return false;
        }
        s.assign(reinterpret_cast<const char*>(buf.data() + at), len);
        at += len;
        return true;
    }
};

inline void encode_event_into(const Event& e, clink::Codec<Event>::Bytes& out) {
    put_i64(out, e.id);
    put_i64(out, e.ts);
    out.push_back(static_cast<std::byte>(e.type));
    put_i64(out, e.card);
    put_i64(out, e.account);
    put_f64(out, e.amount);
    out.push_back(static_cast<std::byte>(e.approved ? 1 : 0));
    out.push_back(static_cast<std::byte>(e.present ? 1 : 0));
    put_f64(out, e.lat);
    put_f64(out, e.lon);
    put_str(out, e.merchant);
    put_str(out, e.country);
}

}  // namespace detail

inline clink::Codec<Event> event_codec() {
    return clink::Codec<Event>{
        .encode =
            [](const Event& e) {
                clink::Codec<Event>::Bytes out;
                detail::encode_event_into(e, out);
                return out;
            },
        .decode = [](clink::Codec<Event>::BytesView b) -> std::optional<Event> {
            detail::Cursor c{b};
            Event e;
            std::uint8_t type = 0;
            std::uint8_t approved = 0;
            std::uint8_t present = 0;
            if (!c.take_i64(e.id) || !c.take_i64(e.ts) || !c.take_u8(type) ||
                !c.take_i64(e.card) || !c.take_i64(e.account) || !c.take_f64(e.amount) ||
                !c.take_u8(approved) || !c.take_u8(present) || !c.take_f64(e.lat) ||
                !c.take_f64(e.lon) || !c.take_str(e.merchant) || !c.take_str(e.country)) {
                return std::nullopt;
            }
            if (c.at != b.size()) {
                return std::nullopt;
            }
            e.type = static_cast<EventType>(type);
            e.approved = approved != 0;
            e.present = present != 0;
            return e;
        },
        .encode_into =
            [](const Event& e, clink::Codec<Event>::Bytes& out) {
                detail::encode_event_into(e, out);
            }};
}

// ---------------------------------------------------------------------------
// Geometry: great-circle distance for the impossible-travel detector.
// ---------------------------------------------------------------------------

inline double haversine_km(double lat1, double lon1, double lat2, double lon2) {
    constexpr double kEarthRadiusKm = 6371.0;
    constexpr double kDegToRad = 3.14159265358979323846 / 180.0;
    const double dlat = (lat2 - lat1) * kDegToRad;
    const double dlon = (lon2 - lon1) * kDegToRad;
    const double a = std::sin(dlat / 2) * std::sin(dlat / 2) +
                     std::cos(lat1 * kDegToRad) * std::cos(lat2 * kDegToRad) *
                         std::sin(dlon / 2) * std::sin(dlon / 2);
    return 2.0 * kEarthRadiusKm * std::asin(std::sqrt(std::min(1.0, a)));
}

// ---------------------------------------------------------------------------
// Alerts.
// ---------------------------------------------------------------------------

struct Alert {
    std::string pattern;
    std::string entity_kind;  // "card" | "account"
    std::int64_t entity_id{0};
    std::int64_t ts{0};  // event time of the triggering evidence
    std::string detail;
};

inline std::string alert_to_json(const Alert& a) {
    std::string out = "{\"pattern\":\"" + a.pattern + "\",\"entity_kind\":\"" + a.entity_kind +
                      "\",\"entity_id\":" + std::to_string(a.entity_id) +
                      ",\"ts\":" + std::to_string(a.ts) + ",\"detail\":\"" + a.detail + "\"}";
    return out;
}

}  // namespace cs
