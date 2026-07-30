#pragma once

// The five card-sentry detectors, each a clink::cep::Pattern over the tape's
// Event type wrapped in a ready-to-wire CepOperator<Event, Alert>. Thresholds
// mirror tools/csgen.py, which steers noise well clear of every decision
// boundary so the manifest's expected alerts are exact.
//
// Two constructions worth reading closely:
//
//   * otp_never_verified emits on the TIMED-OUT side output: the pattern
//     describes the healthy flow (request then verify within the window) and
//     the alert is the partial that never completed. The main output is the
//     healthy completion and is discarded by the pipeline.
//
//   * structuring gives the quantified step and its follow-up step
//     complementary iterative predicates (running band total below the
//     trip threshold captures; the transfer that crosses it trips). The two
//     predicates partition the event space, so the default greedy quantifier
//     can never swallow the tripping transfer into the repetition step.

#include <chrono>
#include <cstdint>
#include <cstdio>
#include <memory>
#include <string>

#include <clink/cep/cep_operator.hpp>
#include <clink/cep/pattern.hpp>
#include <clink/runtime/output_tag.hpp>

#include "events.hpp"

namespace cs {

// Detection thresholds (mirrored in tools/csgen.py).
inline constexpr double kProbeMax = 2.00;
inline constexpr double kStrikeMin = 250.00;
inline constexpr double kStrikeVsProbe = 20.0;  // strike >= this x the largest probe
inline constexpr double kTravelKmh = 900.0;
inline constexpr double kTravelMinKm = 200.0;  // ignore in-city jitter outright
inline constexpr double kDrainMin = 1500.00;
inline constexpr double kStructLo = 800.00;
inline constexpr double kStructHi = 1000.00;
inline constexpr double kStructTotal = 3000.00;
inline constexpr std::chrono::minutes kCardTestingWindow{10};
inline constexpr std::chrono::hours kTravelWindow{6};
inline constexpr std::chrono::minutes kTakeoverWindow{30};
inline constexpr std::chrono::minutes kOtpWindow{5};
inline constexpr std::chrono::hours kStructWindow{24};

using CepOp = clink::cep::CepOperator<Event, Alert>;
using Match = clink::cep::PatternMatch<Event>;

namespace detail {

inline std::string fmt2(double v) {
    char buf[32];
    std::snprintf(buf, sizeof(buf), "%.2f", v);
    return buf;
}

inline const Event* first_of(const Match& m, const char* step) {
    const auto it = m.find(step);
    if (it == m.end() || it->second.empty()) {
        return nullptr;
    }
    return &it->second.front();
}

inline const Event* last_of(const Match& m, const char* step) {
    const auto it = m.find(step);
    if (it == m.end() || it->second.empty()) {
        return nullptr;
    }
    return &it->second.back();
}

inline double band_total(const Match& m) {
    const auto it = m.find("t");
    if (it == m.end()) {
        return 0.0;
    }
    double sum = 0.0;
    for (const auto& e : it->second) {
        sum += e.amount;
    }
    return sum;
}

inline bool in_band(const Event& e) {
    return e.type == EventType::Transfer && e.amount >= kStructLo && e.amount < kStructHi;
}

}  // namespace detail

// --- card_testing -----------------------------------------------------------
// A burst of small declined authorisations (the fraudster validating a stolen
// number) followed by one large approved purchase. Quantified begin step plus
// an iterative strike predicate sized against the captured probes.
inline std::shared_ptr<CepOp> make_card_testing() {
    using clink::cep::IterativePredicate;
    auto p = clink::cep::Pattern<Event>::begin("probe")
                 .where([](const Event& e) {
                     return e.type == EventType::Auth && !e.approved && e.amount <= kProbeMax;
                 })
                 .times(4, 20)
                 .followed_by("strike")
                 .where(IterativePredicate<Event>{[](const Event& e, const Match& m) {
                     if (e.type != EventType::Auth || !e.approved || e.amount < kStrikeMin) {
                         return false;
                     }
                     const auto it = m.find("probe");
                     if (it == m.end() || it->second.empty()) {
                         return false;
                     }
                     double largest = 0.0;
                     for (const auto& probe : it->second) {
                         largest = std::max(largest, probe.amount);
                     }
                     return e.amount >= kStrikeVsProbe * largest;
                 }})
                 .within(std::chrono::milliseconds(kCardTestingWindow))
                 .after_match_skip(clink::cep::SkipStrategy::skip_past_last_event());

    auto select = [](const Match& m) -> Alert {
        const auto* strike = detail::first_of(m, "strike");
        const auto it = m.find("probe");
        const std::size_t probes = it == m.end() ? 0 : it->second.size();
        Alert a;
        a.pattern = "card_testing";
        a.entity_kind = "card";
        a.entity_id = strike != nullptr ? strike->card : 0;
        a.ts = strike != nullptr ? strike->ts : 0;
        a.detail = std::to_string(probes) + " probes then " +
                   detail::fmt2(strike != nullptr ? strike->amount : 0.0) + " approved";
        return a;
    };
    auto op = std::make_shared<CepOp>(
        p, event_codec(), [](const Event& e) { return e.card; },
        std::function<Alert(const Match&)>(select), "cs_card_testing");
    op->set_uid("cs-card-testing");
    return op;
}

// --- impossible_travel ------------------------------------------------------
// Two card-present authorisations whose great-circle distance over elapsed
// event time exceeds any plausible speed. The second step's iterative
// predicate reads the anchor authorisation from the partial match.
inline std::shared_ptr<CepOp> make_impossible_travel() {
    using clink::cep::IterativePredicate;
    auto p = clink::cep::Pattern<Event>::begin("a")
                 .where([](const Event& e) {
                     return e.type == EventType::Auth && e.approved && e.present;
                 })
                 .followed_by("b")
                 .where(IterativePredicate<Event>{[](const Event& e, const Match& m) {
                     if (e.type != EventType::Auth || !e.approved || !e.present) {
                         return false;
                     }
                     const auto* a = detail::last_of(m, "a");
                     if (a == nullptr) {
                         return false;
                     }
                     const double km = haversine_km(a->lat, a->lon, e.lat, e.lon);
                     if (km < kTravelMinKm) {
                         return false;
                     }
                     const double hours =
                         static_cast<double>(std::max<std::int64_t>(e.ts - a->ts, 1)) / 3.6e6;
                     return km / hours > kTravelKmh;
                 }})
                 .within(std::chrono::milliseconds(kTravelWindow))
                 .after_match_skip(clink::cep::SkipStrategy::skip_past_last_event());

    auto select = [](const Match& m) -> Alert {
        const auto* a = detail::first_of(m, "a");
        const auto* b = detail::first_of(m, "b");
        Alert out;
        out.pattern = "impossible_travel";
        out.entity_kind = "card";
        out.entity_id = b != nullptr ? b->card : 0;
        out.ts = b != nullptr ? b->ts : 0;
        if (a != nullptr && b != nullptr) {
            const double km = haversine_km(a->lat, a->lon, b->lat, b->lon);
            const double hours =
                static_cast<double>(std::max<std::int64_t>(b->ts - a->ts, 1)) / 3.6e6;
            out.detail = a->country + " to " + b->country + ", " + detail::fmt2(km) + " km at " +
                         detail::fmt2(km / hours) + " km/h";
        }
        return out;
    };
    auto op = std::make_shared<CepOp>(
        p, event_codec(), [](const Event& e) { return e.card; },
        std::function<Alert(const Match&)>(select), "cs_impossible_travel");
    op->set_uid("cs-impossible-travel");
    return op;
}

// --- account_takeover -------------------------------------------------------
// Repeated login failures, then a password change, then a draining transfer,
// with NO successful OTP verification anywhere between the failures and the
// drain. Two negative (not_followed_by) zones express the absence.
inline std::shared_ptr<CepOp> make_account_takeover() {
    auto p = clink::cep::Pattern<Event>::begin("fails")
                 .where([](const Event& e) { return e.type == EventType::LoginFail; })
                 .times(3, 8)
                 .not_followed_by("verify_pre")
                 .where([](const Event& e) { return e.type == EventType::OtpVerify; })
                 .followed_by("pw")
                 .where([](const Event& e) { return e.type == EventType::PasswordChange; })
                 .not_followed_by("verify_post")
                 .where([](const Event& e) { return e.type == EventType::OtpVerify; })
                 .followed_by("drain")
                 .where([](const Event& e) {
                     return e.type == EventType::Transfer && e.amount >= kDrainMin;
                 })
                 .within(std::chrono::milliseconds(kTakeoverWindow))
                 .after_match_skip(clink::cep::SkipStrategy::skip_past_last_event());

    auto select = [](const Match& m) -> Alert {
        const auto* drain = detail::first_of(m, "drain");
        const auto it = m.find("fails");
        const std::size_t fails = it == m.end() ? 0 : it->second.size();
        Alert a;
        a.pattern = "account_takeover";
        a.entity_kind = "account";
        a.entity_id = drain != nullptr ? drain->account : 0;
        a.ts = drain != nullptr ? drain->ts : 0;
        a.detail = std::to_string(fails) + " failed logins, password change, then " +
                   detail::fmt2(drain != nullptr ? drain->amount : 0.0) + " out, no OTP";
        return a;
    };
    auto op = std::make_shared<CepOp>(
        p, event_codec(), [](const Event& e) { return e.account; },
        std::function<Alert(const Match&)>(select), "cs_account_takeover");
    op->set_uid("cs-account-takeover");
    return op;
}

// --- otp_never_verified -----------------------------------------------------
// The pattern describes the HEALTHY flow: a request verified inside the
// window. The alert is the timed-out partial - the request whose verify
// never came - emitted on the side output configured by the caller via
// otp_timed_out_tag(). The completed (healthy) matches are discarded.
inline clink::OutputTag<Alert> otp_timed_out_tag() {
    return clink::OutputTag<Alert>("otp_timed_out");
}

inline std::shared_ptr<CepOp> make_otp_never_verified() {
    auto p = clink::cep::Pattern<Event>::begin("req")
                 .where([](const Event& e) { return e.type == EventType::OtpRequest; })
                 .followed_by("ok")
                 .where([](const Event& e) { return e.type == EventType::OtpVerify; })
                 .within(std::chrono::milliseconds(kOtpWindow));

    auto healthy = [](const Match& m) -> Alert {
        // Completed = verified in time. The pipeline drops these; the value
        // exists only because a select function must produce one.
        const auto* req = detail::first_of(m, "req");
        Alert a;
        a.pattern = "otp_verified_ok";
        a.entity_kind = "account";
        a.entity_id = req != nullptr ? req->account : 0;
        a.ts = req != nullptr ? req->ts : 0;
        return a;
    };
    auto op = std::make_shared<CepOp>(
        p, event_codec(), [](const Event& e) { return e.account; },
        std::function<Alert(const Match&)>(healthy), "cs_otp_never_verified");
    op->set_uid("cs-otp-never-verified");

    auto timed_out = [](const Match& m) -> Alert {
        const auto* req = detail::first_of(m, "req");
        Alert a;
        a.pattern = "otp_never_verified";
        a.entity_kind = "account";
        a.entity_id = req != nullptr ? req->account : 0;
        a.ts = req != nullptr ? req->ts : 0;
        a.detail = "otp requested, never verified within " +
                   std::to_string(kOtpWindow.count()) + " min";
        return a;
    };
    op->with_timed_out_output<Alert>(otp_timed_out_tag(),
                                     std::function<Alert(const Match&)>(timed_out));
    return op;
}

// --- structuring ------------------------------------------------------------
// Repeated transfers just under the reporting band ceiling whose running
// total crosses the trip threshold. The quantified step captures while the
// running total stays below the threshold; the complementary tip step takes
// exactly the transfer that crosses it.
inline std::shared_ptr<CepOp> make_structuring() {
    using clink::cep::IterativePredicate;
    auto p = clink::cep::Pattern<Event>::begin("t")
                 .where(IterativePredicate<Event>{[](const Event& e, const Match& m) {
                     return detail::in_band(e) && detail::band_total(m) + e.amount < kStructTotal;
                 }})
                 .times(2, 12)
                 .followed_by("tip")
                 .where(IterativePredicate<Event>{[](const Event& e, const Match& m) {
                     return detail::in_band(e) && detail::band_total(m) + e.amount >= kStructTotal;
                 }})
                 .within(std::chrono::milliseconds(kStructWindow))
                 .after_match_skip(clink::cep::SkipStrategy::skip_past_last_event());

    auto select = [](const Match& m) -> Alert {
        const auto* tip = detail::first_of(m, "tip");
        const auto it = m.find("t");
        const std::size_t below = it == m.end() ? 0 : it->second.size();
        const double total = detail::band_total(m) + (tip != nullptr ? tip->amount : 0.0);
        Alert a;
        a.pattern = "structuring";
        a.entity_kind = "account";
        a.entity_id = tip != nullptr ? tip->account : 0;
        a.ts = tip != nullptr ? tip->ts : 0;
        a.detail = std::to_string(below + 1) + " sub-threshold transfers totalling " +
                   detail::fmt2(total);
        return a;
    };
    auto op = std::make_shared<CepOp>(
        p, event_codec(), [](const Event& e) { return e.account; },
        std::function<Alert(const Match&)>(select), "cs_structuring");
    op->set_uid("cs-structuring");
    return op;
}

}  // namespace cs
