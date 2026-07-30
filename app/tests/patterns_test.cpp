// Tests for the five card-sentry detectors, written against clink's public
// testing framework: OneInputOperatorHarness drives each CepOperator through
// its production hooks with explicit event times and watermarks, so match,
// non-match, negation, timeout and skip behaviour are all pinned
// deterministically. Plain main(), non-zero exit on failure - no test
// framework dependency.

#include <cstdint>
#include <iostream>
#include <string>
#include <vector>

#include <clink/test/one_input_harness.hpp>

#include "events.hpp"
#include "patterns.hpp"

namespace {

int g_failures = 0;

void check(bool ok, const char* what, int line) {
    if (!ok) {
        std::cerr << "FAIL (line " << line << "): " << what << "\n";
        ++g_failures;
    }
}
#define CHECK(cond) check((cond), #cond, __LINE__)

using Harness = clink::test::OneInputOperatorHarness<cs::Event, cs::Alert>;

constexpr std::int64_t T0 = 1'753'920'000'000;  // matches the tape's epoch

cs::Event auth(std::int64_t card, double amount, bool approved, std::int64_t ts,
               double lat = 51.5074, double lon = -0.1278, bool present = false) {
    cs::Event e;
    e.id = ts % 1'000'000;
    e.ts = ts;
    e.type = cs::EventType::Auth;
    e.card = card;
    e.amount = amount;
    e.approved = approved;
    e.present = present;
    e.lat = lat;
    e.lon = lon;
    e.merchant = "m";
    e.country = "GB";
    return e;
}

cs::Event acct(cs::EventType type, std::int64_t account, std::int64_t ts, double amount = 0.0) {
    cs::Event e;
    e.id = ts % 1'000'000;
    e.ts = ts;
    e.type = type;
    e.account = account;
    e.amount = amount;
    return e;
}

// ---------------------------------------------------------------------------

void test_event_codec_round_trip() {
    const auto codec = cs::event_codec();
    cs::Event e = auth(9001, 486.31, true, T0 + 123, 1.3521, 103.8198, true);
    e.account = 17;
    e.merchant = "harbour-dutyfree";
    e.country = "SG";
    const auto bytes = codec.encode(e);
    const auto back = codec.decode(bytes);
    CHECK(back.has_value());
    CHECK(back->id == e.id && back->ts == e.ts && back->type == e.type);
    CHECK(back->card == e.card && back->account == e.account);
    CHECK(back->amount == e.amount && back->approved == e.approved);
    CHECK(back->present == e.present && back->lat == e.lat && back->lon == e.lon);
    CHECK(back->merchant == e.merchant && back->country == e.country);

    clink::Codec<cs::Event>::Bytes appended;
    codec.encode_into(e, appended);
    CHECK(appended == bytes);
}

void test_card_testing_fires_once_and_needs_min_probes() {
    // Six probes then a strike: exactly one alert (skip_past_last_event
    // collapses the suffix partials that also reached the minimum).
    {
        auto h = Harness::create(cs::make_card_testing());
        h.open();
        std::int64_t t = T0;
        for (int i = 0; i < 6; ++i) {
            h.process_element(auth(9001, 0.5 + 0.2 * i, false, t), t);
            t += 60'000;
        }
        h.process_element(auth(9001, 486.31, true, t), t);
        const auto alerts = h.output().values();
        CHECK(alerts.size() == 1);
        if (!alerts.empty()) {
            CHECK(alerts[0].pattern == "card_testing");
            CHECK(alerts[0].entity_id == 9001);
            CHECK(alerts[0].ts == t);
        }
    }
    // Three probes are below the quantifier minimum: no alert.
    {
        auto h = Harness::create(cs::make_card_testing());
        h.open();
        std::int64_t t = T0;
        for (int i = 0; i < 3; ++i) {
            h.process_element(auth(9001, 1.0, false, t), t);
            t += 60'000;
        }
        h.process_element(auth(9001, 486.31, true, t), t);
        CHECK(h.output().values().empty());
    }
    // A modest approved purchase after probes is not a strike.
    {
        auto h = Harness::create(cs::make_card_testing());
        h.open();
        std::int64_t t = T0;
        for (int i = 0; i < 6; ++i) {
            h.process_element(auth(9001, 1.0, false, t), t);
            t += 60'000;
        }
        h.process_element(auth(9001, 30.0, true, t), t);
        CHECK(h.output().values().empty());
    }
    // Probes spread wider than the window never assemble.
    {
        auto h = Harness::create(cs::make_card_testing());
        h.open();
        std::int64_t t = T0;
        for (int i = 0; i < 6; ++i) {
            h.process_element(auth(9001, 1.0, false, t), t);
            h.process_watermark(t);  // advance event time between probes
            t += 4 * 60'000;         // 4 min apart: any 4 span > 10 min
        }
        h.process_element(auth(9001, 486.31, true, t), t);
        CHECK(h.output().values().empty());
    }
}

void test_impossible_travel_speed_gate() {
    // London then Singapore 45 minutes later: fires.
    {
        auto h = Harness::create(cs::make_impossible_travel());
        h.open();
        h.process_element(auth(9101, 60, true, T0, 51.5074, -0.1278, true), T0);
        const std::int64_t t2 = T0 + 45 * 60'000;
        h.process_element(auth(9101, 85, true, t2, 1.3521, 103.8198, true), t2);
        const auto alerts = h.output().values();
        CHECK(alerts.size() == 1);
        if (!alerts.empty()) {
            CHECK(alerts[0].pattern == "impossible_travel");
            CHECK(alerts[0].entity_id == 9101);
        }
    }
    // London then Paris 50 minutes later (~344 km, ~413 km/h): plausible.
    {
        auto h = Harness::create(cs::make_impossible_travel());
        h.open();
        h.process_element(auth(9103, 60, true, T0, 51.5074, -0.1278, true), T0);
        const std::int64_t t2 = T0 + 50 * 60'000;
        h.process_element(auth(9103, 85, true, t2, 48.8566, 2.3522, true), t2);
        CHECK(h.output().values().empty());
    }
    // Card-not-present auths never anchor or complete travel pairs.
    {
        auto h = Harness::create(cs::make_impossible_travel());
        h.open();
        h.process_element(auth(9103, 60, true, T0, 51.5074, -0.1278, false), T0);
        const std::int64_t t2 = T0 + 45 * 60'000;
        h.process_element(auth(9103, 85, true, t2, 1.3521, 103.8198, false), t2);
        CHECK(h.output().values().empty());
    }
}

void test_account_takeover_negation_zones() {
    // Fails -> password change -> drain, no OTP: fires once.
    {
        auto h = Harness::create(cs::make_account_takeover());
        h.open();
        std::int64_t t = T0;
        for (int i = 0; i < 5; ++i) {
            h.process_element(acct(cs::EventType::LoginFail, 8001, t), t);
            t += 60'000;
        }
        t += 120'000;
        h.process_element(acct(cs::EventType::PasswordChange, 8001, t), t);
        t += 180'000;
        h.process_element(acct(cs::EventType::Transfer, 8001, t, 2'400.0), t);
        const auto alerts = h.output().values();
        CHECK(alerts.size() == 1);
        if (!alerts.empty()) {
            CHECK(alerts[0].pattern == "account_takeover");
            CHECK(alerts[0].entity_id == 8001);
        }
    }
    // An OTP verify between the fails and the password change kills it:
    // that is a verified recovery, not a takeover.
    {
        auto h = Harness::create(cs::make_account_takeover());
        h.open();
        std::int64_t t = T0;
        for (int i = 0; i < 4; ++i) {
            h.process_element(acct(cs::EventType::LoginFail, 8002, t), t);
            t += 60'000;
        }
        h.process_element(acct(cs::EventType::OtpVerify, 8002, t), t);
        t += 60'000;
        h.process_element(acct(cs::EventType::PasswordChange, 8002, t), t);
        t += 120'000;
        h.process_element(acct(cs::EventType::Transfer, 8002, t, 2'900.0), t);
        CHECK(h.output().values().empty());
    }
    // An OTP verify between the password change and the transfer kills it.
    {
        auto h = Harness::create(cs::make_account_takeover());
        h.open();
        std::int64_t t = T0;
        for (int i = 0; i < 4; ++i) {
            h.process_element(acct(cs::EventType::LoginFail, 8003, t), t);
            t += 60'000;
        }
        t += 60'000;
        h.process_element(acct(cs::EventType::PasswordChange, 8003, t), t);
        t += 30'000;
        h.process_element(acct(cs::EventType::OtpVerify, 8003, t), t);
        t += 60'000;
        h.process_element(acct(cs::EventType::Transfer, 8003, t, 2'900.0), t);
        CHECK(h.output().values().empty());
    }
    // Two failures are below the quantifier minimum: never assembles.
    {
        auto h = Harness::create(cs::make_account_takeover());
        h.open();
        std::int64_t t = T0;
        for (int i = 0; i < 2; ++i) {
            h.process_element(acct(cs::EventType::LoginFail, 8004, t), t);
            t += 60'000;
        }
        h.process_element(acct(cs::EventType::PasswordChange, 8004, t), t);
        t += 60'000;
        h.process_element(acct(cs::EventType::Transfer, 8004, t, 2'900.0), t);
        CHECK(h.output().values().empty());
    }
}

void test_otp_timeout_rides_the_side_output() {
    const auto tag = cs::otp_timed_out_tag();
    // Request never verified: the alert appears on the timed-out side
    // output once the watermark passes request + window; main stays empty.
    {
        auto h = Harness::create(cs::make_otp_never_verified());
        h.register_side_output(tag);
        h.open();
        h.process_element(acct(cs::EventType::OtpRequest, 8101, T0), T0);
        h.process_watermark(T0 + 6 * 60'000);  // past the 5-minute window
        CHECK(h.output().values().empty());
        const auto timed_out = h.side_output_values(tag);
        CHECK(timed_out.size() == 1);
        if (!timed_out.empty()) {
            CHECK(timed_out[0].pattern == "otp_never_verified");
            CHECK(timed_out[0].entity_id == 8101);
            CHECK(timed_out[0].ts == T0);
        }
    }
    // Verified inside the window (even close to its edge): healthy
    // completion on main, nothing on the side output.
    {
        auto h = Harness::create(cs::make_otp_never_verified());
        h.register_side_output(tag);
        h.open();
        h.process_element(acct(cs::EventType::OtpRequest, 8104, T0), T0);
        const std::int64_t t2 = T0 + 290'000;
        h.process_element(acct(cs::EventType::OtpVerify, 8104, t2), t2);
        h.process_watermark(T0 + 30 * 60'000);
        CHECK(h.output().values().size() == 1);  // the discarded-healthy record
        CHECK(h.side_output_values(tag).empty());
    }
    // One lost request among healthy pairs on the same account: exactly
    // one timeout, attributed to the lost request's timestamp.
    {
        auto h = Harness::create(cs::make_otp_never_verified());
        h.register_side_output(tag);
        h.open();
        h.process_element(acct(cs::EventType::OtpRequest, 8101, T0), T0);
        const std::int64_t t2 = T0 + 40 * 60'000;
        h.process_element(acct(cs::EventType::OtpRequest, 8101, t2), t2);
        h.process_element(acct(cs::EventType::OtpVerify, 8101, t2 + 45'000), t2 + 45'000);
        h.process_watermark(t2 + 60 * 60'000);
        const auto timed_out = h.side_output_values(tag);
        CHECK(timed_out.size() == 1);
        if (!timed_out.empty()) {
            CHECK(timed_out[0].ts == T0);
        }
    }
}

void test_structuring_running_total_trips() {
    // 850 + 920 + 880 captured below the threshold; 940 crosses: one alert.
    {
        auto h = Harness::create(cs::make_structuring());
        h.open();
        std::int64_t t = T0;
        for (const double amount : {850.0, 920.0, 880.0}) {
            h.process_element(acct(cs::EventType::Transfer, 8201, t, amount), t);
            t += 2 * 3'600'000;
        }
        h.process_element(acct(cs::EventType::Transfer, 8201, t, 940.0), t);
        const auto alerts = h.output().values();
        CHECK(alerts.size() == 1);
        if (!alerts.empty()) {
            CHECK(alerts[0].pattern == "structuring");
            CHECK(alerts[0].entity_id == 8201);
            CHECK(alerts[0].detail.find("3590") != std::string::npos);
        }
    }
    // Two band transfers cannot reach the cumulative threshold.
    {
        auto h = Harness::create(cs::make_structuring());
        h.open();
        h.process_element(acct(cs::EventType::Transfer, 8202, T0, 870.0), T0);
        h.process_element(acct(cs::EventType::Transfer, 8202, T0 + 3'600'000, 910.0),
                          T0 + 3'600'000);
        h.process_watermark(T0 + 48 * 3'600'000);
        CHECK(h.output().values().empty());
    }
    // Out-of-band transfers never capture, whatever they sum to.
    {
        auto h = Harness::create(cs::make_structuring());
        h.open();
        std::int64_t t = T0;
        for (const double amount : {1'500.0, 2'000.0, 1'800.0, 2'200.0}) {
            h.process_element(acct(cs::EventType::Transfer, 8203, t, amount), t);
            t += 3'600'000;
        }
        CHECK(h.output().values().empty());
    }
    // Band transfers spread beyond the 24 h window never assemble: the
    // watermark evicts each partial before the next transfer arrives.
    {
        auto h = Harness::create(cs::make_structuring());
        h.open();
        std::int64_t t = T0;
        for (const double amount : {850.0, 920.0, 880.0, 940.0}) {
            h.process_element(acct(cs::EventType::Transfer, 8204, t, amount), t);
            h.process_watermark(t);
            t += 26 * 3'600'000;
        }
        CHECK(h.output().values().empty());
    }
}

}  // namespace

int main() {
    test_event_codec_round_trip();
    test_card_testing_fires_once_and_needs_min_probes();
    test_impossible_travel_speed_gate();
    test_account_takeover_negation_zones();
    test_otp_timeout_rides_the_side_output();
    test_structuring_running_total_trips();

    if (g_failures != 0) {
        std::cerr << g_failures << " check(s) failed\n";
        return 1;
    }
    std::cout << "all pattern tests passed\n";
    return 0;
}
