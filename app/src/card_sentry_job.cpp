// card-sentry as a compiled clink job plugin (.so).
//
// The same five CEP detectors as the embedded app, expressed on the fluent
// Pipeline API and packaged with CLINK_REGISTER_JOB. The pattern builders
// and alert selectors in patterns.hpp are shared with the embedded
// pipeline, so the two deployments cannot drift.
//
// Deployment model: the submit tool dlopens this .so to retrieve the
// JobGraphSpec; the coordinator ships the .so to every worker, which
// dlopens it so the inline-op registrations resolve there too. Inline CEP
// lambdas are in-process-only on the plain fluent path - packaging them
// as this plugin is what makes them cluster-runnable.
//
// The OTP detector uses the fluent timed-out surface: its ALERT stream is
// the timed-out side output (requests whose verify never arrived);
// completed verifications drain to a diagnostics file the gate ignores.
//
// Configuration is read from the SUBMITTER's environment at build time and
// baked into the graph spec:
//   CS_EVENTS   input tape path as workers see it (default /data/events.ndjson)
//   CS_OUT_DIR  alert output directory as workers see it (default /out)

#include <chrono>
#include <cstdlib>
#include <string>
#include <vector>

#include <clink/api/builtin_connectors.hpp>
#include <clink/api/pipeline.hpp>
#include <clink/cep/cep.hpp>
#include <clink/job/register_job.hpp>
#include <clink/runtime/output_tag.hpp>
#include <clink/time/event_time.hpp>

#include "events.hpp"
#include "patterns.hpp"

namespace {

std::string env_or(const char* key, const char* fallback) {
    if (const char* v = std::getenv(key); v != nullptr && *v != '\0') {
        return v;
    }
    return fallback;
}

void define_job(clink::api::Pipeline& env) {
    using cs::Alert;
    using cs::Event;
    using cs::EventType;

    env.registry().register_type<Event>("cs_event", cs::event_codec());
    env.registry().register_type<Alert>("cs_alert", cs::alert_codec());

    const std::string in = env_or("CS_EVENTS", "/data/events.ndjson");
    const std::string out_dir = env_or("CS_OUT_DIR", "/out");

    auto events =
        env.source<std::string>(clink::api::FileTextSource::builder().path(in).build())
            .flat_map<Event>([](const std::string& line) {
                std::vector<Event> one;
                if (auto e = cs::parse_event(line)) {
                    one.push_back(std::move(*e));
                }
                return one;
            })
            .assign_timestamps_bounded([](const Event& e) { return clink::EventTime{e.ts}; },
                                       std::chrono::seconds(60));

    const auto sink_alerts = [&out_dir](clink::api::DataStream<Alert> stream,
                                        const std::string& name) {
        stream.map<std::string>([](const Alert& a) { return cs::alert_to_json(a); })
            .sink(clink::api::FileTextSink::builder()
                      .path(out_dir + "/alerts-" + name + ".ndjson")
                      .build());
    };

    // card_testing: declined-probe burst then an approved strike.
    sink_alerts(
        clink::cep::pattern(
            events.filter([](const Event& e) { return e.type == EventType::Auth; })
                .key_by([](const Event& e) { return e.card; }),
            cs::card_testing_pattern(), cs::event_codec())
            .select<Alert>(std::function<Alert(const cs::Match&)>(cs::card_testing_alert)),
        "card-testing");

    // impossible_travel: card-present auths at implausible speed.
    sink_alerts(
        clink::cep::pattern(
            events.filter([](const Event& e) { return e.type == EventType::Auth; })
                .key_by([](const Event& e) { return e.card; }),
            cs::impossible_travel_pattern(), cs::event_codec())
            .select<Alert>(std::function<Alert(const cs::Match&)>(cs::impossible_travel_alert)),
        "impossible-travel");

    // account_takeover: fails -> password change -> drain, no OTP between.
    sink_alerts(
        clink::cep::pattern(
            events
                .filter([](const Event& e) {
                    return e.type == EventType::LoginFail ||
                           e.type == EventType::PasswordChange ||
                           e.type == EventType::OtpVerify || e.type == EventType::Transfer;
                })
                .key_by([](const Event& e) { return e.account; }),
            cs::account_takeover_pattern(), cs::event_codec())
            .select<Alert>(std::function<Alert(const cs::Match&)>(cs::account_takeover_alert)),
        "account-takeover");

    // otp_never_verified: the alert channel IS the timed-out side output.
    {
        const auto tag = cs::otp_timed_out_tag();
        auto healthy =
            clink::cep::pattern(
                events
                    .filter([](const Event& e) {
                        return e.type == EventType::OtpRequest ||
                               e.type == EventType::OtpVerify;
                    })
                    .key_by([](const Event& e) { return e.account; }),
                cs::otp_pattern(), cs::event_codec())
                .select_with_timed_out<Alert>(
                    std::function<Alert(const cs::Match&)>(cs::otp_healthy_alert), tag,
                    std::function<Alert(const cs::Match&)>(cs::otp_timed_out_alert));
        sink_alerts(healthy.side_output<Alert>(tag), "otp-never-verified");
        // Healthy completions drain to a diagnostics file (not alerts-*);
        // in the spec world every output needs a consumer.
        healthy.map<std::string>([](const Alert& a) { return cs::alert_to_json(a); })
            .sink(clink::api::FileTextSink::builder()
                      .path(out_dir + "/otp-healthy.ndjson")
                      .build());
    }

    // structuring: sub-band transfers whose running total crosses the trip.
    sink_alerts(
        clink::cep::pattern(
            events.filter([](const Event& e) { return e.type == EventType::Transfer; })
                .key_by([](const Event& e) { return e.account; }),
            cs::structuring_pattern(), cs::event_codec())
            .select<Alert>(std::function<Alert(const cs::Match&)>(cs::structuring_alert)),
        "structuring");
}

}  // namespace

CLINK_REGISTER_JOB("card-sentry",
                   "0.4",
                   "five CEP fraud detectors over the card-sentry tape",
                   define_job);
