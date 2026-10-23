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
// Shape, in the payment-rail configuration (cluster/run-rail.sh):
//
//   kafka payments.events -> parse -> event-time watermarks
//     -> idempotency guard (keyed by event id, event-time TTL)
//     -> five detectors
//        -> each alert stream: file (diagnostic) + postgres 2PC + kafka 2PC
//
// Configuration is read from the SUBMITTER's environment when the graph
// spec is built, and travels to the workers inside the spec: source and
// sink params, and the guard's TTL as an operator param. That last point
// is a rule, learnt the hard way: define_job runs in EVERY process that
// loads this plugin (the submitter, then each worker, which needs the
// inline operators registered), so a value captured into an operator's
// constructor at define time is whatever THAT process's environment says.
// Only what is in the spec is the same everywhere. The variables:
//   CS_SOURCE        "file" (default) or "kafka"
//   CS_EVENTS        file mode: tape path as workers see it (default /data/events.ndjson)
//   CS_KAFKA_BROKERS kafka mode: bootstrap servers as workers see them
//   CS_KAFKA_TOPIC   kafka mode: the events topic (default payments.events)
//   CS_KAFKA_GROUP   kafka mode: consumer group id (default card-sentry); the
//                    engine's checkpoints, not the group, decide where a
//                    restore resumes
//   CS_GUARD         "1" (default) runs the idempotency guard in front of the
//                    detectors; "0" makes it a pass-through (the rail scene's
//                    negative control). The operator stays in the graph
//                    either way: the graph's SHAPE must not depend on the
//                    environment, because the inline operators are numbered
//                    in the order define_job registers them, and a shape that
//                    differs between the submitter and a worker gives the
//                    same operator two names
//   CS_GUARD_TTL_MS  the guard's event-time TTL (default 24 h)
//   CS_GUARD_DEBUG   "1" logs the guard's restored state at open and every
//                    replay it admits (worker stderr)
//   CS_OUT_DIR       alert output directory as workers see it (default /out);
//                    the file sinks are at-least-once diagnostics
//   CS_PG_DSN        when set, every alert ALSO lands in Postgres through the
//                    exactly-once two-phase-commit sink (PREPARE TRANSACTION /
//                    COMMIT PREPARED, committed when its checkpoint is globally
//                    durable) - the case store
//   CS_PG_TABLE      the case table (default "alerts")
//   CS_KAFKA_ALERTS_TOPIC  when set (kafka brokers required), every alert ALSO
//                    lands on this topic inside a broker transaction that
//                    commits with the same checkpoint; read it with
//                    isolation.level=read_committed

#include <chrono>
#include <cstdlib>
#include <memory>
#include <string>
#include <vector>

#include <clink/api/builtin_connectors.hpp>
#include <clink/api/pipeline.hpp>
#include <clink/cep/cep.hpp>
#include <clink/job/register_job.hpp>
#include <clink/operators/process_function.hpp>
#include <clink/plugin/plugin.hpp>
#include <clink/runtime/output_tag.hpp>
#include <clink/time/event_time.hpp>

#include "decision.hpp"
#include "events.hpp"
#include "idempotency.hpp"
#include "patterns.hpp"

namespace {

std::string env_or(const char* key, const char* fallback) {
    if (const char* v = std::getenv(key); v != nullptr && *v != '\0') {
        return v;
    }
    return fallback;
}

constexpr std::chrono::seconds kLatenessBound{60};

void define_job(clink::api::Pipeline& env) {
    using cs::Alert;
    using cs::Event;
    using cs::EventType;

    env.registry().register_type<Event>("cs_event", cs::event_codec());
    env.registry().register_type<Alert>("cs_alert", cs::alert_codec());
    env.registry().register_type<cs::Decision>("cs_decision", cs::decision_codec());

    const std::string source_kind = env_or("CS_SOURCE", "file");
    const std::string in = env_or("CS_EVENTS", "/data/events.ndjson");
    const std::string brokers = env_or("CS_KAFKA_BROKERS", "");
    const std::string events_topic = env_or("CS_KAFKA_TOPIC", "payments.events");
    const std::string group = env_or("CS_KAFKA_GROUP", "card-sentry");
    const bool guard_on = env_or("CS_GUARD", "1") != "0";
    const auto guard_ttl = std::chrono::milliseconds{std::stoll(env_or("CS_GUARD_TTL_MS", "86400000"))};
    const bool guard_debug = env_or("CS_GUARD_DEBUG", "0") == "1";
    const std::string out_dir = env_or("CS_OUT_DIR", "/out");
    const std::string pg_dsn = env_or("CS_PG_DSN", "");
    const std::string pg_table = env_or("CS_PG_TABLE", "alerts");
    const std::string alerts_topic = env_or("CS_KAFKA_ALERTS_TOPIC", "");
    // The diagnostics file sinks are at-least-once and cap the job's
    // guarantee; CS_DIAG=0 drops them where a transactional store takes
    // the alerts, so a run can be end-to-end exactly-once on every sink.
    const bool diag = env_or("CS_DIAG", "1") != "0";
    // The decision path's stores, each optional: a two-phase-commit table,
    // an upsert table keyed by the authorisation id, a plain append table.
    const std::string decisions_table = env_or("CS_DECISIONS_TABLE", "");
    const std::string decisions_fast_table = env_or("CS_DECISIONS_FAST_TABLE", "");
    const std::string decisions_append_table = env_or("CS_DECISIONS_APPEND_TABLE", "");

    // The input. The Kafka source checkpoints its per-partition offsets as
    // operator state, so a restore resumes exactly where the checkpoint cut
    // the stream; the consumer group is a courtesy, never the authority.
    clink::api::DataStream<std::string> lines = [&] {
        if (source_kind == "kafka") {
            if (brokers.empty()) {
                throw std::runtime_error("CS_SOURCE=kafka needs CS_KAFKA_BROKERS");
            }
            clink::api::SourceDescriptor src;
            src.op_type = "kafka_source_string";
            src.channel_type = "string";
            src.params = {{"brokers", brokers},
                          {"topic", events_topic},
                          {"group_id", group},
                          {"auto_offset_reset", "earliest"}};
            return env.source<std::string>(src, "cs-events-kafka");
        }
        return env.source<std::string>(
            clink::api::FileTextSource::builder().path(in).build(), "cs-events-file");
    }();

    auto parsed =
        lines
            .flat_map<Event>([](const std::string& line) {
                std::vector<Event> one;
                if (auto e = cs::parse_event(line)) {
                    one.push_back(std::move(*e));
                }
                return one;
            })
            .assign_timestamps_bounded([](const Event& e) { return clink::EventTime{e.ts}; },
                                       kLatenessBound);

    // The idempotency guard: keyed by the event id, so a replayed producer
    // batch is dropped before any detector can count it twice. Registered
    // as a named operator whose TTL comes from its spec params (see the
    // rule above), and registered unconditionally so every worker can
    // build it whatever its own environment says. Stateful, so it carries
    // a uid: that is what lets a restore find its seen-set.
    env.registry().register_operator<Event, Event>(
        "cs_idempotency_guard",
        [](const clink::plugin::BuildContext& ctx) -> std::shared_ptr<clink::Operator<Event, Event>> {
            const auto ttl = std::chrono::milliseconds{std::stoll(ctx.param_or("ttl_ms", "86400000"))};
            const bool debug = ctx.param_or("debug", "0") == "1";
            const bool enabled = ctx.param_or("enabled", "1") != "0";
            return std::make_shared<clink::detail::KeyedProcessFunctionAdapter<std::int64_t, Event, Event>>(
                std::make_shared<cs::IdempotencyGuardFn>(ttl, kLatenessBound, debug, enabled),
                [](const Event& e) { return e.id; }, nullptr, "idempotency_guard");
        });
    auto events = parsed.key_by([](const Event& e) { return e.id; })
                      .process<Event>("cs_idempotency_guard",
                                      {{"ttl_ms", std::to_string(guard_ttl.count())},
                                       {"debug", guard_debug ? "1" : "0"},
                                       {"enabled", guard_on ? "1" : "0"},
                                       // Declared for the delivery analyser: its output
                                       // is a function of its state and its input.
                                       {"determinism", "deterministic"}},
                                      "cs-idempotency-guard");
    events.uid("cs-idempotency-guard");

    // The decision path: one post-authorisation risk decision per card
    // authorisation, keyed by card, from the card's recent declines and its
    // last fix (decision.hpp). A named operator with its thresholds as spec params,
    // for the same reason as the guard. Stateful, so it carries a uid.
    env.registry().register_operator<Event, cs::Decision>(
        "cs_decision",
        [](const clink::plugin::BuildContext& ctx)
            -> std::shared_ptr<clink::Operator<Event, cs::Decision>> {
            cs::DecisionPolicy policy;
            policy.window_ms = std::stoll(ctx.param_or("window_ms", "3600000"));
            policy.review_at = std::stoi(ctx.param_or("review_at", "50"));
            policy.block_at = std::stoi(ctx.param_or("block_at", "100"));
            policy.review_amount = std::stod(ctx.param_or("review_amount", "400"));
            policy.max_speed_kmh = std::stod(ctx.param_or("max_speed_kmh", "900"));
            return std::make_shared<
                clink::detail::KeyedProcessFunctionAdapter<std::int64_t, Event, cs::Decision>>(
                std::make_shared<cs::DecisionFn>(policy),
                [](const Event& e) { return e.card; }, nullptr, "decision");
        });
    auto decisions = events.filter([](const Event& e) { return e.type == EventType::Auth; })
                         .key_by([](const Event& e) { return e.card; })
                         .process<cs::Decision>("cs_decision",
                                                {{"window_ms", "3600000"},
                                                 {"review_at", "50"},
                                                 {"block_at", "100"},
                                                 {"review_amount", "400"},
                                                 {"max_speed_kmh", "900"},
                                                 // Declared, so the analyser warns rather
                                                 // than reports unknown: every field but one
                                                 // is a function of the restored state and
                                                 // the replayed input.
                                                 {"determinism",
                                                  "nondeterministic: decided_at is the wall "
                                                  "clock at emission"}},
                                                "cs-decision");
    decisions.uid("cs-decision");
    auto decision_jsons =
        decisions.map<std::string>([](const cs::Decision& d) { return cs::decision_to_json(d); });
    {
        // Three stores for one stream, each with the delivery its sink can
        // offer; the rail scene reads all three from outside the engine.
        const std::string columns = "id,ts,card,amount,decision,reason,risk,decided_at";
        bool any = false;
        if (!pg_dsn.empty() && !decisions_table.empty()) {
            // Exactly-once: rows appear when their checkpoint completes.
            clink::api::SinkDescriptor pg;
            pg.op_type = "postgres_2pc_sink";
            pg.channel_type = "string";
            pg.params = {{"conninfo", pg_dsn}, {"table", decisions_table}, {"columns", columns}};
            decision_jsons.sink(pg, "cs-decisions-2pc");
            any = true;
        }
        if (!pg_dsn.empty() && !decisions_fast_table.empty()) {
            // Effectively-once: INSERT ... ON CONFLICT (id) DO UPDATE at every
            // flush, so a replayed interval rewrites the same rows. Visible
            // at the flush, before the checkpoint completes.
            clink::api::SinkDescriptor pg;
            pg.op_type = "postgres_upsert_sink";
            pg.channel_type = "string";
            pg.params = {{"conninfo", pg_dsn},
                         {"table", decisions_fast_table},
                         {"columns", columns},
                         {"key_columns", "id"},
                         {"batch_records", "200"}};
            decision_jsons.sink(pg, "cs-decisions-fast");
            any = true;
        }
        if (!pg_dsn.empty() && !decisions_append_table.empty()) {
            // At-least-once: plain inserts, so a replayed interval adds its
            // rows a second time.
            clink::api::SinkDescriptor pg;
            pg.op_type = "postgres_sink";
            pg.channel_type = "string";
            pg.params = {{"conninfo", pg_dsn},
                         {"table", decisions_append_table},
                         {"columns", columns},
                         {"batch_records", "200"}};
            decision_jsons.sink(pg, "cs-decisions-append");
            any = true;
        }
        if (!any) {
            decision_jsons.sink(
                clink::api::FileTextSink::builder().path(out_dir + "/decisions.ndjson").build());
        }
    }

    const auto sink_alerts = [&](clink::api::DataStream<Alert> stream, const std::string& name) {
        auto jsons =
            stream.map<std::string>([](const Alert& a) { return cs::alert_to_json(a); });
        // Diagnostics: at-least-once, duplicates after a restore are expected.
        // Kept whenever no transactional store takes the alerts, so the
        // stream always has a sink.
        if (diag || (pg_dsn.empty() && alerts_topic.empty())) {
            jsons.sink(clink::api::FileTextSink::builder()
                           .path(out_dir + "/alerts-" + name + ".ndjson")
                           .build());
        }
        if (!pg_dsn.empty()) {
            // The exactly-once case store: rows land iff their checkpoint
            // completes globally; a prepared-but-uncommitted transaction
            // survives a worker crash and is committed on restart. The
            // sink consumes the alert JSON directly - keys map to the
            // case table's columns.
            //
            // The explicit id is the sink's STABLE identity: a 2PC sink
            // names its PREPARE TRANSACTION gid after its operator id, so
            // sibling sinks must not share one. A per-detector id keeps
            // each sink's transactions (and its per-checkpoint committable
            // state) distinct and stable across restarts.
            clink::api::SinkDescriptor pg;
            pg.op_type = "postgres_2pc_sink";
            pg.channel_type = "string";
            pg.params = {{"conninfo", pg_dsn},
                         {"table", pg_table},
                         {"columns", "pattern,entity_kind,entity_id,ts,detail"}};
            jsons.sink(pg, "cs-case-sink-" + name);
        }
        if (!alerts_topic.empty()) {
            // The exactly-once alert feed: records are produced inside a
            // broker transaction that is sealed at the barrier and committed
            // when the checkpoint is globally durable. A consumer sees them
            // only with isolation.level=read_committed; the aborted
            // transactions of a killed worker stay on the log, invisible.
            //
            // One transactional id per sink: a broker fences the previous
            // incarnation of an id, which is how a restarted sink's open
            // settles what its predecessor left behind.
            //
            // replay_suppression is OFF for this feed. The sink's default
            // suppression is exact only for watermark-monotone input
            // (windowed emissions); an alert carries the event time of its
            // evidence, and a timed-out OTP alert's evidence is older than
            // the watermark that fired it, so the sink could not classify
            // it. Receipts and in-doubt resolution still make a worker loss
            // duplicate-free; the documented floor without suppression is
            // one checkpoint interval of duplicates should a transaction
            // expire during a long outage, which the read_committed gate
            // would catch.
            clink::api::SinkDescriptor kafka;
            kafka.op_type = "kafka_2pc_sink_string";
            kafka.channel_type = "string";
            kafka.params = {{"brokers", brokers},
                            {"topic", alerts_topic},
                            {"transactional_id", "cs-alerts-" + name},
                            {"replay_suppression", "false"}};
            jsons.sink(kafka, "cs-alerts-sink-" + name);
        }
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
        // in the spec world every output needs a consumer. Under CS_DIAG=0
        // they go to the exactly-once file sink instead (parts published by
        // rename when their checkpoint completes), so no at-least-once sink
        // remains to cap the job's guarantee.
        auto healthy_jsons =
            healthy.map<std::string>([](const Alert& a) { return cs::alert_to_json(a); });
        if (diag) {
            healthy_jsons.sink(clink::api::FileTextSink::builder()
                                   .path(out_dir + "/otp-healthy.ndjson")
                                   .build());
        } else {
            clink::api::SinkDescriptor two_phase;
            two_phase.op_type = "file_2pc_sink_string";
            two_phase.channel_type = "string";
            two_phase.params = {{"dir", out_dir + "/otp-healthy-2pc"}};
            healthy_jsons.sink(two_phase, "cs-otp-healthy-2pc");
        }
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
                   "0.5",
                   "five CEP fraud detectors over the card-sentry tape",
                   define_job);
