// card-sentry - five CEP fraud detectors plus a dynamic-rules detector
// over one event tape, in one embedded clink pipeline:
//
//   FileSource -> FlatMap(parse) -> WatermarkAssigner
//     -> fork(6)  (broadcast tee: every branch sees every event + watermark)
//         -> filter -> CepOperator(card_testing)      \
//         -> filter -> CepOperator(impossible_travel)  |
//         -> filter -> CepOperator(account_takeover)   | union -> render
//         -> filter -> CepOperator(otp_never_verified) |   -> FileSink
//         -> filter -> CepOperator(structuring)        |
//         -> filter -> broadcast_process(watchlist) <- + <- rules FileSource
//
// The watchlist detector joins the auth branch with a SECOND stream of
// effective-dated merchant rules through broadcast state (see
// watchlist.hpp for why its output is deterministic despite the two
// streams racing).
//
// The OTP detector is wired inside-out relative to the others: its pattern
// describes the healthy request-then-verify flow, its completed matches are
// dropped, and the alert channel is the operator's TIMED-OUT side output -
// the requests whose verify never arrived inside the window.
//
// Event time comes from the tape's `ts` field through a bounded
// out-of-orderness watermark strategy, so arrival order (which the tape
// deliberately scrambles) never decides an alert; the watermark does.
//
// Run from the repository root after scripts/get-clink.sh and tools/csgen.py:
//
//   ./app/build/card_sentry --in data/events.ndjson --out out/alerts.ndjson

#include <chrono>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <map>
#include <memory>
#include <string>
#include <vector>

#include <clink/connectors/file_sink.hpp>
#include <clink/connectors/file_source.hpp>
#include <clink/connectors/text_format.hpp>
#include <clink/operators/flat_map_operator.hpp>
#include <clink/operators/map_operator.hpp>
#include <clink/operators/watermark_assigner_operator.hpp>
#include <clink/runtime/dag.hpp>
#include <clink/runtime/local_executor.hpp>
#include <clink/state/in_memory_state_backend.hpp>
#include <clink/time/watermark_strategy.hpp>

#include "events.hpp"
#include "patterns.hpp"
#include "rules.hpp"
#include "watchlist.hpp"

namespace {

std::string arg_or(int argc, char** argv, const std::string& flag, std::string fallback) {
    for (int i = 1; i + 1 < argc; ++i) {
        if (argv[i] == "--" + flag) {
            return argv[i + 1];
        }
    }
    return fallback;
}

// Per-branch relevance filter: keeps CEP state small and the patterns easy
// to reason about. Each detector still re-checks types in its predicates.
std::shared_ptr<clink::FlatMapOperator<cs::Event, cs::Event>> keep_types(
    std::initializer_list<cs::EventType> types) {
    const std::vector<cs::EventType> keep(types);
    return std::make_shared<clink::FlatMapOperator<cs::Event, cs::Event>>(
        [keep](const cs::Event& e) {
            std::vector<cs::Event> out;
            for (const auto t : keep) {
                if (e.type == t) {
                    out.push_back(e);
                    break;
                }
            }
            return out;
        });
}

}  // namespace

int main(int argc, char** argv) {
    using namespace clink;

    const std::string in = arg_or(argc, argv, "in", "data/events.ndjson");
    const std::string rules_in = arg_or(argc, argv, "rules", "data/rules.ndjson");
    const std::string out = arg_or(argc, argv, "out", "out/alerts.ndjson");

    if (!std::filesystem::exists(in) || !std::filesystem::exists(rules_in)) {
        std::cerr << "input not found: " << in << " / " << rules_in
                  << " (run tools/csgen.py first)\n";
        return 2;
    }
    std::filesystem::create_directories(std::filesystem::path(out).parent_path());

    Dag dag;

    auto source = std::make_shared<FileSource<std::string>>(in, string_text_format(), 1024);
    auto parse = std::make_shared<FlatMapOperator<std::string, cs::Event>>(
        [](const std::string& line) {
            std::vector<cs::Event> one;
            if (auto e = cs::parse_event(line)) {
                one.push_back(std::move(*e));
            }
            return one;
        });
    // Event time from the payload, tolerating the tape's arrival scramble
    // (bounded at 60 s; the generator caps lateness at 40 s).
    auto assigner = std::make_shared<WatermarkAssignerOperator<cs::Event>>(
        [](const cs::Event& e) { return EventTime{e.ts}; },
        std::make_unique<BoundedOutOfOrdernessStrategy<cs::Event>>(std::chrono::seconds(60)),
        "event_time");

    auto h_src = dag.add_source<std::string>(source);
    auto h_parsed = dag.add_operator<std::string, cs::Event>(h_src, parse);
    auto h_timed = dag.add_operator<cs::Event, cs::Event>(h_parsed, assigner);

    auto branches = dag.fork<cs::Event>(h_timed, 6);

    std::vector<StageHandle<cs::Alert>> alert_streams;

    // card_testing: declined-probe burst then an approved strike.
    {
        auto h = dag.add_operator<cs::Event, cs::Event>(
            branches[0], keep_types({cs::EventType::Auth}));
        alert_streams.push_back(
            dag.add_operator<cs::Event, cs::Alert>(h, cs::make_card_testing()));
    }
    // impossible_travel: card-present auths at implausible speed.
    {
        auto h = dag.add_operator<cs::Event, cs::Event>(
            branches[1], keep_types({cs::EventType::Auth}));
        alert_streams.push_back(
            dag.add_operator<cs::Event, cs::Alert>(h, cs::make_impossible_travel()));
    }
    // account_takeover: fails -> password change -> drain, no OTP between.
    {
        auto h = dag.add_operator<cs::Event, cs::Event>(
            branches[2], keep_types({cs::EventType::LoginFail, cs::EventType::PasswordChange,
                                     cs::EventType::OtpVerify, cs::EventType::Transfer}));
        alert_streams.push_back(
            dag.add_operator<cs::Event, cs::Alert>(h, cs::make_account_takeover()));
    }
    // otp_never_verified: alerts ride the timed-out side output; the
    // completed (healthy) matches are dropped.
    {
        auto h = dag.add_operator<cs::Event, cs::Event>(
            branches[3], keep_types({cs::EventType::OtpRequest, cs::EventType::OtpVerify}));
        auto h_cep = dag.add_operator<cs::Event, cs::Alert>(h, cs::make_otp_never_verified());
        alert_streams.push_back(dag.side_output<cs::Alert>(h_cep, cs::otp_timed_out_tag()));
        auto drop_healthy = std::make_shared<FlatMapOperator<cs::Alert, cs::Alert>>(
            [](const cs::Alert&) { return std::vector<cs::Alert>{}; });
        alert_streams.push_back(dag.add_operator<cs::Alert, cs::Alert>(h_cep, drop_healthy));
    }
    // structuring: sub-band transfers whose running total crosses the trip.
    {
        auto h = dag.add_operator<cs::Event, cs::Event>(
            branches[4], keep_types({cs::EventType::Transfer}));
        alert_streams.push_back(
            dag.add_operator<cs::Event, cs::Alert>(h, cs::make_structuring()));
    }
    // watchlist: auths joined with the effective-dated rules stream via
    // broadcast state. The stable slot/operator name pins the state
    // identity (broadcast_process has no uid hook; the name serves).
    {
        auto rules_source =
            std::make_shared<FileSource<std::string>>(rules_in, string_text_format(), 64);
        auto parse_rules = std::make_shared<FlatMapOperator<std::string, cs::Rule>>(
            [](const std::string& line) {
                std::vector<cs::Rule> one;
                if (auto r = cs::parse_rule(line)) {
                    one.push_back(std::move(*r));
                }
                return one;
            });
        auto h_rules_raw = dag.add_source<std::string>(rules_source);
        auto h_rules = dag.add_operator<std::string, cs::Rule>(h_rules_raw, parse_rules);

        auto h_auths = dag.add_operator<cs::Event, cs::Event>(
            branches[5], keep_types({cs::EventType::Auth}));

        auto fn = std::make_shared<cs::WatchlistFn>();
        auto [pb, pm] =
            clink::detail::build_broadcast_process_callbacks<cs::Event, cs::Rule, cs::Alert,
                                                             cs::RuleSet>(fn);
        alert_streams.push_back(dag.broadcast_process<cs::Event, cs::Rule, cs::Alert, cs::RuleSet>(
            h_auths, h_rules, pb, pm, cs::rule_set_codec(), "cs_watchlist_rules", "watchlist"));
    }

    auto h_alerts = dag.union_streams<cs::Alert>(alert_streams);
    auto render = std::make_shared<MapOperator<cs::Alert, std::string>>(
        [](const cs::Alert& a) { return cs::alert_to_json(a); });
    auto h_lines = dag.add_operator<cs::Alert, std::string>(h_alerts, render);
    dag.add_sink<std::string>(h_lines, std::make_shared<FileSink<std::string>>(
                                           out, string_text_format()));

    JobConfig cfg;
    cfg.state_backend = std::make_shared<InMemoryStateBackend>();

    LocalExecutor exec(std::move(dag), std::move(cfg));
    exec.run();

    // Branch threads interleave, so the alert file's ORDER is not
    // deterministic; the alert SET is. Summarise per pattern.
    std::ifstream written(out);
    std::map<std::string, int> by_pattern;
    std::size_t total = 0;
    for (std::string line; std::getline(written, line);) {
        ++total;
        const auto key = line.find("\"pattern\":\"");
        if (key != std::string::npos) {
            const auto start = key + 11;
            const auto end = line.find('"', start);
            ++by_pattern[line.substr(start, end - start)];
        }
    }
    std::cout << total << " alert(s) written to " << out << "\n";
    for (const auto& [pattern, n] : by_pattern) {
        std::cout << "  " << pattern << ": " << n << "\n";
    }
    return 0;
}
