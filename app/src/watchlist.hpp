#pragma once

// The dynamic-rules detector: a BroadcastProcessFunction whose control
// stream carries effective-dated merchant rules and whose main stream
// carries authorisations.
//
// Two properties make its output deterministic despite the inherent race
// between the two streams:
//
//   1. Rules are effective-dated. An auth alerts iff a matching rule's
//      activate_at is at or before the AUTH's event time - a comparison
//      between two event times, indifferent to processing order.
//   2. The bootstrap barrier. Auths are buffered until the end_of_rules
//      marker seals the rule set, so an auth can never race ahead of a
//      rule that was published before the tape began. A missing marker
//      means no watchlist alerts at all - a broken rules feed fails the
//      oracle loudly instead of silently under-alerting.
//
// The buffer is a runner-local member, not state: on this embedded tape
// the seal arrives within milliseconds of start, long before the first
// checkpoint could matter. The cluster round revisits this with the
// in-flight capture the broadcast runner already provides.

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include <clink/operators/process_function.hpp>
#include <clink/state/broadcast_state.hpp>

#include "events.hpp"
#include "patterns.hpp"
#include "rules.hpp"

namespace cs {

class WatchlistFn final : public clink::BroadcastProcessFunction<Event, Rule, Alert, RuleSet> {
public:
    void process_broadcast_element(const Rule& r,
                                   clink::BroadcastState<RuleSet>& state,
                                   clink::Collector<Alert>& out) override {
        RuleSet rs = state.get().value_or(RuleSet{});
        if (r.kind == RuleKind::EndOfRules) {
            rs.sealed = true;
        } else {
            rs.rules.push_back(r);
        }
        state.put(rs);
        // The seal itself flushes the bootstrap buffer - the main stream
        // may already have drained entirely (both handlers run on the one
        // runner thread, so this cannot race process_element).
        if (rs.sealed && !pending_.empty()) {
            std::vector<Event> drain;
            drain.swap(pending_);
            for (const auto& buffered : drain) {
                evaluate_(buffered, rs, out);
            }
        }
    }

    void process_element(const Event& e,
                         const clink::BroadcastState<RuleSet>& state,
                         clink::Collector<Alert>& out) override {
        const auto rs = state.get();
        if (!rs.has_value() || !rs->sealed) {
            pending_.push_back(e);
            return;
        }
        if (!pending_.empty()) {
            std::vector<Event> drain;
            drain.swap(pending_);
            for (const auto& buffered : drain) {
                evaluate_(buffered, *rs, out);
            }
        }
        evaluate_(e, *rs, out);
    }

    std::string name() const override { return "watchlist"; }

private:
    void evaluate_(const Event& e, const RuleSet& rs, clink::Collector<Alert>& out) {
        if (e.type != EventType::Auth) {
            return;
        }
        for (const auto& r : rs.rules) {
            if (r.merchant != e.merchant || e.ts < r.activate_at) {
                continue;
            }
            if (r.kind == RuleKind::MerchantWatchlist) {
                Alert a;
                a.pattern = "watchlist_hit";
                a.entity_kind = "card";
                a.entity_id = e.card;
                a.ts = e.ts;
                a.detail = "auth at watchlisted " + e.merchant + " for " +
                           detail::fmt2(e.amount);
                out.collect(std::move(a));
            } else if (r.kind == RuleKind::MerchantCap && e.amount > r.cap) {
                Alert a;
                a.pattern = "cap_exceeded";
                a.entity_kind = "card";
                a.entity_id = e.card;
                a.ts = e.ts;
                a.detail = "auth at " + e.merchant + " for " + detail::fmt2(e.amount) +
                           " over cap " + detail::fmt2(r.cap);
                out.collect(std::move(a));
            }
        }
    }

    std::vector<Event> pending_;  // bootstrap buffer until the rule set seals
};

}  // namespace cs
