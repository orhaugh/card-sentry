#pragma once

// The idempotency guard: exactly-once inside the engine does not dedupe
// the input.
//
// Producers on a payment rail are at-least-once. A gateway retries a batch
// after a timeout, a connector replays from its last commit, an operator
// re-sends a file. Each puts the same event on the topic twice, under the
// same id, and a detector that counts declines or sums transfers counts it
// twice. clink guarantees that every RECORD it reads affects the output
// exactly once; it cannot know that two records are one event. That
// knowledge is the event id, and this operator applies it: the first record
// with an id passes, every later one is dropped.
//
// The seen-set is keyed state under an EVENT-TIME TTL, so it is bounded by
// data time rather than wall-clock: an id can reappear after `ttl` of
// watermark progress and is admitted again. That is the contract's edge,
// stated rather than hidden - a producer that may replay a day-old batch
// needs a day-long TTL, and the state cost is the number of distinct ids
// inside that window. The set is checkpointed with everything else, so a
// restore resumes it and a replayed interval meets the same answers.

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <optional>
#include <string>

#include <clink/core/codec.hpp>
#include <clink/operators/process_function.hpp>
#include <clink/state/keyed_state.hpp>

#include "events.hpp"

namespace cs {

inline constexpr const char* kSeenSlot = "seen";
inline constexpr std::chrono::hours kDefaultSeenTtl{24};

class IdempotencyGuardFn final : public clink::KeyedProcessFunction<std::int64_t, Event, Event> {
public:
    // `ttl` is event time. `lateness_bound` is the watermark assigner's
    // out-of-orderness bound upstream; it only matters before the first
    // watermark arrives (see process_element).
    // `debug` prints the guard's decisions that matter to stderr (the
    // worker log on a cluster): the state it opened with, and every
    // admission of an id below the highest id already admitted, which on
    // a tape whose ids are monotonic can only be a replay getting through.
    // `enabled` false makes the guard a pass-through that keeps no state:
    // the scene's negative control. It is a parameter rather than an
    // absent operator because a plugin job's graph must have the same
    // shape in every process that builds it (see card_sentry_job.cpp).
    explicit IdempotencyGuardFn(std::chrono::milliseconds ttl = kDefaultSeenTtl,
                                std::chrono::milliseconds lateness_bound = std::chrono::seconds{60},
                                bool debug = false,
                                bool enabled = true)
        : ttl_(ttl), lateness_bound_(lateness_bound), debug_(debug), enabled_(enabled) {}

    void open(clink::RuntimeContext& ctx) override {
        clink::TtlConfig ttl;
        ttl.ttl = ttl_;
        ttl.domain = clink::TtlTimeDomain::EventTime;
        seen_.emplace(ctx.keyed_state<std::int64_t, std::int64_t>(
            kSeenSlot, clink::int64_codec(), clink::int64_codec(), ttl));
        if (debug_) {
            std::uint64_t entries = 0;
            std::int64_t max_key = 0;
            std::int64_t max_ts = 0;
            seen_->scan([&](std::int64_t key, const std::int64_t& ts) {
                ++entries;
                max_key = std::max(max_key, key);
                max_ts = std::max(max_ts, ts);
            });
            max_admitted_id_ = max_key;
            std::fprintf(stderr,
                         "[cs.guard] open: %llu entries restored, max id %lld, max ts %lld, "
                         "ttl %lld ms, slot has_watermark=%d\n",
                         static_cast<unsigned long long>(entries), static_cast<long long>(max_key),
                         static_cast<long long>(max_ts), static_cast<long long>(ttl_.count()),
                         seen_->has_watermark() ? 1 : 0);
        }
    }

    void process_element(const Event& e,
                         clink::ProcessFunctionContext<Event>& ctx,
                         clink::Collector<Event>& out) override {
        if (!enabled_) {
            ++admitted_;
            if (const auto ts = ctx.timestamp(); ts.has_value()) {
                out.collect_with_timestamp(e, *ts);
            } else {
                out.collect(e);
            }
            return;
        }
        // The slot measures its TTL against the clock the operator feeds it,
        // and keeps the highest value it has been given. Two sources feed
        // it: the event time the record itself implies (its timestamp less
        // the assigner's bound, which the real watermark can never be
        // below), and the operator's current watermark when there is one.
        // The first matters before any watermark has arrived - the slot
        // would otherwise stamp an entry relative to the epoch and the
        // first real watermark would expire it at once - and again after a
        // restore, when the watermark is unknown until the next one lands.
        seen_->advance_watermark(e.ts - lateness_bound_.count());
        clock_ = std::max(clock_, e.ts - lateness_bound_.count());
        const auto wm = ctx.current_watermark();
        if (wm.has_value()) {
            seen_->advance_watermark(wm->millis());
            clock_ = std::max(clock_, wm->millis());
        }

        const auto key = current_key();
        if (seen_->get(key).has_value()) {
            ++dropped_;
            return;
        }
        seen_->put(key, e.ts);
        ++admitted_;
        if (debug_) {
            if (key < max_admitted_id_) {
                const auto st = seen_->ttl_stats();
                std::fprintf(stderr,
                             "[cs.guard] REPLAY ADMITTED id=%lld ts=%lld (max admitted id %lld) "
                             "clock=%lld wm=%lld expired_on_read=%llu expired_in_cleanup=%llu "
                             "admitted=%llu dropped=%llu\n",
                             static_cast<long long>(key), static_cast<long long>(e.ts),
                             static_cast<long long>(max_admitted_id_), static_cast<long long>(clock_),
                             static_cast<long long>(wm.has_value() ? wm->millis() : -1),
                             static_cast<unsigned long long>(st.expired_on_read),
                             static_cast<unsigned long long>(st.expired_in_cleanup),
                             static_cast<unsigned long long>(admitted_),
                             static_cast<unsigned long long>(dropped_));
            } else if (admitted_ % 100000 == 0) {
                const auto st = seen_->ttl_stats();
                std::fprintf(stderr,
                             "[cs.guard] progress admitted=%llu dropped=%llu id=%lld clock=%lld "
                             "expired_on_read=%llu\n",
                             static_cast<unsigned long long>(admitted_),
                             static_cast<unsigned long long>(dropped_), static_cast<long long>(key),
                             static_cast<long long>(clock_),
                             static_cast<unsigned long long>(st.expired_on_read));
            }
            max_admitted_id_ = std::max(max_admitted_id_, key);
        }
        // Event time travels with the record. A bare collect() strips it,
        // and every window and pattern downstream would then ignore the event.
        if (const auto ts = ctx.timestamp(); ts.has_value()) {
            out.collect_with_timestamp(e, *ts);
        } else {
            out.collect(e);
        }
    }

    [[nodiscard]] std::uint64_t admitted() const noexcept { return admitted_; }
    [[nodiscard]] std::uint64_t dropped() const noexcept { return dropped_; }
    std::string name() const override { return "idempotency_guard"; }

private:
    std::chrono::milliseconds ttl_;
    std::chrono::milliseconds lateness_bound_;
    bool debug_;
    bool enabled_;
    std::optional<clink::KeyedState<std::int64_t, std::int64_t>> seen_;
    std::uint64_t admitted_{0};
    std::uint64_t dropped_{0};
    std::int64_t clock_{0};
    std::int64_t max_admitted_id_{0};
};

}  // namespace cs
