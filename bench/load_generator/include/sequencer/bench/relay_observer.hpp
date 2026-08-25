#pragma once

// The third round trip bench/load_generator/README.md describes:
// submission to receipt via the relay gateway's real gRPC stream
// (specification.md §8.9), observed on the *same machine* as the
// client. Deliberately not a gateway — no codec, no client protocol
// translation, no application meaning — just a background
// grpc::ClientReader<Record> loop, the same client code
// relay_grpc_test.cpp's own TestGrpcRelayClient already proves out,
// correlating each arrival against this same run's own send times by
// journal sequence number.
//
// A separate, opt-in library target from sequencer_bench_load_generator
// (see this directory's CMakeLists.txt): it pulls in gRPC and
// gateway/relay's generated proto code, a heavier dependency an
// application not comparing against the relay path has no reason to
// link.
//
// Thread synchronization, precisely: recordSend() runs on whichever
// thread learns a request's assigned journal sequence number (a brpc
// completion callback, in examples/counter's SubmitRequester);
// subscribeLoop() runs on this class's own dedicated thread, reading
// the relay's stream. The two race genuinely, not just in theory: the
// relay tails the journal directly and can disseminate a record
// *before* the synchronous acknowledgement has finished its own trip
// back through the input gateway to the client — exactly the
// fast-relay-delivery case this class exists to measure, so it must
// not be silently dropped as a lost race. See recordSend()/
// subscribeLoop()'s own comments for the lock-free ring buffer and
// bounded-wait design that follows from that.

#include <grpcpp/grpcpp.h>
#include <hdr/hdr_histogram.h>

#include <sequencer/journal/record_view.hpp>

#include "relay_grpc.grpc.pb.h"

#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <memory>
#include <string>
#include <thread>

namespace sequencer::bench {

class RelayObserver {
 public:
  // `relayGrpcAddr`: the relay's real-gRPC listen address ("host:port").
  // `fromSequenceNumber`: 0 subscribes from the beginning of the
  // journal (matching relay semantics exactly) — fine, if slower to
  // catch up, against a journal with little prior history; pass the
  // first sequence number this run's own requests will plausibly be
  // assigned to skip straight to it against a journal with a lot.
  // `ringCapacityPow2` is rounded up to the next power of two and
  // becomes the send-time ring's size — size it to comfortably exceed
  // the total number of requests this run will ever issue (rate x
  // duration, for open loop): recordSend() and subscribeLoop() tag
  // each slot with the journal sequence number currently occupying
  // it, so an undersized ring degrades to silently-dropped samples
  // (a stale tag never matches) rather than misattributed ones, but
  // sized correctly there is no wraparound at all within one run,
  // since sequence numbers are assigned once each and never reused.
  RelayObserver(std::string relayGrpcAddr, std::uint64_t fromSequenceNumber, std::size_t ringCapacityPow2)
      : relayGrpcAddr_(std::move(relayGrpcAddr)), fromSequenceNumber_(fromSequenceNumber) {
    std::size_t capacity = 1;
    while (capacity < ringCapacityPow2) {
      capacity <<= 1;
    }
    mask_ = capacity - 1;
    sendTimesUs_ = std::make_unique<std::atomic<std::int64_t>[]>(capacity);
    tags_ = std::make_unique<std::atomic<std::uint64_t>[]>(capacity);
    for (std::size_t i = 0; i < capacity; ++i) {
      tags_[i].store(kUnpublished, std::memory_order_relaxed);
    }

    static constexpr std::int64_t kHighestTrackableUs = 60L * 1000L * 1000L;
    hdr_init(1, kHighestTrackableUs, 3, &observed_);
  }

  ~RelayObserver() {
    stop();
    if (observed_ != nullptr) {
      hdr_close(observed_);
    }
  }

  RelayObserver(const RelayObserver&) = delete;
  RelayObserver& operator=(const RelayObserver&) = delete;

  // Connects and starts the subscriber thread. Call once, before the
  // load generator starts sending — a request whose relay delivery
  // races ahead of this call starting would otherwise be missed.
  void start() {
    auto channel = grpc::CreateChannel(relayGrpcAddr_, grpc::InsecureChannelCredentials());
    stub_ = gateway::relay::grpc_proto::RelayService::NewStub(channel);
    subscriberThread_ = std::thread([this] { subscribeLoop(); });
  }

  // Restricts histogram recording to sends whose reference time falls
  // in [measureStartUs, measureEndUs] — the same warmup-excluded
  // window LoadGenerator's own histogram uses, so the two summaries
  // describe the same span and are actually comparable. Without this,
  // every warmup-phase request the relay happens to deliver (all of
  // them, typically, since the relay has no warmup concept of its
  // own) would inflate relay_completed and skew percentiles toward
  // cold-start latency the main summary deliberately excludes.
  // Optional: unset (the default) records everything, which is fine
  // for a caller not doing a windowed comparison. recordSend() itself
  // is unaffected either way — every sample is still tracked for
  // correlation, just not necessarily tallied into the histogram.
  void setMeasurementWindow(std::int64_t measureStartUs, std::int64_t measureEndUs) {
    measureStartUs_.store(measureStartUs, std::memory_order_relaxed);
    measureEndUs_.store(measureEndUs, std::memory_order_relaxed);
    windowSet_.store(true, std::memory_order_release);
  }

  // Cancels the streaming call and joins the subscriber thread. Safe
  // to call more than once (idempotent) and safe to omit — the
  // destructor calls it too.
  void stop() {
    if (!stopRequested_.exchange(true, std::memory_order_relaxed)) {
      context_.TryCancel();
    }
    if (subscriberThread_.joinable()) {
      subscriberThread_.join();
    }
  }

  // Called from any sending thread, once per request, as soon as the
  // journal sequence number that request was assigned becomes known —
  // NOT at send time, since (unlike the load generator's own
  // synchronous-receipt latency) the journal sequence number isn't
  // known until the response arrives; see load_generator.hpp's
  // LoadGeneratorRequester comment for why `sendTimeUs` is threaded
  // all the way through to here instead. Wait-free: two relaxed/
  // release atomic stores, no lock, no allocation — safe to call from
  // a latency-sensitive completion callback.
  void recordSend(std::uint64_t journalSequenceNumber, std::int64_t sendTimeUs) noexcept {
    const std::size_t slot = journalSequenceNumber & mask_;
    sendTimesUs_[slot].store(sendTimeUs, std::memory_order_relaxed);
    // release: pairs with subscribeLoop()'s acquire load of the same
    // slot, so a reader that observes this tag is guaranteed to also
    // observe the sendTimeUs store above, not a stale one.
    tags_[slot].store(journalSequenceNumber, std::memory_order_release);
    // See firstSeenSeq_'s own comment: records this loop as the low
    // end of "this run's own sequence numbers", the first time it's
    // called with anything smaller than what's recorded so far.
    std::uint64_t floor = firstSeenSeq_.load(std::memory_order_relaxed);
    while ((floor == 0 || journalSequenceNumber < floor) &&
           !firstSeenSeq_.compare_exchange_weak(floor, journalSequenceNumber, std::memory_order_relaxed)) {
    }
  }

  // Prints the relay-observed round-trip percentile summary. Field
  // names are deliberately namespaced ("relay_p50_us", not "p50") so
  // this can never be mistaken by raft-tests/sweep/sweep.sh's
  // whitespace-anchored grep for the load generator's own "p50" et
  // al. — see this directory's README for why that distinction has to
  // be airtight, not just true in practice. Call after both the load
  // generator's own run() and this object's stop() have returned.
  void printSummary() const {
    std::printf("\n=== relay-observed summary ===\n");
    std::printf(
        "(submission to receipt via the relay's real gRPC stream -- not the\n"
        " synchronous ack path above; see bench/load_generator/README.md)\n");
    std::printf("relay_completed=%ld\n", static_cast<long>(observed_->total_count));
    std::printf("relay_dropped_races=%ld\n", static_cast<long>(droppedRaces_.load(std::memory_order_relaxed)));
    std::printf("relay_skipped_historical=%ld\n",
                 static_cast<long>(skippedHistorical_.load(std::memory_order_relaxed)));
    std::printf("relay_p50_us=%ld\n", static_cast<long>(hdr_value_at_percentile(observed_, 50.0)));
    std::printf("relay_p90_us=%ld\n", static_cast<long>(hdr_value_at_percentile(observed_, 90.0)));
    std::printf("relay_p99_us=%ld\n", static_cast<long>(hdr_value_at_percentile(observed_, 99.0)));
    std::printf("relay_p99_9_us=%ld\n", static_cast<long>(hdr_value_at_percentile(observed_, 99.9)));
    std::printf("relay_max_us=%ld\n", static_cast<long>(hdr_max(observed_)));
    std::printf("relay_mean_us=%ld\n", static_cast<long>(hdr_mean(observed_)));
  }

 private:
  static constexpr std::uint64_t kUnpublished = static_cast<std::uint64_t>(-1);

  static std::int64_t nowMicros() {
    return std::chrono::duration_cast<std::chrono::microseconds>(
               std::chrono::steady_clock::now().time_since_epoch())
        .count();
  }

  void subscribeLoop() {
    gateway::relay::grpc_proto::SubscribeRequest request;
    request.set_from_sequence_number(fromSequenceNumber_);
    std::unique_ptr<grpc::ClientReader<gateway::relay::grpc_proto::Record>> reader(
        stub_->Subscribe(&context_, request));

    gateway::relay::grpc_proto::Record record;
    while (reader->Read(&record)) {
      // Captured immediately, before any wait below — the eventual
      // recorded latency must reflect when this record actually
      // arrived, not how long resolving a race against recordSend()
      // happened to take.
      const std::int64_t nowUs = nowMicros();

      const auto* base = reinterpret_cast<const std::byte*>(record.raw_record().data());
      const journal::RecordView view(base, static_cast<std::uint32_t>(record.raw_record().size()));
      const std::uint64_t seq = view.sequenceNumber();
      const std::size_t slot = seq & mask_;

      // A record below the lowest sequence number recordSend() has
      // ever seen cannot possibly belong to this run — it predates
      // every request this process has made, which only happens when
      // --relay_from_sequence_number left real prior history ahead of
      // it (0, the default, always does against a journal that isn't
      // fresh). Skipping the wait for these specifically, rather than
      // treating every one as a potential race worth up to 100ms of
      // patience, is what keeps a long backlog from turning into a
      // multi-minute stall before this loop ever reaches live traffic
      // — reproduced directly: 110 backlog records, all correctly
      // recognized as unrelated, in the time waitForTag's own bound
      // would have spent on a handful.
      const std::uint64_t floor = firstSeenSeq_.load(std::memory_order_relaxed);
      if (floor != 0 && seq < floor) {
        skippedHistorical_.fetch_add(1, std::memory_order_relaxed);
        continue;
      }

      if (waitForTag(slot, seq)) {
        const std::int64_t sendTimeUs = sendTimesUs_[slot].load(std::memory_order_relaxed);
        if (inMeasurementWindow(sendTimeUs)) {
          const std::int64_t latencyUs = std::max<std::int64_t>(1, nowUs - sendTimeUs);
          hdr_record_value_atomic(observed_, latencyUs);
        }
      } else {
        droppedRaces_.fetch_add(1, std::memory_order_relaxed);
      }
    }
  }

  // The relay tails the journal directly and can deliver a record
  // before recordSend() has run for it at all — the synchronous
  // receipt that triggers recordSend() has an extra hop (through the
  // input gateway) the relay's own path does not. A bounded retry, not
  // an immediate skip: skipping unconditionally would systematically
  // under-count exactly the fastest, most interesting relay
  // deliveries. Spin first (this thread has nothing else to do while
  // waiting, and the expected wait for a genuine race is
  // microseconds), then back off; the overall bound is wall-clock time,
  // not iteration count, and deliberately generous — long enough to
  // resolve a race against a genuinely slow-tail ack too (a p99.9
  // ack is exactly the kind of sample worth keeping correlated, not
  // the kind to give up on early), not just a fast-path one. Gives up
  // only once the wait comfortably exceeds any plausible ack latency,
  // at which point either the ring is undersized or the ack genuinely
  // never arrived — either way, this one sample is dropped
  // (relay_dropped_races), not misattributed.
  bool waitForTag(std::size_t slot, std::uint64_t seq) const {
    constexpr int kSpinIterations = 20000;  // sub-millisecond, the common-race case
    constexpr auto kMaxWait = std::chrono::milliseconds(100);
    int spins = 0;
    std::chrono::steady_clock::time_point backoffStart{};
    while (tags_[slot].load(std::memory_order_acquire) != seq) {
      if (spins < kSpinIterations) {
        ++spins;
        continue;
      }
      const auto now = std::chrono::steady_clock::now();
      if (spins == kSpinIterations) {
        backoffStart = now;
        ++spins;
      }
      if (now - backoffStart >= kMaxWait) {
        return false;
      }
      std::this_thread::sleep_for(std::chrono::microseconds(50));
    }
    return true;
  }

  bool inMeasurementWindow(std::int64_t sendTimeUs) const {
    if (!windowSet_.load(std::memory_order_acquire)) {
      return true;
    }
    return sendTimeUs >= measureStartUs_.load(std::memory_order_relaxed) &&
           sendTimeUs <= measureEndUs_.load(std::memory_order_relaxed);
  }

  std::string relayGrpcAddr_;
  std::uint64_t fromSequenceNumber_;
  std::size_t mask_ = 0;
  std::unique_ptr<std::atomic<std::int64_t>[]> sendTimesUs_;
  std::unique_ptr<std::atomic<std::uint64_t>[]> tags_;

  struct hdr_histogram* observed_ = nullptr;
  std::atomic<std::int64_t> droppedRaces_{0};
  std::atomic<std::int64_t> skippedHistorical_{0};
  // The lowest journal sequence number recordSend() has ever been
  // called with — 0 means "none yet". Not the ring's own tags_ (those
  // get reused within a run by design; this is a floor that, once
  // set, only ever moves down): subscribeLoop() uses it to recognize
  // "predates this run entirely" without waiting on it, see there.
  std::atomic<std::uint64_t> firstSeenSeq_{0};
  std::atomic<bool> stopRequested_{false};
  std::atomic<bool> windowSet_{false};
  std::atomic<std::int64_t> measureStartUs_{0};
  std::atomic<std::int64_t> measureEndUs_{0};
  std::thread subscriberThread_;
  std::unique_ptr<gateway::relay::grpc_proto::RelayService::Stub> stub_;
  grpc::ClientContext context_;
};

}  // namespace sequencer::bench
