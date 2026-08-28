#pragma once

// A reusable open/closed-loop load generator, factored out of
// examples/counter's own (previously much more primitive — no
// histogram, no warmup/measure window at all) load_generator_main.cpp
// so that a future sequencer application (a matching engine, say) gets
// the same benchmark-grade harness for free instead of reimplementing
// it. Deliberately NOT in sdk/cpp/: that library is kept header-only
// and dependency-light on purpose (journal + evidence only, no brpc —
// see sdk/cpp/CMakeLists.txt and propose_client.hpp's own file
// comment), and a load generator inherently needs brpc, gflags/glog,
// and an HDR histogram — a different, heavier dependency shape that
// doesn't belong forced into sdk/'s contract.
//
// Modeled closely on ../../../raft-tests/braft/client.cpp's proven
// design (this repository's actual benchmarking harness lives in that
// sibling repository, not here — see bench/load_generator/README.md):
// open loop schedules sends at a fixed target rate and measures each
// one from its *scheduled* time, so time spent waiting to send is
// charged to the system under test, which is what makes it immune to
// coordinated omission and able to show a saturation knee at all;
// closed loop keeps a fixed number of requests outstanding and by
// construction cannot show one. This file intentionally matches that
// design's percentile-summary text output field-for-field (see
// printSummary() below) so raft-tests/sweep/sweep.sh's log-scraping
// keeps working unmodified against this repository's own products.
//
// What's generic here (open/closed-loop timing, HDR histograms,
// warmup/measure/drain windows, the percentile summary, the qps/
// latency rolling report line) versus what's application-specific
// (how to build one request's bytes and where to send them) is split
// exactly the way InputCodec/OutputCodec split meaning from mechanism
// elsewhere in this repository (specification.md §8.5): an application
// implements LoadGeneratorRequester; everything else is this header.

#include <bvar/bvar.h>
#include <hdr/hdr_histogram.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <cstdio>
#include <functional>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

namespace sequencer::bench {

struct LoadGeneratorConfig {
  std::string mode = "open";  // "open" or "closed"

  // Open loop.
  std::int64_t rate = 0;  // requests/second; required (>=1) in open mode
  int burst = 1;          // requests sharing one scheduled instant
  std::int64_t maxInflight = 0;  // 0 => derive (see run())
  std::string pace = "spin";     // "spin": tight-loop precision; "park": usleep, cheaper on cores

  // Closed loop.
  int threadNum = 100;  // outstanding requests kept in flight

  int warmupSeconds = 10;
  int measureSeconds = 30;
  int drainTimeoutSeconds = 10;  // grace period for in-flight replies once the window closes
  std::string hdrOut;            // optional: write a full percentile report here
  // Optional: dump the measured histogram as "value,count" lines, one
  // per recorded bucket. Unlike hdrOut's percentile report this is
  // MERGEABLE — summing several clients' counts into one histogram and
  // reading percentiles off that is the only correct way to get an
  // aggregate p50/p99 across a split load. Averaging per-client
  // percentiles is not the percentile of the union and can be wrong in
  // either direction.
  std::string hdrRawOut;
};

// What an application supplies: how to send one request, tagged with a
// monotonically increasing `sequence` an implementation may use to
// build deterministic content. `sendTimeUs` is the *reference* send
// time this request's latency will be measured from — the scheduled
// instant in open mode (immune to coordinated omission), the actual
// issue instant in closed mode — handed to the implementation (not
// just kept internal to LoadGenerator) so it can independently
// correlate this same request against a side channel using the exact
// same time base LoadGenerator's own histogram uses, without
// LoadGenerator needing to know that side channel exists — see
// relay_observer.hpp's RelayObserver and examples/counter's
// SubmitRequester for a concrete instance: the journal sequence
// number a request is assigned isn't known until the response
// arrives, so a requester that wants to correlate it against
// something else (RelayObserver::recordSend) has to be the one
// carrying `sendTimeUs` forward to that point, not LoadGenerator.
// `onDone(ok)` must be called exactly once — synchronously before
// send() returns (fine for either loop mode) or later from any thread
// (e.g. a brpc callback; required for open mode, since scheduling
// would otherwise block on each reply).
class LoadGeneratorRequester {
 public:
  virtual ~LoadGeneratorRequester() = default;
  virtual void send(std::int64_t sequence, std::int64_t sendTimeUs, std::function<void(bool ok)> onDone) = 0;
};

// Runs synchronously — blocks for warmup + measure + drain, prints the
// summary (matching raft-tests/braft/client.cpp's field names and
// layout so sweep.sh keeps parsing it unmodified), and returns.
// Returns false only for a configuration error (caught before any
// traffic is sent); a degraded run (drops, timeouts) still returns
// true — that is what the summary's own warnings are for.
class LoadGenerator {
 public:
  LoadGenerator(LoadGeneratorRequester& requester, LoadGeneratorConfig config)
      : requester_(requester), config_(std::move(config)) {}

  bool run() {
    if (config_.mode != "open" && config_.mode != "closed") {
      std::fprintf(stderr, "LoadGenerator: mode must be \"open\" or \"closed\"\n");
      return false;
    }
    const bool isOpen = config_.mode == "open";
    if (isOpen) {
      if (config_.rate < 1) {
        std::fprintf(stderr, "LoadGenerator: rate must be >= 1 in open mode\n");
        return false;
      }
      if (config_.burst < 1) {
        std::fprintf(stderr, "LoadGenerator: burst must be >= 1\n");
        return false;
      }
      if (config_.maxInflight < 1) {
        // Roughly ten times the steady-state in-flight count Little's
        // law implies at this rate for a 10 ms p99, floored so low
        // rates still have room — trips only on real pathology, not
        // ordinary jitter (same derivation raft-tests/braft/client.cpp
        // uses).
        config_.maxInflight = std::max<std::int64_t>(1000, config_.rate / 10);
      }
    }

    static constexpr std::int64_t kHighestTrackableUs = 60L * 1000L * 1000L;
    if (hdr_init(1, kHighestTrackableUs, 3, &measured_) != 0 || hdr_init(1, kHighestTrackableUs, 3, &lag_) != 0) {
      std::fprintf(stderr, "LoadGenerator: failed to allocate histograms\n");
      return false;
    }

    const auto start = std::chrono::steady_clock::now();
    const auto warmupEnd = start + std::chrono::seconds(config_.warmupSeconds);
    const auto end = warmupEnd + std::chrono::seconds(config_.measureSeconds);

    std::thread reporter([this, warmupEnd] { reportLoop(warmupEnd); });

    if (isOpen) {
      runOpenLoop(start, end);
    } else {
      runClosedLoop(end);
    }

    measureEnd_ = std::chrono::steady_clock::now();
    stopReporter_.store(true, std::memory_order_relaxed);
    reporter.join();
    printSummary(isOpen);

    hdr_close(measured_);
    hdr_close(lag_);
    return true;
  }

 private:
  using Clock = std::chrono::steady_clock;

  void record(std::int64_t latencyUs) {
    latencyRecorder_ << latencyUs;
    completed_.fetch_add(1, std::memory_order_relaxed);
    if (measuring_.load(std::memory_order_relaxed)) {
      if (latencyUs < 1) {
        latencyUs = 1;
      }
      hdr_record_value_atomic(measured_, latencyUs);
    }
  }

  // -------------------------------------------------------------------
  // Open loop
  // -------------------------------------------------------------------

  void runOpenLoop(Clock::time_point start, Clock::time_point end) {
    const double intervalUs = 1'000'000.0 / static_cast<double>(config_.rate);
    const bool spin = config_.pace != "park";
    std::int64_t sequence = 0;

    while (true) {
      const auto scheduled = start + std::chrono::microseconds(static_cast<std::int64_t>(sequence * intervalUs));
      if (scheduled > end) {
        break;
      }
      const auto now = Clock::now();
      if (now < scheduled) {
        if (!spin && (scheduled - now) > std::chrono::microseconds(150)) {
          std::this_thread::sleep_for(std::chrono::microseconds(50));
        }
        continue;
      }

      for (int i = 0; i < config_.burst; ++i) {
        if (inflight_.load(std::memory_order_relaxed) >= config_.maxInflight) {
          // The rig itself failed to offer this request at the
          // scheduled instant — a genuine miss of the target rate,
          // counted rather than silently skipped (mirrors
          // raft-tests/braft/client.cpp's own "dropped-by-rig").
          dropped_.fetch_add(1, std::memory_order_relaxed);
          ++sequence;
          continue;
        }
        const auto scheduledUs =
            std::chrono::duration_cast<std::chrono::microseconds>(scheduled.time_since_epoch()).count();
        inflight_.fetch_add(1, std::memory_order_relaxed);
        if (measuring_.load(std::memory_order_relaxed)) {
          const std::int64_t lagUs =
              std::chrono::duration_cast<std::chrono::microseconds>(Clock::now() - scheduled).count();
          hdr_record_value_atomic(lag_, lagUs < 1 ? 1 : lagUs);
        }
        requester_.send(sequence, scheduledUs, [this, scheduledUs](bool ok) {
          inflight_.fetch_sub(1, std::memory_order_relaxed);
          if (!ok) {
            // A failed request (channel error, rejection, timeout at
            // the transport layer) can resolve far faster than a real
            // round trip — recording it anyway would silently pull
            // the histogram toward zero, exactly masquerading as a
            // fast system rather than a broken one. See failed_'s own
            // comment on printSummary()'s warning.
            failed_.fetch_add(1, std::memory_order_relaxed);
            return;
          }
          const std::int64_t nowUs =
              std::chrono::duration_cast<std::chrono::microseconds>(Clock::now().time_since_epoch()).count();
          record(nowUs - scheduledUs);
        });
        ++sequence;
      }
    }

    // Drain: replies still arriving belong to requests scheduled inside
    // the window, so let them land before the histogram closes.
    const auto drainDeadline = Clock::now() + std::chrono::seconds(config_.drainTimeoutSeconds);
    while (inflight_.load(std::memory_order_relaxed) > 0 && Clock::now() < drainDeadline) {
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
  }

  // -------------------------------------------------------------------
  // Closed loop
  // -------------------------------------------------------------------

  void runClosedLoop(Clock::time_point end) {
    std::vector<std::thread> senders;
    senders.reserve(config_.threadNum);
    for (int i = 0; i < config_.threadNum; ++i) {
      senders.emplace_back([this, end] {
        while (Clock::now() < end) {
          std::mutex m;
          std::condition_variable cv;
          bool done = false;
          bool succeeded = false;
          const auto sentAt = Clock::now();
          const std::int64_t sentAtUs = std::chrono::duration_cast<std::chrono::microseconds>(
                                             sentAt.time_since_epoch())
                                             .count();
          const std::int64_t sequence = nextSequence_.fetch_add(1, std::memory_order_relaxed);
          requester_.send(sequence, sentAtUs, [&](bool ok) {
            std::lock_guard<std::mutex> lock(m);
            done = true;
            succeeded = ok;
            cv.notify_one();
          });
          std::unique_lock<std::mutex> lock(m);
          cv.wait(lock, [&] { return done; });
          if (!succeeded) {
            // See runOpenLoop's identical check: a failed request's
            // fast local resolution must never be recorded as a real
            // latency sample.
            failed_.fetch_add(1, std::memory_order_relaxed);
            continue;
          }
          const std::int64_t latencyUs =
              std::chrono::duration_cast<std::chrono::microseconds>(Clock::now() - sentAt).count();
          record(latencyUs);
        }
      });
    }
    for (auto& t : senders) {
      t.join();
    }
  }

  // -------------------------------------------------------------------

  void reportLoop(Clock::time_point warmupEnd) {
    while (!stopReporter_.load(std::memory_order_relaxed)) {
      std::this_thread::sleep_for(std::chrono::seconds(1));
      const bool warm = Clock::now() >= warmupEnd;
      if (warm && !measuring_.exchange(true, std::memory_order_relaxed)) {
        // Open the measurement window at the exact instant recording
        // starts, so the sample count and window length describe the
        // same span.
        measureStart_ = Clock::now();
      }
      std::fprintf(stderr, "load_generator: at qps=%ld latency=%ld%s\n",
                   static_cast<long>(latencyRecorder_.qps(1)), static_cast<long>(latencyRecorder_.latency(1)),
                   warm ? "" : " [warmup]");
    }
  }

  void printSummary(bool isOpen) {
    const double windowS = measureEnd_ > measureStart_
                                ? std::chrono::duration<double>(measureEnd_ - measureStart_).count()
                                : 0.0;
    const std::int64_t count = measured_->total_count;
    const double achieved = windowS > 0 ? static_cast<double>(count) / windowS : 0.0;
    const std::int64_t dropped = dropped_.load(std::memory_order_relaxed);

    std::printf("\n=== summary ===\n");
    std::printf("mode                 %s\n", config_.mode.c_str());
    if (isOpen) {
      std::printf("offered rate         %ld req/s (burst %d)\n", static_cast<long>(config_.rate), config_.burst);
      std::printf("max inflight         %ld\n", static_cast<long>(config_.maxInflight));
    } else {
      std::printf("outstanding          %d\n", config_.threadNum);
    }
    std::printf("measure window       %.1f s\n", windowS);
    std::printf("completed            %ld\n", static_cast<long>(count));
    std::printf("achieved rate        %.0f req/s\n", achieved);
    std::printf("dropped-by-rig       %ld\n", static_cast<long>(dropped));
    std::printf("failed               %ld\n", static_cast<long>(failed_.load(std::memory_order_relaxed)));
    std::printf("unanswered           %ld\n", static_cast<long>(inflight_.load(std::memory_order_relaxed)));
    if (count > 0) {
      std::printf("latency us   p50      %ld\n", static_cast<long>(hdr_value_at_percentile(measured_, 50.0)));
      std::printf("             p90      %ld\n", static_cast<long>(hdr_value_at_percentile(measured_, 90.0)));
      std::printf("             p99      %ld\n", static_cast<long>(hdr_value_at_percentile(measured_, 99.0)));
      std::printf("             p99.9    %ld\n", static_cast<long>(hdr_value_at_percentile(measured_, 99.9)));
      std::printf("             p99.99   %ld\n", static_cast<long>(hdr_value_at_percentile(measured_, 99.99)));
      std::printf("             max      %ld\n", static_cast<long>(hdr_max(measured_)));
      std::printf("             mean     %ld\n", static_cast<long>(hdr_mean(measured_)));
    } else {
      // hdr_mean/hdr_max on an empty histogram return meaningless
      // sentinel values (not 0) — printing those as if they were real
      // numbers would be its own small version of this same class of
      // bug (a fast, wrong number standing in for "no data").
      std::printf("latency us   n/a (no successful completions)\n");
    }

    if (isOpen && lag_->total_count > 0) {
      std::printf(
          "schedule lag us       p50 %ld  p99 %ld  max %ld"
          "   (how late sends were; large values mean the rig, not the system under test)\n",
          static_cast<long>(hdr_value_at_percentile(lag_, 50.0)), static_cast<long>(hdr_value_at_percentile(lag_, 99.0)),
          static_cast<long>(hdr_max(lag_)));
    }

    if (isOpen && dropped > 0) {
      std::printf(
          "\nWARNING: %ld requests were never sent, so an offered rate of %ld req/s was not\n"
          "actually achieved. This run cannot be reported as such.\n",
          static_cast<long>(dropped), static_cast<long>(config_.rate));
    }

    const std::int64_t failed = failed_.load(std::memory_order_relaxed);
    if (failed > 0) {
      std::printf(
          "\nWARNING: %ld requests came back failed (ok == false) and were excluded from the\n"
          "latency histogram above. If this number is large relative to completed, do not\n"
          "trust the percentiles: check the target is actually reachable and answering — a\n"
          "systematic failure (wrong address, connection refused, an immediate rejection)\n"
          "resolves fast and, before this check existed, silently pulled p50 toward zero\n"
          "instead of showing up as an error.\n",
          static_cast<long>(failed));
    }

    if (!isOpen && count > 0) {
      // Little's law: outstanding should equal throughput x latency. A
      // large deviation means the rig, not the system under test, is
      // what the numbers describe.
      const double impliedOutstanding = achieved * (hdr_mean(measured_) / 1e6);
      const double ratio = impliedOutstanding / static_cast<double>(config_.threadNum);
      std::printf("little's law ratio   %.2f%s\n", ratio,
                   (ratio < 0.9 || ratio > 1.1) ? "   WARNING: >10% off, suspect a rig bug" : "");
    }

    if (!config_.hdrRawOut.empty()) {
      FILE* f = std::fopen(config_.hdrRawOut.c_str(), "w");
      if (f != nullptr) {
        // Bucket midpoints and counts, exactly as recorded: the merge
        // is a sum, and re-recording a midpoint lands in the same
        // bucket it came from, so a merged histogram is accurate to
        // this histogram's own precision (3 significant figures).
        std::fprintf(f, "value,count\n");
        hdr_iter iter;
        hdr_iter_recorded_init(&iter, measured_);
        while (hdr_iter_next(&iter)) {
          if (iter.count > 0) {
            std::fprintf(f, "%lld,%lld\n", static_cast<long long>(iter.value),
                          static_cast<long long>(iter.count));
          }
        }
        std::fclose(f);
        std::printf("hdr raw              %s\n", config_.hdrRawOut.c_str());
      } else {
        std::fprintf(stderr, "failed to write %s\n", config_.hdrRawOut.c_str());
      }
    }

    if (!config_.hdrOut.empty()) {
      FILE* f = std::fopen(config_.hdrOut.c_str(), "w");
      if (f != nullptr) {
        hdr_percentiles_print(measured_, f, 5, 1.0, CLASSIC);
        std::fclose(f);
        std::printf("hdr report           %s\n", config_.hdrOut.c_str());
      } else {
        std::fprintf(stderr, "failed to write %s\n", config_.hdrOut.c_str());
      }
    }
  }

  LoadGeneratorRequester& requester_;
  LoadGeneratorConfig config_;

  struct hdr_histogram* measured_ = nullptr;
  struct hdr_histogram* lag_ = nullptr;
  bvar::LatencyRecorder latencyRecorder_{"load_generator"};

  std::atomic<bool> measuring_{false};
  std::atomic<bool> stopReporter_{false};
  std::atomic<std::int64_t> inflight_{0};
  std::atomic<std::int64_t> dropped_{0};
  // Requests actually sent that came back failed (ok == false) —
  // distinct from dropped_ (never sent at all, backpressured by
  // maxInflight). See printSummary()'s warning: a run with any of
  // these cannot be reported as a real latency measurement, since
  // it means an unknown fraction of "completed" would otherwise have
  // been a fast local failure rather than a genuine round trip.
  std::atomic<std::int64_t> failed_{0};
  std::atomic<std::int64_t> completed_{0};
  std::atomic<std::int64_t> nextSequence_{0};
  Clock::time_point measureStart_{};
  Clock::time_point measureEnd_{};
};

}  // namespace sequencer::bench
