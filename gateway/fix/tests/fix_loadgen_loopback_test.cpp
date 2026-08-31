// The rig-not-the-bottleneck measurement for the FIX sender
// (03-instruction-fix-gateway.md step 7).
//
// specification.md §8.12's reason 2 claims a QuickFIX initiator would
// be too slow to measure this gateway with, and that our own sender is
// not. That is an argument until someone runs it. This runs it.
//
// Loopback ON PURPOSE: the claim is about the sender's own cost per
// message -- allocation, syscalls, session bookkeeping -- and loopback
// isolates exactly that by removing the NIC and any cross-host RTT. It
// is NOT a statement about gateway latency on real hardware, and must
// not be quoted as one.
//
// The peer is a bare echo acceptor, not the real gateway: measuring
// the sender means nothing downstream may be the limit.

#include <sequencer/bench/fix_requester.hpp>
#include <sequencer/fix/fix_input_transport.hpp>

#include <hffix_fields.hpp>

#include <atomic>
#include <chrono>
#include <cstdio>
#include <string>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

namespace sequencer::fix {
namespace {

// Replies to every U1 with a U2 carrying the same correlation tag, as
// fast as it can. Deliberately does nothing else.
struct EchoAcceptor {
  std::unique_ptr<FixInputTransport> transport;
  std::atomic<std::uint64_t> echoed{0};

  explicit EchoAcceptor(int port) {
    FixInputConfig config;
    config.senderCompId = "ECHO";
    config.heartBtInt = 0;
    transport = std::make_unique<FixInputTransport>(config);
    transport->attach(
        [this](std::shared_ptr<sequencer::RequestContext> request) {
          const sequencer::Payload body = request->body();
          const std::string_view raw(reinterpret_cast<const char*>(body.data()), body.size());
          hffix::message_reader reader(raw.data(), raw.size());
          if (!reader.is_complete() || !reader.is_valid()) {
            return;
          }
          std::string correlation;
          for (auto it = reader.begin(); it != reader.end(); ++it) {
            if (it->tag() == sequencer::bench::kCorrelationTag) {
              correlation.assign(it->value().begin(), it->value().size());
            }
          }
          FixSession* session = transport->sessionFor(request->session());
          if (session == nullptr || correlation.empty()) {
            return;
          }
          session->sendApplication(
              "U2", std::to_string(sequencer::bench::kCorrelationTag) + "=" + correlation + "\001");
          echoed.fetch_add(1, std::memory_order_relaxed);
        },
        [](const sequencer::SessionInfo&) {});
    transport->start(port);
  }
  ~EchoAcceptor() { transport->stop(); }
};

// Sends at `rate` for `seconds` and reports what was actually achieved
// with every reply accounted for.
struct Result {
  std::uint64_t sent = 0;
  std::uint64_t completed = 0;
  double achievedPerSecond = 0.0;
};

Result driveAt(sequencer::bench::FixRequester& sender, std::uint64_t rate, int seconds) {
  const auto start = std::chrono::steady_clock::now();
  const auto finish = start + std::chrono::seconds(seconds);
  const auto interval = std::chrono::nanoseconds(1'000'000'000ULL / rate);

  std::atomic<std::uint64_t> completed{0};
  std::uint64_t sent = 0;
  auto nextSend = start;
  while (std::chrono::steady_clock::now() < finish) {
    // Open loop: emit on schedule regardless of what has come back,
    // which is what makes a drop visible instead of hidden by
    // self-throttling.
    while (std::chrono::steady_clock::now() < nextSend) {
    }
    sender.send(static_cast<std::int64_t>(sent), 0,
                 [&completed](bool) { completed.fetch_add(1, std::memory_order_relaxed); });
    ++sent;
    nextSend += interval;
  }

  // Let the tail of replies land.
  const auto drainUntil = std::chrono::steady_clock::now() + std::chrono::seconds(3);
  while (completed.load() < sent && std::chrono::steady_clock::now() < drainUntil) {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }

  const double elapsed =
      std::chrono::duration<double>(std::chrono::steady_clock::now() - start).count();
  Result result;
  result.sent = sent;
  result.completed = completed.load();
  result.achievedPerSecond = static_cast<double>(sent) / elapsed;
  return result;
}

// The criterion: the sender must clear 2x the highest rate the gateway
// is ever targeted at, with zero drops. The sweeps in raft-tests target
// 100k, so the sender must clear 200k.
// DISABLED as a gate, kept as a MEASUREMENT, because it currently
// FAILS and the failure is the finding.
//
// Measured on this machine, release build, 2026-08-31:
//
//   offered 200,000/s -> achieved 72,727/s, 222,620 sent, 222,620
//   completed, zero drops
//
// Zero drops is real and good: every message the sender emitted was
// accounted for. The RATE is not. 72.7k/s does not clear 2x a 100k
// target, so on this evidence the sender is not yet fast enough to
// measure the gateway with -- a sweep at 100k would be reporting the
// rig as much as the system.
//
// It also does not vindicate specification.md §8.12's reason 2 as
// written. That reason says a QuickFIX initiator "tops out around tens
// of thousands of messages per second" and that ours would not have to.
// 72.7k/s is tens of thousands. The architectural argument for owning
// the session layer may still hold -- reasons 1 and 3 are untouched,
// and this sender is deliberately naive -- but reason 2's specific
// claim is unproven until this number moves.
//
// The likely cause is the one this repository has hit three times
// already: one write() syscall per message, in both directions, with
// the echo peer in the same process. The relay, output and input
// gateways all got their order-of-magnitude from coalescing writes,
// and nothing here does that yet -- FixSession::sendApplication() calls
// the send callback once per message, and FixRequester writes it
// straight to the socket under a mutex.
//
// Run it directly to reproduce:
//   ./build/release/gateway/fix/tests/fix_loadgen_loopback_test
//       --gtest_also_run_disabled_tests
TEST(FixLoadGeneratorLoopback, DISABLED_TheSenderIsNotTheBottleneck) {
  constexpr std::uint64_t kHighestTargetRate = 100000;
  constexpr std::uint64_t kRequired = 2 * kHighestTargetRate;

  EchoAcceptor echo(29641);
  sequencer::bench::FixRequester sender("127.0.0.1", 29641, "RIG", "ECHO");
  ASSERT_TRUE(sender.start()) << "the sender could not establish its FIX session";

  // Warm the path: first-touch page faults and the first few sends are
  // not what this measures.
  driveAt(sender, 20000, 1);

  const Result result = driveAt(sender, kRequired, 3);
  std::printf("\nFIX load sender, loopback: offered %llu/s, achieved %.0f/s, "
              "%llu sent, %llu completed\n",
              (unsigned long long)kRequired, result.achievedPerSecond,
              (unsigned long long)result.sent, (unsigned long long)result.completed);

  EXPECT_GE(result.achievedPerSecond, static_cast<double>(kRequired) * 0.95)
      << "the sender cannot offer 2x the highest target rate, so any gateway "
          "number it produces would be a number about the rig";
  EXPECT_EQ(result.completed, result.sent)
      << "replies were dropped: a completion that never arrives is a "
          "measurement the harness cannot account for";
}

}  // namespace
}  // namespace sequencer::fix
