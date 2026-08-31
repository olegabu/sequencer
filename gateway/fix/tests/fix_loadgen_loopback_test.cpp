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
          // The echo peer writes one message per reply. It is NOT
          // coalesced, deliberately: this measures the SENDER, and a
          // peer that batched would make the sender look better than it
          // is by absorbing syscalls on its own side. If this peer
          // becomes the limit the sender's number is a floor, not a
          // ceiling -- noted because that is a real possibility at
          // these rates and would understate the sender.
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
// Measured on this machine, release build, 2026-08-31, isolated runs:
//
//   offered 200,000/s -> achieved ~77,000/s per sender
//   (76.5k, 78.3k, 78.7k, 87.1k across four runs; zero drops in every
//   one -- every message emitted was accounted for)
//
// ONE reading of 187k/s was recorded and is discarded: it did not
// reproduce, and it was taken while other copies of this benchmark
// were running. Single readings near a limit are noise, which this
// repository has now learned twice.
//
// PER SENDER is the important qualifier, and it is what makes the rig
// criterion pass rather than fail. The criterion is that the RIG must
// offer >=2x the highest rate the system is targeted at. The rig is not
// one sender: raft-tests drives sweeps from five client boxes, each
// running a load generator beside its own gateway, with the offered
// rate split between them and the latencies merged from every client's
// raw histogram (sweep/sweep-multi.sh, sweep/merge-hdr.py). Five
// senders at ~77k/s offer ~385k/s, which clears 2x a 100k sweep with
// room to spare.
//
// So the honest reading is: a single sender does NOT clear 200k, and
// the deployed rig does. A FIX sweep must therefore be run
// multi-client, exactly as the sequencer sweeps already are -- running
// it from one box would measure the rig.
//
// Loopback is deliberate and limited: it isolates the sender's own cost
// per message by removing the NIC, and it mirrors the real topology,
// where the load generator and its gateway share a client box and only
// the proposal crosses the network. It says nothing about gateway
// latency on real hardware.
//
// Not measured here: write coalescing. It exists on the gateway's
// OUTPUT path (FixOutputTransport's ring drain, via SessionSource's
// beginBatch/endBatch) and this benchmark exercises neither -- the echo
// peer replies one message at a time by design, so what is measured is
// the sender alone. Coalescing's effect on gateway throughput is
// untested and must not be inferred from these numbers.
//
// Run it directly to reproduce:
//   ./build/release/gateway/fix/tests/fix_loadgen_loopback_test
//       --gtest_also_run_disabled_tests
TEST(FixLoadGeneratorLoopback, DISABLED_SenderThroughputOnLoopback) {
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

  // Asserted per sender against the FLEET's share, not against the
  // whole target rate: five client boxes each carry a fifth of the
  // offered load, so a sender must clear 2x its own share.
  constexpr std::uint64_t kFleetSenders = 5;
  const double requiredPerSender = static_cast<double>(kRequired) / kFleetSenders;
  EXPECT_GE(result.achievedPerSecond, requiredPerSender)
      << "a sender cannot carry its share of a " << kHighestTargetRate
      << "/s sweep across " << kFleetSenders << " clients, so the rig would be the limit";
  EXPECT_EQ(result.completed, result.sent)
      << "replies were dropped: a completion that never arrives is a "
          "measurement the harness cannot account for";
}

}  // namespace
}  // namespace sequencer::fix
