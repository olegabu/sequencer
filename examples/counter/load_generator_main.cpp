// specification.md §10's "a small open-loop-capable client" — now the
// thin, counter-specific half of a benchmark-grade harness, matching
// raft-tests/braft/client.cpp's own design: the generic open/closed-
// loop scheduling, HDR histograms, and percentile-summary reporting
// live in bench/load_generator/ (reusable by any future sequencer
// application); this file supplies only what's actually counter-
// specific — the request body, where it goes, and (optionally)
// parsing the assigned sequence number back out of the response so
// bench/load_generator/'s RelayObserver can correlate it against the
// relay's own delivery — see that class's own file comment for why
// that correlation has to happen here rather than inside the generic
// engine: the journal sequence number lives inside this example's own
// JSON response body, not in any generic field LoadGenerator itself
// could read.

#include <sequencer/bench/load_generator.hpp>
#include <sequencer/bench/relay_observer.hpp>

#include <brpc/callback.h>
#include <brpc/channel.h>
#include <brpc/controller.h>

#include <gflags/gflags.h>
#include <glog/logging.h>

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <memory>
#include <string>
#include <thread>

#include "input_gateway.pb.h"
#include "json_util.hpp"

DEFINE_string(input_gateway_addr, "", "The input gateway's \"ip:port\" to submit to (required)");
DEFINE_string(mode, "open", "closed: keep thread_num outstanding. open: emit at rate");
DEFINE_int64(rate, 0, "Target requests per second (open mode)");
DEFINE_int32(burst, 1, "Requests per scheduled instant, sharing its scheduled time (open mode)");
DEFINE_int64(max_inflight, 0, "Cap on unanswered requests (open mode); 0 derives one");
DEFINE_int32(thread_num, 100, "Number of concurrent senders (closed mode)");
DEFINE_int32(warmup, 10, "Seconds of traffic discarded before measuring");
DEFINE_int32(measure, 30, "Seconds recorded");
DEFINE_int32(drain_timeout, 10, "Seconds to wait for in-flight replies after the window closes");
DEFINE_string(pace, "spin", "open mode wait strategy between sends: spin or park");
DEFINE_string(hdr_out, "", "Write a percentile report here");

// Phase 3 (bench/load_generator/README.md): empty disables it — the
// default — so a plain phase-1 run pays nothing extra.
DEFINE_string(relay_grpc_addr, "",
              "The relay gateway's real-gRPC listen address (\"ip:port\") to also observe deliveries from; "
              "empty disables relay observation entirely");
DEFINE_uint64(relay_from_sequence_number, 0, "RelayObserver's own Subscribe starting point; 0 = from the beginning");
DEFINE_int64(relay_ring_capacity, 0,
             "RelayObserver's send-time ring size; 0 derives a generous one from --rate/--thread_num and the "
             "warmup/measure/drain window (see relay_observer.hpp's own comment on undersizing this)");

namespace {

// One delta per sequence number, deterministic (not random) so that
// concurrent closed-loop sender threads never share mutable RNG state
// — the actual value submitted plays no role in the measurement.
std::int64_t deltaFor(std::int64_t sequence) { return (sequence % 201) - 100; }

class SubmitRequester : public sequencer::bench::LoadGeneratorRequester {
 public:
  SubmitRequester(std::string inputGatewayAddr, sequencer::bench::RelayObserver* relayObserver)
      : relayObserver_(relayObserver) {
    brpc::ChannelOptions options;
    options.timeout_ms = 2000;
    CHECK_EQ(0, channel_.Init(inputGatewayAddr.c_str(), &options))
        << "failed to connect to " << inputGatewayAddr;
  }

  void send(std::int64_t sequence, std::int64_t sendTimeUs, std::function<void(bool ok)> onDone) override {
    sequencer::gateway::input::proto::SubmitService_Stub stub(&channel_);
    auto* ctx = new Context();
    ctx->sendTimeUs = sendTimeUs;
    ctx->relayObserver = relayObserver_;
    ctx->onDone = std::move(onDone);
    ctx->cntl.request_attachment().append("{\"delta\": " + std::to_string(deltaFor(sequence)) + "}");
    stub.Submit(&ctx->cntl, &ctx->request, &ctx->response, brpc::NewCallback(&onRpcDone, ctx));
  }

 private:
  struct Context {
    brpc::Controller cntl;
    sequencer::gateway::input::proto::SubmitRequest request;
    sequencer::gateway::input::proto::SubmitResponse response;
    std::int64_t sendTimeUs = 0;
    sequencer::bench::RelayObserver* relayObserver = nullptr;
    std::function<void(bool ok)> onDone;
  };

  // brpc::NewCallback's closure self-deletes after this runs; `ctx` is
  // a separate allocation this function owns and must free itself.
  static void onRpcDone(Context* rawCtx) {
    std::unique_ptr<Context> ctx(rawCtx);
    const bool ok = !ctx->cntl.Failed();
    if (ok && ctx->relayObserver != nullptr) {
      // CounterInputCodec::toOutput's own response shape
      // ({"sequence_number":N,"total":M}) — see counter_input_codec.cpp.
      // A missing/unparseable field here just means this one sample
      // isn't correlated against the relay; the synchronous-receipt
      // measurement above is unaffected either way.
      const std::string body = ctx->cntl.response_attachment().to_string();
      if (auto seq = sequencer::examples::counter::extractJsonIntField(body, "sequence_number")) {
        ctx->relayObserver->recordSend(static_cast<std::uint64_t>(*seq), ctx->sendTimeUs);
      }
    }
    ctx->onDone(ok);
  }

  brpc::Channel channel_;
  sequencer::bench::RelayObserver* relayObserver_;
};

}  // namespace

int main(int argc, char** argv) {
  google::InitGoogleLogging(argv[0]);
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  if (FLAGS_input_gateway_addr.empty()) {
    LOG(ERROR) << "load_generator: --input_gateway_addr is required";
    return 1;
  }

  std::unique_ptr<sequencer::bench::RelayObserver> relayObserver;
  if (!FLAGS_relay_grpc_addr.empty()) {
    std::int64_t ringCapacity = FLAGS_relay_ring_capacity;
    if (ringCapacity <= 0) {
      const std::int64_t assumedPeakRate =
          FLAGS_rate > 0 ? FLAGS_rate : static_cast<std::int64_t>(FLAGS_thread_num) * 1000;
      const std::int64_t totalSeconds = FLAGS_warmup + FLAGS_measure + FLAGS_drain_timeout + 5;
      // 2x the naive rate x duration estimate: headroom against burst
      // and against underestimating closed-mode throughput.
      ringCapacity = std::max<std::int64_t>(1 << 16, assumedPeakRate * totalSeconds * 2);
    }
    relayObserver = std::make_unique<sequencer::bench::RelayObserver>(
        FLAGS_relay_grpc_addr, FLAGS_relay_from_sequence_number, static_cast<std::size_t>(ringCapacity));
    relayObserver->start();
    LOG(INFO) << "load_generator: observing relay at " << FLAGS_relay_grpc_addr << " (ring capacity "
              << ringCapacity << ")";
  }

  SubmitRequester requester(FLAGS_input_gateway_addr, relayObserver.get());

  sequencer::bench::LoadGeneratorConfig config;
  config.mode = FLAGS_mode;
  config.rate = FLAGS_rate;
  config.burst = FLAGS_burst;
  config.maxInflight = FLAGS_max_inflight;
  config.threadNum = FLAGS_thread_num;
  config.warmupSeconds = FLAGS_warmup;
  config.measureSeconds = FLAGS_measure;
  config.drainTimeoutSeconds = FLAGS_drain_timeout;
  config.pace = FLAGS_pace;
  config.hdrOut = FLAGS_hdr_out;

  if (relayObserver) {
    // Mirrors LoadGenerator::run()'s own warmup/measure window
    // computation exactly (same clock, same starting instant) so
    // relay-observed samples are filtered to the same span the main
    // summary's histogram uses — see RelayObserver::setMeasurementWindow's
    // own comment for why this matters. Accurate to within the
    // reporter thread's own ~1s polling granularity, which does not
    // meaningfully bias percentiles at any realistic rate/duration.
    const std::int64_t nowUs = std::chrono::duration_cast<std::chrono::microseconds>(
                                    std::chrono::steady_clock::now().time_since_epoch())
                                    .count();
    const std::int64_t warmupEndUs = nowUs + static_cast<std::int64_t>(FLAGS_warmup) * 1'000'000;
    const std::int64_t endUs = warmupEndUs + static_cast<std::int64_t>(FLAGS_measure) * 1'000'000;
    relayObserver->setMeasurementWindow(warmupEndUs, endUs);
  }

  sequencer::bench::LoadGenerator generator(requester, config);
  const bool ok = generator.run();

  if (relayObserver) {
    // Give any relay deliveries already in flight a moment to land —
    // the load generator's own drain_timeout covers the ack path, not
    // this separate one — then stop and report.
    std::this_thread::sleep_for(std::chrono::seconds(2));
    relayObserver->stop();
    relayObserver->printSummary();
  }

  return ok ? 0 : 1;
}
