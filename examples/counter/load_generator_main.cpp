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

#include <sequencer/bench/brpc_output_observer.hpp>
#include <sequencer/bench/grpc_output_observer.hpp>
#include <sequencer/bench/fix_requester.hpp>

#include "counter_fix_codecs.hpp"
#include <sequencer/bench/load_generator.hpp>
#include <sequencer/bench/relay_observer.hpp>
#include <sequencer/bench/websocket_output_observer.hpp>

#include <brpc/callback.h>
#include <brpc/channel.h>
#include <brpc/controller.h>

#include <gflags/gflags.h>
#include <glog/logging.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <memory>
#include <optional>
#include <sstream>
#include <string>
#include <thread>

#include "input_gateway.pb.h"
#include "json_util.hpp"
#include "node.pb.h"

DEFINE_string(input_gateway_addr, "", "The input gateway's \"ip:port\" to submit to (required "
              "unless --node_addr is set)");
// The "what does the input gateway hop actually cost?" arm. See
// ProposeRequester below for exactly what this skips and what that
// means for reading the two numbers side by side.
DEFINE_string(node_addr, "",
              "Bypass the input gateway: call the node's own ProposeService at this \"ip:port\" "
              "directly. Comma-separated endpoints are allowed; the first one that answers "
              "without a redirect is used for the whole run.");
// The FIX arm. Unlike --input_gateway_addr and --node_addr, this is a
// SESSION gateway: the same FIX session carries the submission and the
// reply, so there is no separate observer to configure -- the round
// trip is measured on one socket (specification.md §8.11, §8.12).
//
// Run it MULTI-CLIENT. A single sender reaches ~77k/s on loopback
// (bench/load_generator/README.md), so one box cannot offer 2x a 100k
// sweep and would measure the rig; five client boxes can. The same
// discipline sweep-multi.sh already applies to the other arms.
DEFINE_string(fix_gateway_addr, "",
              "A FIX session gateway's \"ip:port\", or a COMMA-SEPARATED list of them. One "
              "session is opened per address and requests are spread round-robin across them, "
              "which is what keeps gateways evenly loaded: with one session per client and an "
              "odd client count, some gateway always gets more clients than another, and the "
              "merged latency then blends a busy gateway with a quiet one");
DEFINE_string(fix_sender_comp_id, "LOADGEN", "This sender's FIX CompID; must be unique per client");
DEFINE_string(fix_target_comp_id, "SEQUENCER", "The gateway's FIX CompID");
DEFINE_int64(fix_client_id, 0,
             "This client's id, carried in the high bits of the application payload. It both "
             "correlates replies and selects the per-client topic replies are published on, so "
             "a session receives only its own. MUST DIFFER PER CLIENT: sharing it makes clients "
             "complete each other's requests AND collapses them onto one topic, which shows up "
             "as impossibly low latency followed by collapse rather than as an error");
DEFINE_string(fix_subscribe_symbol, "TOTALS",
              "Subscribe to this broadcast topic by MarketDataRequest before sending; empty "
              "skips it. examples/counter broadcasts its totals, so this is required there");

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
DEFINE_string(hdr_raw_out, "",
              "Write the measured histogram as mergeable \"value,count\" lines. Needed to compute a "
              "correct aggregate p50/p99 when load is split across several clients — averaging their "
              "reported percentiles is not the percentile of the union.");

// Phase 3 (bench/load_generator/README.md): empty disables it — the
// default — so a plain phase-1 run pays nothing extra.
DEFINE_string(relay_grpc_addr, "",
              "The relay gateway's real-gRPC listen address (\"ip:port\") to also observe deliveries from; "
              "empty disables relay observation entirely");
DEFINE_uint64(relay_from_sequence_number, 0, "RelayObserver's own Subscribe starting point; 0 = from the beginning");
DEFINE_int64(relay_ring_capacity, 0,
             "RelayObserver's send-time ring size; 0 derives a generous one from --rate/--thread_num and the "
             "warmup/measure/drain window (see relay_observer.hpp's own comment on undersizing this)");

// The fourth round trip (bench/load_generator/README.md): empty
// disables it — the default — so a plain run pays nothing extra.
// Independent of --relay_grpc_addr: both may be set at once for a
// three-way ack/relay/output-gateway comparison in a single run.
DEFINE_string(output_observer, "", "Which output-gateway flavor to observe deliveries from: empty (default, "
                                    "disabled), grpc, brpc, or websocket");
DEFINE_string(output_gateway_addr, "",
              "The selected output gateway's \"ip:port\" to observe (required if --output_observer is set)");
DEFINE_string(output_gateway_topic, "totals", "Topic to subscribe to on the output gateway");
DEFINE_int64(output_ring_capacity, 0, "Same sizing rule as --relay_ring_capacity, 0 derives one");

namespace {

// One delta per sequence number, deterministic (not random) so that
// concurrent closed-loop sender threads never share mutable RNG state
// — the actual value submitted plays no role in the measurement.
std::int64_t deltaFor(std::int64_t sequence) { return (sequence % 201) - 100; }

// Submits straight to a node's ProposeService, skipping the input
// gateway entirely — the control arm for "what is that hop worth?".
//
// Read the two arms carefully, because this one skips three things at
// once, not just a network hop:
//   1. the hop itself (client -> gateway -> node becomes client -> node),
//   2. CounterInputCodec::toInput's JSON parse — the 8-byte input is
//      built here directly, since ProposeRequest carries raw bytes, and
//   3. NodeProposer's per-request brpc::Channel construction (see
//      gateway/input/src/node_proposer.hpp: it builds and Init()s a
//      fresh Channel inside propose(), for every request; this class
//      Init()s one channel for the whole run).
// A deployment cannot actually skip (1) or (2) — specification.md §3.3
// is explicit that clients submit through a gateway, and something has
// to turn a client's wire format into an input. (3) is the one that is
// simply a cost, not a feature.
class ProposeRequester : public sequencer::bench::LoadGeneratorRequester {
 public:
  ProposeRequester(const std::string& nodeAddrs, sequencer::bench::RelayObserver* relayObserver,
                    sequencer::bench::OutputGatewayObserver* outputObserver)
      : relayObserver_(relayObserver), outputObserver_(outputObserver) {
    std::vector<std::string> endpoints;
    std::string current;
    for (const char c : nodeAddrs) {
      if (c == ',') {
        if (!current.empty()) endpoints.push_back(current);
        current.clear();
      } else {
        current.push_back(c);
      }
    }
    if (!current.empty()) endpoints.push_back(current);
    CHECK(!endpoints.empty()) << "--node_addr must name at least one endpoint";

    // Find the leader once, synchronously, before the measured run —
    // a redirect mid-benchmark would otherwise show up as latency that
    // belongs to leader discovery rather than to the commit path.
    for (const std::string& endpoint : endpoints) {
      brpc::ChannelOptions options;
      options.timeout_ms = 2000;
      if (channel_.Init(endpoint.c_str(), &options) != 0) {
        continue;
      }
      sequencer::node::proto::ProposeService_Stub stub(&channel_);
      sequencer::node::proto::ProposeRequest request;
      const std::int64_t probe = 0;
      request.set_input(&probe, sizeof(probe));
      sequencer::node::proto::ProposeResponse response;
      brpc::Controller cntl;
      stub.Propose(&cntl, &request, &response, nullptr);
      if (!cntl.Failed() && !response.redirect() && response.error_message().empty()) {
        LOG(INFO) << "load_generator: proposing directly to the node at " << endpoint;
        return;
      }
    }
    LOG(FATAL) << "load_generator: no endpoint in --node_addr answered as leader";
  }

  void send(std::int64_t sequence, std::int64_t sendTimeUs, std::function<void(bool ok)> onDone) override {
    sequencer::node::proto::ProposeService_Stub stub(&channel_);
    auto* ctx = new Context();
    ctx->sendTimeUs = sendTimeUs;
    ctx->relayObserver = relayObserver_;
    ctx->outputObserver = outputObserver_;
    ctx->onDone = std::move(onDone);
    // The same 8-byte little-endian delta CounterInputCodec::toInput
    // would have produced from {"delta": N}.
    const std::int64_t delta = deltaFor(sequence);
    ctx->request.set_input(&delta, sizeof(delta));
    stub.Propose(&ctx->cntl, &ctx->request, &ctx->response, brpc::NewCallback(&onRpcDone, ctx));
  }

 private:
  struct Context {
    brpc::Controller cntl;
    sequencer::node::proto::ProposeRequest request;
    sequencer::node::proto::ProposeResponse response;
    std::int64_t sendTimeUs = 0;
    sequencer::bench::RelayObserver* relayObserver = nullptr;
    sequencer::bench::OutputGatewayObserver* outputObserver = nullptr;
    std::function<void(bool ok)> onDone;
  };

  static void onRpcDone(Context* rawCtx) {
    std::unique_ptr<Context> ctx(rawCtx);
    const bool ok = !ctx->cntl.Failed() && !ctx->response.redirect() &&
                    ctx->response.error_message().empty();
    if (!ok) {
      static std::atomic<int> loggedFailures{0};
      if (loggedFailures.fetch_add(1, std::memory_order_relaxed) < 5) {
        LOG(WARNING) << "load_generator: propose failed: "
                     << (ctx->cntl.Failed() ? ctx->cntl.ErrorText() : ctx->response.error_message());
      }
    }
    // No JSON to parse here: ProposeResponse carries the assigned
    // sequence number as a real field, which is exactly what the
    // observers need to correlate a dissemination against its send.
    if (ok && (ctx->relayObserver != nullptr || ctx->outputObserver != nullptr) &&
        ctx->response.has_sequence_number()) {
      const std::uint64_t seq = ctx->response.sequence_number();
      if (ctx->relayObserver != nullptr) {
        ctx->relayObserver->recordSend(seq, ctx->sendTimeUs);
      }
      if (ctx->outputObserver != nullptr) {
        ctx->outputObserver->recordSend(seq, ctx->sendTimeUs);
      }
    }
    ctx->onDone(ok);
  }

  brpc::Channel channel_;
  sequencer::bench::RelayObserver* relayObserver_ = nullptr;
  sequencer::bench::OutputGatewayObserver* outputObserver_ = nullptr;
};

class SubmitRequester : public sequencer::bench::LoadGeneratorRequester {
 public:
  SubmitRequester(std::string inputGatewayAddr, sequencer::bench::RelayObserver* relayObserver,
                   sequencer::bench::OutputGatewayObserver* outputObserver)
      : relayObserver_(relayObserver), outputObserver_(outputObserver) {
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
    ctx->outputObserver = outputObserver_;
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
    sequencer::bench::OutputGatewayObserver* outputObserver = nullptr;
    std::function<void(bool ok)> onDone;
  };

  // brpc::NewCallback's closure self-deletes after this runs; `ctx` is
  // a separate allocation this function owns and must free itself.
  static void onRpcDone(Context* rawCtx) {
    std::unique_ptr<Context> ctx(rawCtx);
    const bool ok = !ctx->cntl.Failed();
    if (!ok) {
      // A failed request is excluded from the latency histogram
      // (LoadGenerator::failed_) but that alone doesn't say *why* --
      // logging the first few makes a systematic failure (wrong
      // address, connection refused) visible immediately instead of
      // requiring a second run with more instrumentation to diagnose.
      // Rate-limited: at scale, thousands of identical failures
      // logging individually would itself become the problem.
      static std::atomic<int> loggedFailures{0};
      if (loggedFailures.fetch_add(1, std::memory_order_relaxed) < 5) {
        LOG(WARNING) << "load_generator: request failed: " << ctx->cntl.ErrorText();
      }
    }
    if (ok && (ctx->relayObserver != nullptr || ctx->outputObserver != nullptr)) {
      // CounterInputCodec::toOutput's own response shape
      // ({"sequence_number":N,"total":M}) — see counter_input_codec.cpp.
      // A missing/unparseable field here just means this one sample
      // isn't correlated against whichever observer(s) are active; the
      // synchronous-receipt measurement above is unaffected either way.
      const std::string body = ctx->cntl.response_attachment().to_string();
      if (auto seq = sequencer::examples::counter::extractJsonIntField(body, "sequence_number")) {
        if (ctx->relayObserver != nullptr) {
          ctx->relayObserver->recordSend(static_cast<std::uint64_t>(*seq), ctx->sendTimeUs);
        }
        if (ctx->outputObserver != nullptr) {
          ctx->outputObserver->recordSend(static_cast<std::uint64_t>(*seq), ctx->sendTimeUs);
        }
      }
    }
    ctx->onDone(ok);
  }

  brpc::Channel channel_;
  sequencer::bench::RelayObserver* relayObserver_;
  sequencer::bench::OutputGatewayObserver* outputObserver_;
};

}  // namespace

namespace {

// Splits "a,b,c" -- used for the FIX endpoint list.
std::vector<std::string> splitCommaSeparated(const std::string& value) {
  std::vector<std::string> parts;
  std::stringstream stream(value);
  std::string part;
  while (std::getline(stream, part, ',')) {
    if (!part.empty()) {
      parts.push_back(part);
    }
  }
  return parts;
}

}  // namespace

int main(int argc, char** argv) {
  google::InitGoogleLogging(argv[0]);
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  if (FLAGS_input_gateway_addr.empty() && FLAGS_node_addr.empty() &&
      FLAGS_fix_gateway_addr.empty()) {
    LOG(ERROR) << "load_generator: one of --input_gateway_addr, --node_addr or "
                  "--fix_gateway_addr is required";
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

  std::unique_ptr<sequencer::bench::OutputGatewayObserver> outputObserver;
  if (!FLAGS_output_observer.empty()) {
    if (FLAGS_output_gateway_addr.empty()) {
      LOG(ERROR) << "load_generator: --output_gateway_addr is required when --output_observer is set";
      return 1;
    }
    std::int64_t ringCapacity = FLAGS_output_ring_capacity;
    if (ringCapacity <= 0) {
      const std::int64_t assumedPeakRate =
          FLAGS_rate > 0 ? FLAGS_rate : static_cast<std::int64_t>(FLAGS_thread_num) * 1000;
      const std::int64_t totalSeconds = FLAGS_warmup + FLAGS_measure + FLAGS_drain_timeout + 5;
      ringCapacity = std::max<std::int64_t>(1 << 16, assumedPeakRate * totalSeconds * 2);
    }
    // extractJsonIntField returns int64_t; sequence numbers are always
    // non-negative, so the narrowing cast back to uint64_t is exact.
    auto extractor = [](const std::string& payload) -> std::optional<std::uint64_t> {
      if (auto seq = sequencer::examples::counter::extractJsonIntField(payload, "sequence_number")) {
        return static_cast<std::uint64_t>(*seq);
      }
      return std::nullopt;
    };
    if (FLAGS_output_observer == "grpc") {
      outputObserver = std::make_unique<sequencer::bench::GrpcOutputObserver>(
          FLAGS_output_gateway_addr, FLAGS_output_gateway_topic, extractor,
          static_cast<std::size_t>(ringCapacity));
    } else if (FLAGS_output_observer == "brpc") {
      outputObserver = std::make_unique<sequencer::bench::BrpcOutputObserver>(
          FLAGS_output_gateway_addr, FLAGS_output_gateway_topic, extractor,
          static_cast<std::size_t>(ringCapacity));
    } else if (FLAGS_output_observer == "websocket") {
      outputObserver = std::make_unique<sequencer::bench::WebSocketOutputObserver>(
          FLAGS_output_gateway_addr, FLAGS_output_gateway_topic, extractor,
          static_cast<std::size_t>(ringCapacity));
    } else {
      LOG(ERROR) << "load_generator: --output_observer must be empty, grpc, brpc, or websocket, got \""
                 << FLAGS_output_observer << "\"";
      return 1;
    }
    outputObserver->start();
    LOG(INFO) << "load_generator: observing output gateway (" << FLAGS_output_observer << ") at "
              << FLAGS_output_gateway_addr << " (ring capacity " << ringCapacity << ")";
  }

  // Two arms of the same rig: through the input gateway (the real
  // deployment path, specification.md §3.3) or straight to a node's
  // Propose. See ProposeRequester's own comment for what the direct
  // arm skips beyond the network hop.
  std::unique_ptr<sequencer::bench::LoadGeneratorRequester> requesterOwner;
  if (!FLAGS_fix_gateway_addr.empty()) {
    const std::vector<std::string> addrs = splitCommaSeparated(FLAGS_fix_gateway_addr);
    if (addrs.empty()) {
      LOG(ERROR) << "load_generator: --fix_gateway_addr must be \"ip:port\" or a list of them";
      return 1;
    }
    auto fan = std::make_unique<sequencer::bench::FixFanoutRequester>();
    for (std::size_t i = 0; i < addrs.size(); ++i) {
      const std::size_t colon = addrs[i].rfind(':');
      if (colon == std::string::npos) {
        LOG(ERROR) << "load_generator: bad FIX address \"" << addrs[i] << "\"";
        return 1;
      }
      // Every SESSION gets its own id, not just every client: sessions
      // share a gateway's broadcast fan-out, so two sessions on one id
      // would each receive the other's replies.
      const std::int64_t sessionId =
          FLAGS_fix_client_id * 100 + static_cast<std::int64_t>(i);
      auto one = std::make_unique<sequencer::bench::FixRequester>(
          addrs[i].substr(0, colon), std::stoi(addrs[i].substr(colon + 1)),
          FLAGS_fix_sender_comp_id + "-" + std::to_string(i), FLAGS_fix_target_comp_id,
          sessionId, sequencer::examples::counter::kClientIdShift);
      if (!one->start()) {
        LOG(ERROR) << "load_generator: FIX session did not establish against " << addrs[i];
        return 1;
      }
      // Its OWN topic, so it receives only its own replies.
      one->subscribe(sequencer::examples::counter::counterTopicFor(sessionId));
      fan->add(std::move(one));
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(300));
    LOG(INFO) << "FIX: " << addrs.size() << " session(s) established";
    requesterOwner = std::move(fan);
  } else if (!FLAGS_node_addr.empty()) {
    requesterOwner = std::make_unique<ProposeRequester>(FLAGS_node_addr, relayObserver.get(),
                                                          outputObserver.get());
  } else {
    requesterOwner = std::make_unique<SubmitRequester>(FLAGS_input_gateway_addr, relayObserver.get(),
                                                         outputObserver.get());
  }
  sequencer::bench::LoadGeneratorRequester& requester = *requesterOwner;

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
  config.hdrRawOut = FLAGS_hdr_raw_out;

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
  if (outputObserver) {
    // Mirrors relayObserver's own window computation above exactly —
    // same clock, same starting instant, so all active observers'
    // samples are filtered to the same span.
    const std::int64_t nowUs = std::chrono::duration_cast<std::chrono::microseconds>(
                                    std::chrono::steady_clock::now().time_since_epoch())
                                    .count();
    const std::int64_t warmupEndUs = nowUs + static_cast<std::int64_t>(FLAGS_warmup) * 1'000'000;
    const std::int64_t endUs = warmupEndUs + static_cast<std::int64_t>(FLAGS_measure) * 1'000'000;
    outputObserver->setMeasurementWindow(warmupEndUs, endUs);
  }

  sequencer::bench::LoadGenerator generator(requester, config);
  const bool ok = generator.run();

  if (relayObserver || outputObserver) {
    // Give any deliveries already in flight a moment to land — the
    // load generator's own drain_timeout covers the ack path, not
    // these separate ones — then stop and report.
    std::this_thread::sleep_for(std::chrono::seconds(2));
    if (relayObserver) {
      relayObserver->stop();
      relayObserver->printSummary();
    }
    if (outputObserver) {
      outputObserver->stop();
      outputObserver->printSummary();
    }
  }

  return ok ? 0 : 1;
}
