// specification.md §10's "a small open-loop-capable client" — now the
// thin, counter-specific half of a benchmark-grade harness, matching
// raft-tests/braft/client.cpp's own design: the generic open/closed-
// loop scheduling, HDR histograms, and percentile-summary reporting
// live in bench/load_generator/ (reusable by any future sequencer
// application); this file supplies only what's actually counter-
// specific — the request body and where it goes.

#include <sequencer/bench/load_generator.hpp>

#include <brpc/callback.h>
#include <brpc/channel.h>
#include <brpc/controller.h>

#include <gflags/gflags.h>
#include <glog/logging.h>

#include <cstdint>
#include <string>

#include "input_gateway.pb.h"

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

namespace {

// One delta per sequence number, deterministic (not random) so that
// concurrent closed-loop sender threads never share mutable RNG state
// — the actual value submitted plays no role in the measurement.
std::int64_t deltaFor(std::int64_t sequence) { return (sequence % 201) - 100; }

class SubmitRequester : public sequencer::bench::LoadGeneratorRequester {
 public:
  explicit SubmitRequester(std::string inputGatewayAddr) {
    brpc::ChannelOptions options;
    options.timeout_ms = 2000;
    CHECK_EQ(0, channel_.Init(inputGatewayAddr.c_str(), &options))
        << "failed to connect to " << inputGatewayAddr;
  }

  void send(std::int64_t sequence, std::function<void(bool ok)> onDone) override {
    sequencer::gateway::input::proto::SubmitService_Stub stub(&channel_);
    auto* ctx = new Context();
    ctx->onDone = std::move(onDone);
    ctx->cntl.request_attachment().append("{\"delta\": " + std::to_string(deltaFor(sequence)) + "}");
    stub.Submit(&ctx->cntl, &ctx->request, &ctx->response, brpc::NewCallback(&onRpcDone, ctx));
  }

 private:
  struct Context {
    brpc::Controller cntl;
    sequencer::gateway::input::proto::SubmitRequest request;
    sequencer::gateway::input::proto::SubmitResponse response;
    std::function<void(bool ok)> onDone;
  };

  // brpc::NewCallback's closure self-deletes after this runs; `ctx` is
  // a separate allocation this function owns and must free itself.
  static void onRpcDone(Context* rawCtx) {
    std::unique_ptr<Context> ctx(rawCtx);
    const bool ok = !ctx->cntl.Failed();
    ctx->onDone(ok);
  }

  brpc::Channel channel_;
};

}  // namespace

int main(int argc, char** argv) {
  google::InitGoogleLogging(argv[0]);
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  if (FLAGS_input_gateway_addr.empty()) {
    LOG(ERROR) << "load_generator: --input_gateway_addr is required";
    return 1;
  }

  SubmitRequester requester(FLAGS_input_gateway_addr);

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

  sequencer::bench::LoadGenerator generator(requester, config);
  return generator.run() ? 0 : 1;
}
