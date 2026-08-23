// specification.md §10: "a small open-loop-capable client, exercised in
// tests — a smoke test and a rough throughput/latency sanity check, not
// a substitute for the benchmarking repository." Open-loop: requests
// are scheduled at a fixed target rate regardless of how quickly (or
// slowly) responses come back — unlike a closed-loop client, which
// would wait for each response before sending the next, silently
// self-throttling to whatever the system's own latency imposes.

#include <brpc/callback.h>
#include <brpc/channel.h>
#include <brpc/controller.h>

#include <gflags/gflags.h>
#include <glog/logging.h>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <iostream>
#include <memory>
#include <random>
#include <string>
#include <thread>

#include "input_gateway.pb.h"

DEFINE_string(input_gateway_addr, "", "The input gateway's \"ip:port\" to submit to (required)");
DEFINE_int32(count, 100, "Number of requests to submit");
DEFINE_double(rate, 100.0,
              "Target requests/second, open-loop — requests are scheduled at this fixed rate "
              "regardless of response latency. 0 means send as fast as this thread can loop.");

namespace {

std::atomic<int> gSucceeded{0};
std::atomic<int> gFailed{0};
std::atomic<int> gCompleted{0};

struct RequestContext {
  brpc::Controller cntl;
  sequencer::gateway::input::proto::SubmitRequest request;
  sequencer::gateway::input::proto::SubmitResponse response;
};

// brpc::NewCallback's closure self-deletes after this runs (see
// brpc/callback.h) — RequestContext is a separate allocation this
// function owns and must free itself.
void onRpcDone(RequestContext* rawCtx) {
  std::unique_ptr<RequestContext> ctx(rawCtx);
  if (ctx->cntl.Failed()) {
    gFailed.fetch_add(1, std::memory_order_relaxed);
  } else {
    gSucceeded.fetch_add(1, std::memory_order_relaxed);
  }
  gCompleted.fetch_add(1, std::memory_order_relaxed);
}

}  // namespace

int main(int argc, char** argv) {
  google::InitGoogleLogging(argv[0]);
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  if (FLAGS_input_gateway_addr.empty()) {
    std::cerr << "load_generator: --input_gateway_addr is required" << std::endl;
    return 1;
  }

  brpc::Channel channel;
  brpc::ChannelOptions channelOptions;
  channelOptions.timeout_ms = 2000;
  if (channel.Init(FLAGS_input_gateway_addr.c_str(), &channelOptions) != 0) {
    std::cerr << "load_generator: failed to connect to " << FLAGS_input_gateway_addr << std::endl;
    return 1;
  }
  sequencer::gateway::input::proto::SubmitService_Stub stub(&channel);

  std::mt19937_64 rng(std::random_device{}());
  std::uniform_int_distribution<std::int64_t> deltaDist(-100, 100);

  const auto start = std::chrono::steady_clock::now();
  const std::chrono::duration<double> interval(FLAGS_rate > 0 ? 1.0 / FLAGS_rate : 0.0);

  for (int i = 0; i < FLAGS_count; ++i) {
    if (FLAGS_rate > 0) {
      std::this_thread::sleep_until(start + i * interval);
    }

    const std::string body = "{\"delta\": " + std::to_string(deltaDist(rng)) + "}";
    auto* ctx = new RequestContext();
    ctx->cntl.request_attachment().append(body);
    stub.Submit(&ctx->cntl, &ctx->request, &ctx->response, brpc::NewCallback(&onRpcDone, ctx));
  }

  while (gCompleted.load(std::memory_order_relaxed) < FLAGS_count) {
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
  }

  const double seconds = std::chrono::duration<double>(std::chrono::steady_clock::now() - start).count();
  std::cout << "load_generator: sent=" << FLAGS_count << " succeeded=" << gSucceeded.load()
            << " failed=" << gFailed.load() << " elapsed=" << seconds << "s"
            << " throughput=" << (seconds > 0 ? FLAGS_count / seconds : 0.0) << " req/s" << std::endl;

  return gFailed.load() > 0 ? 1 : 0;
}
