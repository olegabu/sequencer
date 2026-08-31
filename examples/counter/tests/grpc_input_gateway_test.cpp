// The typed gRPC submission path (grpc_input_gateway_main.cpp), driven
// as a real deployment: a real counter_node and a real
// counter_grpc_input_gateway process, talking over real sockets, with
// a real gRPC client.
//
// It exists for one specific hazard. That gateway moved from a
// blocking propose() to proposeAsync(), which BATCHES: several
// concurrent submissions are packed into one ProposeBatch and their
// results come back positionally. A mis-sized or mis-ordered batch
// would therefore hand a client someone else's sequence number and
// total -- silently, and only under concurrency, which the sequential
// demo_grpc.sh walkthrough cannot see. gateway/input's own chassis has
// ConcurrentSubmitsEachGetTheirOwnSequenceNumber guarding the same
// path for the same reason; this is that guard for the typed path.

#include <grpcpp/grpcpp.h>

#include "counter_input_grpc.grpc.pb.h"

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <future>
#include <set>
#include <string>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

#include "child_process.hpp"

namespace sequencer::examples::counter {
namespace {

// One caller's submission and what came back for it.
struct Reply {
  std::int64_t delta = 0;
  std::int64_t sequenceNumber = 0;
  std::int64_t total = 0;
};

std::filesystem::path makeTempDir() {
  const std::filesystem::path dir =
      std::filesystem::temp_directory_path() /
      ("counter-grpc-input-test-" + std::to_string(::getpid()) + "-" +
       std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
  std::filesystem::create_directories(dir);
  return dir;
}

TEST(CounterGrpcInputGateway, ConcurrentSubmitsEachGetTheirOwnSequenceNumberAndTotal) {
  const std::filesystem::path nodeDataDir = makeTempDir();
  const std::string nodePeer = "127.0.0.1:28971:0";
  ChildProcess node(COUNTER_NODE_MAIN_PATH, {"--peer=" + nodePeer, "--peers=" + nodePeer,
                                              "--data_dir=" + nodeDataDir.string(),
                                              "--election_timeout_ms=300"});
  std::this_thread::sleep_for(std::chrono::milliseconds(500));

  ChildProcess gateway(COUNTER_GRPC_INPUT_GATEWAY_MAIN_PATH,
                        {"--node_peers=127.0.0.1:28971", "--listen_port=28972"});
  std::this_thread::sleep_for(std::chrono::milliseconds(500));

  auto channel = grpc::CreateChannel("127.0.0.1:28972", grpc::InsecureChannelCredentials());
  ASSERT_TRUE(channel->WaitForConnected(std::chrono::system_clock::now() +
                                         std::chrono::seconds(10)));

  // Every caller submits a DISTINCT delta, which is what makes a
  // mispairing detectable. An earlier version of this test had every
  // caller submit 1, and that cannot catch anything: with identical
  // requests, permuting the results within a batch still hands each
  // caller a self-consistent pair, and the set of sequence numbers is
  // still 1..N. It was verified against a deliberately reversed
  // result-pairing in NodeProposer::onBatchDone, which it passed --
  // hence this shape.
  constexpr int kConcurrency = 64;
  std::vector<std::future<Reply>> futures;
  futures.reserve(kConcurrency);
  for (int i = 0; i < kConcurrency; ++i) {
    const std::int64_t delta = i + 1;  // 1..64, all distinct
    futures.push_back(std::async(std::launch::async, [channel, delta]() {
      auto stub = grpc_proto::CounterSubmitService::NewStub(channel);
      grpc_proto::SubmitDeltaRequest request;
      request.set_delta(delta);
      grpc_proto::SubmitDeltaResponse response;
      grpc::ClientContext context;
      const grpc::Status status = stub->SubmitDelta(&context, request, &response);
      EXPECT_TRUE(status.ok()) << status.error_message();
      return Reply{delta, static_cast<std::int64_t>(response.sequence_number()),
                   static_cast<std::int64_t>(response.total())};
    }));
  }

  std::vector<Reply> replies;
  replies.reserve(kConcurrency);
  for (auto& f : futures) {
    replies.push_back(f.get());
  }

  // Dense and gap-free, 1..N (specification.md §2.1).
  std::set<std::int64_t> sequenceNumbers;
  for (const Reply& r : replies) {
    sequenceNumbers.insert(r.sequenceNumber);
  }
  ASSERT_EQ(sequenceNumbers.size(), static_cast<std::size_t>(kConcurrency));
  EXPECT_EQ(*sequenceNumbers.begin(), 1);
  EXPECT_EQ(*sequenceNumbers.rbegin(), kConcurrency);

  // The actual guard: in commit order, each caller's reported total
  // must exceed the previous one by exactly the delta THAT caller
  // submitted. A result handed to the wrong caller breaks this, because
  // the delta credited to a sequence number would not be the one the
  // state machine actually applied there.
  std::sort(replies.begin(), replies.end(),
            [](const Reply& a, const Reply& b) { return a.sequenceNumber < b.sequenceNumber; });
  std::int64_t running = 0;
  for (const Reply& r : replies) {
    running += r.delta;
    EXPECT_EQ(r.total, running)
        << "at sequence number " << r.sequenceNumber << ", the caller that submitted delta "
        << r.delta << " was told total " << r.total << " but the deltas committed up to there sum to "
        << running << " -- a batched result was paired with the wrong caller";
  }
}

}  // namespace
}  // namespace sequencer::examples::counter
