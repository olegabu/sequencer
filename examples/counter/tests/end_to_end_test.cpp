// specification.md §15 item 6: "an end-to-end test submitting through
// an input gateway and observing through an output gateway." Four real
// processes — counter_node, counter_input_gateway, and
// counter_output_gateway, the actual compiled binaries, not stand-ins —
// plus a real WebSocket client, proving the entire pipeline described
// in specification.md §3's topology diagram end to end:
//
//   client --[JSON over HTTP-style attachment]--> input gateway
//     --[Propose]--> node --[journal, colocated mmap]--> output gateway
//     --[WebSocket]--> client

#include "child_process.hpp"

#include <sequencer/journal/reader.hpp>

#include <brpc/channel.h>
#include <brpc/controller.h>

#include <boost/asio/connect.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/beast/core.hpp>
#include <boost/beast/websocket.hpp>

#include "input_gateway.pb.h"

#include <chrono>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

namespace sequencer::examples::counter {
namespace {

namespace beast = boost::beast;
namespace websocket = beast::websocket;
namespace net = boost::asio;
using tcp = net::ip::tcp;

std::filesystem::path makeTempDir() {
  std::string tmpl = (std::filesystem::temp_directory_path() / "counter_e2e_XXXXXX").string();
  if (::mkdtemp(tmpl.data()) == nullptr) {
    throw std::runtime_error("mkdtemp failed");
  }
  return tmpl;
}

// A minimal synchronous WebSocket client — see
// gateway/output/tests's and websocket_transport_test.cpp's identical
// pattern; not shared into child_process.hpp since it's Beast-specific
// and this is currently its only other use.
class TestWsClient {
 public:
  // "/totals": sequencer::WebSocketOutputTransport (gateway/output/)
  // routes a connecting client's topic from the WebSocket URL's
  // request path — CounterOutputCodec broadcasts to "totals", so that's
  // what a client must connect to in order to receive anything.
  explicit TestWsClient(int port) : ws_(ioContext_) {
    tcp::resolver resolver(ioContext_);
    const auto results = resolver.resolve("127.0.0.1", std::to_string(port));
    net::connect(ws_.next_layer(), results);
    ws_.handshake("127.0.0.1", "/totals");
  }

  std::string readOne() {
    beast::flat_buffer buffer;
    ws_.read(buffer);
    return beast::buffers_to_string(buffer.data());
  }

 private:
  net::io_context ioContext_;
  websocket::stream<tcp::socket> ws_;
};

std::unique_ptr<TestWsClient> connectWithRetry(int port, std::chrono::seconds timeout) {
  const auto deadline = std::chrono::steady_clock::now() + timeout;
  while (std::chrono::steady_clock::now() < deadline) {
    try {
      return std::make_unique<TestWsClient>(port);
    } catch (const std::exception&) {
      std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
  }
  return nullptr;
}

struct SubmitResult {
  bool ok = false;
  std::string body;
  std::string errorText;
};

SubmitResult submitDelta(brpc::Channel& channel, std::int64_t delta) {
  sequencer::gateway::input::proto::SubmitService_Stub stub(&channel);
  sequencer::gateway::input::proto::SubmitRequest request;
  sequencer::gateway::input::proto::SubmitResponse response;
  brpc::Controller cntl;
  const std::string body = "{\"delta\": " + std::to_string(delta) + "}";
  cntl.request_attachment().append(body);
  stub.Submit(&cntl, &request, &response, nullptr);

  SubmitResult result;
  result.ok = !cntl.Failed();
  if (result.ok) {
    result.body = cntl.response_attachment().to_string();
  } else {
    result.errorText = cntl.ErrorText();
  }
  return result;
}

TEST(CounterEndToEnd, SubmitThroughInputGatewayIsObservedThroughOutputGateway) {
  const std::filesystem::path nodeDataDir = makeTempDir();
  const std::filesystem::path resumeFile = makeTempDir() / "resume";

  const std::string nodePeer = "127.0.0.1:28981:0";
  ChildProcess node(COUNTER_NODE_MAIN_PATH, {"--peer=" + nodePeer, "--peers=" + nodePeer,
                                              "--data_dir=" + nodeDataDir.string(),
                                              "--election_timeout_ms=300"});

  // Give the single-node group a moment to self-elect before pointing
  // gateways at it — the input gateway's own NodeProposer would
  // eventually succeed via retries regardless, but this keeps the test
  // from spending its own timeout budget on that instead of the actual
  // pipeline.
  std::this_thread::sleep_for(std::chrono::milliseconds(500));

  ChildProcess inputGateway(COUNTER_INPUT_GATEWAY_MAIN_PATH,
                             {"--node_peers=127.0.0.1:28981", "--listen_port=28982"});
  ChildProcess outputGateway(COUNTER_OUTPUT_GATEWAY_MAIN_PATH,
                              {"--data_dir=" + nodeDataDir.string(),
                               "--resume_file=" + resumeFile.string(), "--listen_port=28983"});

  // Connect the WebSocket observer before submitting anything — the
  // output gateway's Fanout delivers live only (see
  // gateway/output/README.md), so a late subscriber would miss earlier
  // broadcasts.
  std::unique_ptr<TestWsClient> observer = connectWithRetry(28983, std::chrono::seconds(5));
  ASSERT_NE(observer, nullptr) << "failed to connect to the output gateway's WebSocket endpoint";
  // As in websocket_transport_test.cpp: the client's handshake can
  // return slightly before the server finishes registering the
  // session on its own io thread.
  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  brpc::Channel inputChannel;
  brpc::ChannelOptions channelOptions;
  channelOptions.timeout_ms = 3000;
  ASSERT_EQ(inputChannel.Init("127.0.0.1:28982", &channelOptions), 0);

  const std::vector<std::int64_t> deltas = {5, -2, 10};
  std::int64_t expectedTotal = 0;
  for (std::size_t i = 0; i < deltas.size(); ++i) {
    SubmitResult result = submitDelta(inputChannel, deltas[i]);
    ASSERT_TRUE(result.ok) << result.errorText;
    expectedTotal += deltas[i];
    const std::string expectedResponse =
        "{\"sequence_number\":" + std::to_string(i + 1) + ",\"total\":" + std::to_string(expectedTotal) + "}";
    EXPECT_EQ(result.body, expectedResponse) << "input gateway response for delta " << deltas[i];

    const std::string observed = observer->readOne();
    EXPECT_EQ(observed, expectedResponse) << "output gateway broadcast for delta " << deltas[i];
  }

  std::filesystem::remove_all(nodeDataDir);
  std::filesystem::remove_all(resumeFile.parent_path());
}

}  // namespace
}  // namespace sequencer::examples::counter
