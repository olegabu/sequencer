// Tests for WebSocketOutputTransport in isolation — a real Boost.Beast
// WebSocket client connects over a real socket, and the test drives
// Fanout::broadcast/toSession directly, verifying actual network
// delivery (not just that the code compiles against Beast's API).

#include <sequencer/websocket_output_transport.hpp>

#include <boost/asio/connect.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/beast/core.hpp>
#include <boost/beast/websocket.hpp>

#include <chrono>
#include <memory>
#include <stdexcept>
#include <string>
#include <thread>

#include <gtest/gtest.h>

namespace sequencer {
namespace {

namespace beast = boost::beast;
namespace websocket = beast::websocket;
namespace net = boost::asio;
using tcp = net::ip::tcp;

// A minimal synchronous WebSocket client. Single-threaded and touches
// its own stream only from this one thread, so Beast's "shared
// objects: unsafe" rule (the same one that shapes
// WebSocketOutputTransport's own design) simply doesn't come up here.
class TestWsClient {
 public:
  TestWsClient(int port, const std::string& topic) : ws_(ioContext_) {
    tcp::resolver resolver(ioContext_);
    const auto results = resolver.resolve("127.0.0.1", std::to_string(port));
    net::connect(ws_.next_layer(), results);
    ws_.handshake("127.0.0.1", "/" + topic);
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

Bytes bytesOf(const std::string& s) {
  return Bytes(reinterpret_cast<const std::byte*>(s.data()), reinterpret_cast<const std::byte*>(s.data()) + s.size());
}

std::unique_ptr<TestWsClient> connectWithRetry(int port, const std::string& topic, std::chrono::seconds timeout) {
  const auto deadline = std::chrono::steady_clock::now() + timeout;
  while (std::chrono::steady_clock::now() < deadline) {
    try {
      return std::make_unique<TestWsClient>(port, topic);
    } catch (const std::exception&) {
      std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
  }
  return nullptr;
}

TEST(WebSocketOutputTransport, BroadcastDeliversToConnectedClient) {
  WebSocketOutputTransport transport;
  transport.start(28971);

  std::unique_ptr<TestWsClient> client = connectWithRetry(28971, "totals", std::chrono::seconds(2));
  ASSERT_NE(client, nullptr);
  // The client's handshake() can return as soon as it sees the HTTP 101
  // response, which races slightly ahead of the server's own
  // registerConnection() call completing on its io thread; a brief
  // pause avoids a broadcast landing before the session is registered.
  std::this_thread::sleep_for(std::chrono::milliseconds(50));

  transport.broadcast("totals", bytesOf(R"({"sequence_number":1,"total":5})"));

  const std::string message = client->readOne();
  EXPECT_EQ(message, R"({"sequence_number":1,"total":5})");

  transport.stop();
}

TEST(WebSocketOutputTransport, MultipleMessagesArriveInOrder) {
  WebSocketOutputTransport transport;
  transport.start(28972);

  std::unique_ptr<TestWsClient> client = connectWithRetry(28972, "totals", std::chrono::seconds(2));
  ASSERT_NE(client, nullptr);
  std::this_thread::sleep_for(std::chrono::milliseconds(50));

  transport.broadcast("totals", bytesOf("first"));
  transport.broadcast("totals", bytesOf("second"));
  transport.broadcast("totals", bytesOf("third"));

  EXPECT_EQ(client->readOne(), "first");
  EXPECT_EQ(client->readOne(), "second");
  EXPECT_EQ(client->readOne(), "third");

  transport.stop();
}

TEST(WebSocketOutputTransport, BroadcastToUnknownTopicIsANoOp) {
  WebSocketOutputTransport transport;
  transport.start(28973);
  // No client connected at all; this must not crash or hang.
  transport.broadcast("nobody-subscribed", bytesOf("hello"));
  transport.stop();
}

TEST(WebSocketOutputTransport, TwoClientsOnDifferentTopicsOnlyReceiveTheirOwn) {
  WebSocketOutputTransport transport;
  transport.start(28974);

  std::unique_ptr<TestWsClient> totalsClient = connectWithRetry(28974, "totals", std::chrono::seconds(2));
  std::unique_ptr<TestWsClient> alertsClient = connectWithRetry(28974, "alerts", std::chrono::seconds(2));
  ASSERT_NE(totalsClient, nullptr);
  ASSERT_NE(alertsClient, nullptr);
  std::this_thread::sleep_for(std::chrono::milliseconds(50));

  transport.broadcast("totals", bytesOf("for totals"));
  transport.broadcast("alerts", bytesOf("for alerts"));

  EXPECT_EQ(totalsClient->readOne(), "for totals");
  EXPECT_EQ(alertsClient->readOne(), "for alerts");

  transport.stop();
}

}  // namespace
}  // namespace sequencer
