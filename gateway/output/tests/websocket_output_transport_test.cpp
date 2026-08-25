// Tests for WebSocketOutputTransport in isolation — a real Boost.Beast
// WebSocket client connects over a real socket, and the test drives
// Fanout::broadcast/toSession directly, verifying actual network
// delivery (not just that the code compiles against Beast's API).
//
// Every broadcast()/toSession() call here is followed by an explicit
// flush() — unlike production use (OutputGatewayImpl::tailLoop() calls
// it once per gathered batch), these tests call the transport directly
// with nothing else driving that cadence, and broadcast()/toSession()
// only *accumulate* now (see websocket_output_transport.cpp's own top
// comment) — without an explicit flush() nothing is ever actually
// sent, which is exactly the hang this comment exists to prevent
// re-introducing.

#include <sequencer/websocket_output_transport.hpp>

#include <boost/asio/connect.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/beast/core.hpp>
#include <boost/beast/websocket.hpp>

#include <chrono>
#include <cstdint>
#include <deque>
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

  // Reads span whatever frame shape the server happened to send them
  // in (see websocket_output_transport.cpp's own batching comment) —
  // doles out one payload at a time regardless, pulling a fresh frame
  // off the socket whenever the buffered one is exhausted.
  std::string readOne() {
    if (buffered_.empty()) {
      beast::flat_buffer buffer;
      ws_.read(buffer);
      const std::string frame = beast::buffers_to_string(buffer.data());
      std::size_t offset = 0;
      while (offset + 4 <= frame.size()) {
        const auto length = static_cast<std::uint32_t>(static_cast<unsigned char>(frame[offset]) << 24 |
                                                          static_cast<unsigned char>(frame[offset + 1]) << 16 |
                                                          static_cast<unsigned char>(frame[offset + 2]) << 8 |
                                                          static_cast<unsigned char>(frame[offset + 3]));
        offset += 4;
        if (offset + length > frame.size()) {
          break;  // truncated frame; shouldn't happen, drop rather than misparse
        }
        buffered_.push_back(frame.substr(offset, length));
        offset += length;
      }
      if (buffered_.empty()) {
        throw std::runtime_error("server sent a frame with no decodable payloads");
      }
    }
    std::string payload = std::move(buffered_.front());
    buffered_.pop_front();
    return payload;
  }

 private:
  net::io_context ioContext_;
  websocket::stream<tcp::socket> ws_;
  std::deque<std::string> buffered_;
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
  transport.flush();

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
  transport.flush();

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
  transport.flush();

  EXPECT_EQ(totalsClient->readOne(), "for totals");
  EXPECT_EQ(alertsClient->readOne(), "for alerts");

  transport.stop();
}

}  // namespace
}  // namespace sequencer
