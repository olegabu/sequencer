#pragma once

// One of three output-gateway observers (see sequence_correlator.hpp's
// own file comment for the shared design) — this one subscribes to
// WebSocketOutputTransport (gateway/output/include/sequencer/
// websocket_output_transport.hpp) via a real Boost.Beast client,
// structurally the same synchronous connect/handshake/read shape
// gateway/output/tests/websocket_output_transport_test.cpp's own
// test-only TestWsClient already proves out, made production-grade: a
// dedicated reader thread (Beast's synchronous API blocks, so this
// needs a thread of its own, same as GrpcOutputObserver's — unlike
// BrpcOutputObserver, whose transport delivers via an async callback
// already) feeding a real correlator instead of a snapshot vector.
//
// Topic is part of the URL path, not a request field
// (websocket_output_transport.hpp:11-19) — "/" + topic, matching
// TestWsClient's own handshake() call exactly.

#include <boost/asio/connect.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/beast/core.hpp>
#include <boost/beast/websocket.hpp>

#include "sequence_correlator.hpp"

#include <atomic>
#include <chrono>
#include <cstdint>
#include <stdexcept>
#include <string>
#include <thread>

namespace sequencer::bench {

class WebSocketOutputObserver final : public OutputGatewayObserver {
 public:
  // `outputGatewayAddr`: "host:port", split on the last ':' (matching
  // the same address shape the gRPC/brpc observers take, rather than
  // exposing a host/port pair just for this one transport).
  // `topic`/`extractor`/`ringCapacityPow2`/`correlatorThreads`: see
  // GrpcOutputObserver's own constructor comment.
  WebSocketOutputObserver(std::string outputGatewayAddr, std::string topic,
                           SequenceCorrelator::SequenceExtractor extractor, std::size_t ringCapacityPow2,
                           int correlatorThreads = 4)
      : topic_(std::move(topic)), correlator_(std::move(extractor), ringCapacityPow2, correlatorThreads),
        ws_(ioContext_) {
    const std::size_t colon = outputGatewayAddr.rfind(':');
    if (colon == std::string::npos) {
      throw std::runtime_error("WebSocketOutputObserver: address must be \"host:port\", got \"" +
                                outputGatewayAddr + "\"");
    }
    host_ = outputGatewayAddr.substr(0, colon);
    port_ = outputGatewayAddr.substr(colon + 1);
  }

  ~WebSocketOutputObserver() override { stop(); }

  WebSocketOutputObserver(const WebSocketOutputObserver&) = delete;
  WebSocketOutputObserver& operator=(const WebSocketOutputObserver&) = delete;

  // Call once, before the load generator starts sending — same
  // requirement as the other two observers. Throws on connection or
  // handshake failure, same convention BrpcOutputObserver::start()
  // uses.
  void start() override {
    correlator_.start();
    namespace net = boost::asio;
    using tcp = net::ip::tcp;
    tcp::resolver resolver(ioContext_);
    const auto results = resolver.resolve(host_, port_);
    net::connect(ws_.next_layer(), results);
    ws_.handshake(host_, "/" + topic_);
    readerThread_ = std::thread([this] { readLoop(); });
  }

  void setMeasurementWindow(std::int64_t measureStartUs, std::int64_t measureEndUs) override {
    correlator_.setMeasurementWindow(measureStartUs, measureEndUs);
  }

  // Idempotent, safe to omit (the destructor calls it too). Beast's
  // synchronous read() has no bounded-wait/cancel of its own — the
  // standard way to unblock a blocking read from another thread is to
  // shut down the underlying socket, which makes the pending read()
  // fail immediately with an error the reader loop treats as "stop."
  void stop() override {
    if (!stopRequested_.exchange(true, std::memory_order_relaxed)) {
      boost::system::error_code ec;
      ws_.next_layer().shutdown(boost::asio::ip::tcp::socket::shutdown_both, ec);
      ws_.next_layer().close(ec);
    }
    if (readerThread_.joinable()) {
      readerThread_.join();
    }
    correlator_.stop();
  }

  void recordSend(std::uint64_t sequenceNumber, std::int64_t sendTimeUs) noexcept override {
    correlator_.recordSend(sequenceNumber, sendTimeUs);
  }

  void printSummary() const override {
    std::printf("\n=== output-gateway (websocket) observed summary ===\n");
    std::printf(
        "(submission to receipt via WebSocketOutputTransport's real WebSocket --\n"
        " not the synchronous ack path above; see bench/load_generator/README.md)\n");
    correlator_.printSummary("output_websocket");
  }

 private:
  static std::int64_t nowMicros() {
    return std::chrono::duration_cast<std::chrono::microseconds>(
               std::chrono::steady_clock::now().time_since_epoch())
        .count();
  }

  // The only job this thread has: keep calling read() as fast as
  // Beast can deliver, capturing the arrival instant immediately and
  // handing each payload straight to the correlator — same discipline
  // GrpcOutputObserver::readLoop() and RelayObserver::readLoop() both
  // follow, for the same reason (see sequence_correlator.hpp's own
  // top comment). Each frame may itself be a batch of several
  // length-prefixed payloads now (WebSocketOutputTransport::flush(),
  // gateway/output/src/websocket_output_transport.cpp) — see that
  // file's own top comment for why a raw concatenation wouldn't decode
  // correctly and what the 4-byte big-endian length prefix is for.
  void readLoop() {
    boost::beast::flat_buffer buffer;
    while (!stopRequested_.load(std::memory_order_relaxed)) {
      boost::system::error_code ec;
      ws_.read(buffer, ec);
      if (ec) {
        break;  // socket closed (stop()) or a genuine transport error either way
      }
      const std::int64_t nowUs = nowMicros();
      const std::string frame = boost::beast::buffers_to_string(buffer.data());
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
        correlator_.deliver(frame.substr(offset, length), nowUs);
        offset += length;
      }
      buffer.consume(buffer.size());
    }
  }

  std::string host_;
  std::string port_;
  std::string topic_;
  SequenceCorrelator correlator_;
  std::atomic<bool> stopRequested_{false};
  std::thread readerThread_;
  boost::asio::io_context ioContext_;
  boost::beast::websocket::stream<boost::asio::ip::tcp::socket> ws_;
};

}  // namespace sequencer::bench
