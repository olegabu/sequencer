// Unit tests for CounterInputCodec and CounterOutputCodec in isolation
// — no gateway, no transport, no node. specification.md §10's JSON
// shapes: {"delta": <integer>} in, {"sequence_number": ..., "total": ...}
// out, on both the input and output side.

#include "../counter_input_codec.hpp"
#include "../counter_client_id.hpp"
#include "../counter_output_codec.hpp"

#include <sequencer/journal/record_view.hpp>

#include <cstring>
#include <span>
#include <string>
#include <vector>

#include <gtest/gtest.h>

namespace sequencer::examples::counter {
namespace {

Payload payloadOf(const std::string& s) {
  return Payload(reinterpret_cast<const std::byte*>(s.data()), s.size());
}

// --- CounterInputCodec ---

TEST(CounterInputCodec, ParsesSimpleDeltaBody) {
  CounterInputCodec codec;
  // A named local, not payloadOf("literal") inline in the braced-init:
  // the temporary std::string bound to payloadOf's parameter would be
  // destroyed at the end of *this* statement, leaving `request.body`
  // dangling by the time toInput() reads it on the next line.
  const std::string body = R"({"delta": 5})";
  ClientRequest request{payloadOf(body)};
  Result<Bytes> result = codec.toInput(request);
  ASSERT_TRUE(result.ok());
  ASSERT_EQ(result.value().size(), sizeof(std::int64_t));
  std::int64_t delta;
  std::memcpy(&delta, result.value().data(), sizeof(delta));
  EXPECT_EQ(delta, 5);
}

TEST(CounterInputCodec, ParsesNegativeDeltaAndTolerantWhitespace) {
  CounterInputCodec codec;
  const std::string body = R"({ "delta" : -13 })";
  ClientRequest request{payloadOf(body)};
  Result<Bytes> result = codec.toInput(request);
  ASSERT_TRUE(result.ok());
  std::int64_t delta;
  std::memcpy(&delta, result.value().data(), sizeof(delta));
  EXPECT_EQ(delta, -13);
}

TEST(CounterInputCodec, RejectsMissingDeltaField) {
  CounterInputCodec codec;
  const std::string body = R"({"amount": 5})";
  ClientRequest request{payloadOf(body)};
  Result<Bytes> result = codec.toInput(request);
  EXPECT_FALSE(result.ok());
  EXPECT_FALSE(result.error().empty());
}

TEST(CounterInputCodec, RejectsNonJsonBody) {
  CounterInputCodec codec;
  const std::string body = "not json at all";
  ClientRequest request{payloadOf(body)};
  Result<Bytes> result = codec.toInput(request);
  EXPECT_FALSE(result.ok());
}

TEST(CounterInputCodec, ToOutputProducesSequenceNumberAndTotalJson) {
  CounterInputCodec codec;
  const std::int64_t total = 42;
  Receipt receipt{7};
  const Payload designated[] = {Payload(reinterpret_cast<const std::byte*>(&total), sizeof(total))};
  Bytes response = codec.toOutput(receipt, std::span<const Payload>(designated, 1));
  const std::string json(reinterpret_cast<const char*>(response.data()), response.size());
  EXPECT_EQ(json, R"({"sequence_number":7,"total":42})");
}

// specification.md §4 lets a state machine designate nothing, and
// §8.11 has the chassis hand this codec an empty span on a
// SessionStream transport regardless of what was designated. Either
// way the codec must produce a well-formed response rather than read
// past an empty span.
TEST(CounterInputCodec, ToOutputHandlesAnEmptyDesignatedSet) {
  CounterInputCodec codec;
  Receipt receipt{9};
  Bytes response = codec.toOutput(receipt, std::span<const Payload>());
  const std::string json(reinterpret_cast<const char*>(response.data()), response.size());
  EXPECT_EQ(json, R"({"sequence_number":9,"total":0})");
}

TEST(CounterInputCodec, OnDisconnectReturnsNulloptForStatelessProtocol) {
  CounterInputCodec codec;
  EXPECT_FALSE(codec.onDisconnect(SessionInfo{1}).has_value());
}

// --- CounterOutputCodec ---

class RecordingFanout : public Fanout {
 public:
  void toSession(SessionId, Bytes) override { FAIL() << "counter never addresses a specific session"; }
  void broadcast(const std::string& topic, Bytes bytes) override {
    lastTopic = topic;
    lastMessage.assign(reinterpret_cast<const char*>(bytes.data()), bytes.size());
    calls++;
  }

  std::string lastTopic;
  std::string lastMessage;
  int calls = 0;
};

journal::RecordView makeRecordView(std::vector<std::byte>& buffer, std::uint64_t seq,
                                    Payload input, const std::vector<Payload>& outputs) {
  buffer.resize(journal::recordEncodedSize(input, outputs));
  journal::encodeRecord(buffer.data(), seq, input, outputs);
  return journal::RecordView(buffer.data(), static_cast<std::uint32_t>(buffer.size()));
}

TEST(CounterOutputCodec, BroadcastsSequenceNumberAndTotalToTheSubmittersTopic) {
  const std::int64_t total = 13;
  const Payload output0(reinterpret_cast<const std::byte*>(&total), sizeof(total));
  // An input too short to carry a client id belongs to client 0, which
  // is the case every ordinary use of the counter takes.
  std::vector<std::byte> buffer;
  const journal::RecordView record = makeRecordView(buffer, 3, payloadOf("irrelevant-input"), {output0});

  CounterOutputCodec codec;
  RecordingFanout fanout;
  codec.toOutput(record, fanout);

  EXPECT_EQ(fanout.calls, 1);
  EXPECT_EQ(fanout.lastTopic, "totals-0");
  EXPECT_EQ(fanout.lastMessage, R"({"sequence_number":3,"total":13})");
}

// The routing that makes a multi-client sweep fair: two clients'
// records go to two different topics, so neither receives the other's
// traffic. Without this the gateway delivers (rate x subscribers).
TEST(CounterOutputCodec, RoutesEachClientsTotalToItsOwnTopic) {
  CounterOutputCodec codec;
  for (const std::int64_t clientId : {0, 1, 4}) {
    const std::int64_t delta = counterDeltaFor(clientId, 7);
    const std::int64_t total = 13;
    const Payload output0(reinterpret_cast<const std::byte*>(&total), sizeof(total));
    const Payload input(reinterpret_cast<const std::byte*>(&delta), sizeof(delta));
    std::vector<std::byte> buffer;
    const journal::RecordView record = makeRecordView(buffer, 3, input, {output0});

    RecordingFanout fanout;
    codec.toOutput(record, fanout);
    EXPECT_EQ(fanout.lastTopic, "totals-" + std::to_string(clientId));
  }
}

TEST(CounterOutputCodec, DefaultsToZeroTotalWhenRecordHasNoOutputs) {
  std::vector<std::byte> buffer;
  const journal::RecordView record = makeRecordView(buffer, 1, payloadOf("input"), {});

  CounterOutputCodec codec;
  RecordingFanout fanout;
  codec.toOutput(record, fanout);

  EXPECT_EQ(fanout.lastMessage, R"({"sequence_number":1,"total":0})");
}

}  // namespace
}  // namespace sequencer::examples::counter
