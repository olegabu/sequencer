#pragma once

// specification.md §8.5: the output-side plug point. The chassis
// (RunOutputGateway) owns journal tailing, the durable resume position,
// transport, and connection lifecycle; the codec owns interpretation
// and routing.

#include <cstdint>
#include <string>

#include <sequencer/journal/record_view.hpp>

namespace sequencer {

using SessionId = std::uint64_t;

// What a codec publishes into, in place of touching any transport
// directly (specification.md §8.5: "Fanout: toSession(owner, bytes) |
// broadcast(topic, bytes).").
class Fanout {
 public:
  virtual ~Fanout() = default;
  virtual void toSession(SessionId owner, Bytes bytes) = 0;
  virtual void broadcast(const std::string& topic, Bytes bytes) = 0;

  // Signals "no more toSession()/broadcast() calls are coming right
  // now" — an opportunity for a transport that buffers writes
  // internally to send everything accumulated as one round trip
  // instead of one per call. OutputGatewayImpl::tailLoop() calls this
  // once after processing a batch of consecutive available records
  // (and once more, unconditionally, from stop() — see there), not
  // once per record. Default no-op: a transport that already writes
  // immediately per call, or manages its own batching independently
  // (GrpcOutputTransport's own consumer-side drain, WebSocketOutput
  // Transport's own per-connection write queue), has nothing to do
  // here.
  virtual void flush() {}
};

class OutputCodec {
 public:
  virtual ~OutputCodec() = default;

  // record -> published bytes, in order, exactly once
  // (specification.md §8.3). Called once per journal record, in
  // sequence-number order; the codec decides per call whether to
  // toSession, broadcast, both, or neither.
  virtual void toOutput(const journal::RecordView& record, Fanout& fanout) = 0;
};

}  // namespace sequencer
