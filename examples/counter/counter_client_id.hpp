#pragma once

// How a benchmark client tells the counter's output codecs which client
// a record came from, so each one receives only its own replies.
//
// Shared by BOTH output codecs -- the JSON one behind brpc/gRPC/
// WebSocket and the FIX one -- because the problem is identical and the
// encoding must be too. It lived in counter_fix_codecs.hpp first; the
// JSON path was left broadcasting to a single "totals" topic, which
// made any multi-client comparison between the two unfair in the
// gateway's favour on the FIX side.
//
// A single shared topic is what the obvious implementation does, and it
// does not scale: every subscriber receives every output, so a
// gateway's delivery load is (rate x subscribers) rather than rate. On
// a five-client run at 100k that is 500k messages/sec of delivery for
// 100k of offered load.
//
// The client id rides in the HIGH BITS of the submitted delta:
//
//     delta = (clientId << kClientIdShift) | sequence
//
// which costs nothing, because the delta is an opaque int64 the state
// machine only sums. An output codec recovers the id by shifting and
// publishes to that client's own topic, so fan-out is exactly one.
//
// This is a benchmark technique, not a pattern to copy. A real
// application addresses Fanout::toSession with the owning session; the
// counter cannot, because its input is eight bytes of delta and carries
// no session.

#include <cstdint>
#include <string>

namespace sequencer::examples::counter {

inline constexpr int kClientIdShift = 40;

// The client id encoded in a submitted delta, or 0 if it carries none.
//
// A delta that is NEGATIVE or too small to hold an id belongs to client
// 0. That covers every ordinary use of the counter -- a person sending
// -2 through the demo is not encoding anything -- and it has to be
// stated because the arithmetic bites otherwise: -2 >> 40 is -1, not 0,
// so a negative delta published to a "-1" topic nobody subscribes to,
// and its total simply vanished. The end-to-end test caught it because
// it submits 5 then -2.
inline std::int64_t counterClientIdOf(std::int64_t delta) {
  return delta > 0 ? (delta >> kClientIdShift) : 0;
}

// What a client submits so its replies come back on its own topic.
inline std::int64_t counterDeltaFor(std::int64_t clientId, std::int64_t sequence) {
  return (clientId << kClientIdShift) | sequence;
}

// The JSON path's per-client topic (brpc / gRPC / WebSocket).
//
// Uniform for every id, INCLUDING 0: "totals-0" rather than a bare
// "totals" for the first client. A special case at zero would be one
// more rule to remember in every sweep template and demo, for no gain.
inline std::string counterTotalsTopicFor(std::int64_t clientId) {
  return "totals-" + std::to_string(clientId);
}

}  // namespace sequencer::examples::counter
