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

  // Addresses one output to one session. specification.md §8.11: on a
  // session transport this is the ONLY path a client's outputs take --
  // every output addressed to the session, designated or not, in
  // sequence-number order. A codec must therefore route a designated
  // output here exactly as it routes any other, and must not skip one
  // on the assumption the input side already replied with it; on a
  // session transport it did not.
  virtual void toSession(SessionId owner, Bytes bytes) = 0;
  virtual void broadcast(const std::string& topic, Bytes bytes) = 0;
};

// specification.md §8.11: a gateway delivers each output to a given
// client exactly once, by the path its transport shape dictates, and
// never by both.
//
// This is the output side of that rule. For a SESSION transport, every
// output addressed to a session -- designated or not -- is delivered
// here, from the journal, in sequence-number order; the input side's
// synchronous reply carries none of them. That ordering is the point:
// a client's fills across different orders arrive in journal order,
// which position and risk logic downstream depend on, and no output is
// ever sent both as a synchronous reply and again from here.
//
// For a request/response transport the division is the other way: the
// input side returns the designated outputs synchronously, and this
// path carries everything else to its audience.
//
// No code change is required of a codec either way -- it emits what the
// record says, and the ROUTING (Fanout::toSession vs broadcast) decides
// the audience. The rule is recorded here because a codec author is
// exactly who would otherwise be tempted to special-case a designated
// output on this side and duplicate it.
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
