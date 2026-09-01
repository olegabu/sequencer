#pragma once

// specification.md §8.5: the input-side plug point. The chassis
// (RunInputGateway) owns sessions, authentication, transport security,
// signature verification, leader tracking, and retries; the codec owns
// meaning — translating a client's wire bytes into the state machine's
// input bytes, and a receipt back into the client's own response
// format.

#include <cstdint>
#include <optional>
#include <span>
#include <string>
#include <utility>
#include <variant>

#include <sequencer/payload.hpp>

namespace sequencer {

// Whatever the client sent, reduced to what a codec needs. The chassis
// has already handled the protocol, session, and authentication layer
// by the time a codec sees this — see input_gateway.hpp's
// SubmitServiceImpl for exactly how `body` is populated regardless of
// whether the client spoke HTTP+JSON, gRPC, or baidu_std.
struct ClientRequest {
  Payload body;

  // The session this arrived on, or 0 for a sessionless transport
  // (brpc, REST, gRPC unary -- there is nothing to correlate a later
  // output with, because the reply goes back on the request).
  //
  // An application that wants its outputs addressed back to the
  // submitting session must carry this into the input it proposes: the
  // journal record is all an OutputCodec sees, so a session id that is
  // not in the record cannot be recovered from it. See
  // examples/counter/counter_fix_codecs.hpp for a worked account of
  // what that costs and why the counter example does not do it.
  std::uint64_t sessionId = 0;
};

// Enough for InputCodec::onDisconnect (specification.md §8.1: "propose
// a disconnect input on session loss, if the state machine defines
// one") to identify which session went away.
struct SessionInfo {
  std::uint64_t sessionId;
};

// A minimal ok-value-or-error-message result — specification.md §8.5's
// `Result<Bytes>`, toInput's one use case: a malformed client request
// is a routine, expected occurrence the gateway must respond to
// gracefully, not throw an exception over.
template <typename T>
class Result {
 public:
  static Result Ok(T value) { return Result(std::move(value), Tag::kOk); }
  static Result Error(std::string message) { return Result(std::move(message), Tag::kError); }

  bool ok() const noexcept { return std::holds_alternative<T>(value_); }
  const T& value() const { return std::get<T>(value_); }
  const std::string& error() const { return std::get<std::string>(value_); }

 private:
  enum class Tag { kOk, kError };
  Result(T value, Tag) : value_(std::move(value)) {}
  Result(std::string message, Tag) : value_(std::move(message)) {}

  std::variant<T, std::string> value_;
};

// specification.md §8.11: which of the two delivery paths carries an
// output to a client is fixed by the SHAPE of the transport, not chosen
// per message.
//
//   RequestResponse -- REST, gRPC unary, brpc. The input's designated
//     outputs are returned synchronously as the reply. Outputs that
//     were not designated reach their audiences through output
//     gateways, as always.
//
//   SessionStream -- FIX being the defining case. Designated outputs
//     are NOT delivered synchronously at all; the gateway's output role
//     delivers EVERY output addressed to the session from the journal,
//     in sequence-number order, and the synchronous receipt is consumed
//     by the gateway for bookkeeping only (admission confirmation and
//     the sequence number, for gap detection and timeouts).
//
// The rule this exists to enforce: a gateway delivers each output to a
// given client exactly once, by the path its transport shape dictates,
// and never by both. That buys exactly-once delivery and cross-order
// ordering by construction, rather than by a de-duplication step that
// can still let a synchronous copy overtake journal-ordered outputs on
// the same session.
enum class TransportShape {
  RequestResponse,
  SessionStream,
};

class InputCodec {
 public:
  virtual ~InputCodec() = default;

  // request -> input bytes, or a rejection reason.
  virtual Result<Bytes> toInput(const ClientRequest& request) = 0;

  // receipt (+ the designated outputs, if any) -> response bytes.
  //
  // `designatedOutputs` is in EMISSION order and may be empty
  // (specification.md §4, §5.2) -- a state machine is free to designate
  // nothing, and a codec must handle that cleanly rather than assuming
  // an element exists.
  //
  // specification.md §8.11: a gateway delivers each output to a given
  // client exactly once, by the path its transport shape dictates, and
  // never by both. This method is that path for RequestResponse
  // transports only; on a SessionStream transport the chassis never
  // calls it with designated outputs, because the output side delivers
  // every output for the session from the journal instead.
  virtual Bytes toOutput(const Receipt& receipt,
                          std::span<const Payload> designatedOutputs) = 0;

  // As above, plus the input bytes this reply answers.
  //
  // Only needed by a codec serving a SessionStream transport with
  // InputGatewayConfig::inlineDesignatedOnSession on. There the reply
  // must be BYTE-IDENTICAL to what the output codec would build from
  // the journal record, because a ResendRequest re-runs the output
  // codec to reproduce it -- and the output codec can see the record's
  // input while this one, by default, cannot. Defaults to the
  // two-argument form, so existing codecs are unaffected.
  virtual Bytes toOutput(const Receipt& receipt, std::span<const Payload> designatedOutputs,
                          Payload /*input*/) {
    return toOutput(receipt, designatedOutputs);
  }

  // Called when a stateful session ends (specification.md §8.1). May
  // return input bytes to propose as a disconnect notification, or
  // std::nullopt if the state machine has no notion of one — the
  // correct answer for a stateless request/response protocol like
  // plain REST, and what every codec should return until it actually
  // needs sessions.
  virtual std::optional<Bytes> onDisconnect(const SessionInfo& session) = 0;
};

}  // namespace sequencer
