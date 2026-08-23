#pragma once

// specification.md §8.5: the input-side plug point. The chassis
// (RunInputGateway) owns sessions, authentication, transport security,
// signature verification, leader tracking, and retries; the codec owns
// meaning — translating a client's wire bytes into the state machine's
// input bytes, and a receipt back into the client's own response
// format.

#include <cstdint>
#include <optional>
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

class InputCodec {
 public:
  virtual ~InputCodec() = default;

  // request -> input bytes, or a rejection reason.
  virtual Result<Bytes> toInput(const ClientRequest& request) = 0;

  // receipt (+ the designated output, if any) -> response bytes.
  virtual Bytes toOutput(const Receipt& receipt, Payload designatedOutput) = 0;

  // Called when a stateful session ends (specification.md §8.1). May
  // return input bytes to propose as a disconnect notification, or
  // std::nullopt if the state machine has no notion of one — the
  // correct answer for a stateless request/response protocol like
  // plain REST, and what every codec should return until it actually
  // needs sessions.
  virtual std::optional<Bytes> onDisconnect(const SessionInfo& session) = 0;
};

}  // namespace sequencer
