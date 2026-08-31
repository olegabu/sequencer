#include "counter_input_codec.hpp"

#include <cstring>
#include <string>

#include "json_util.hpp"

namespace sequencer::examples::counter {

Result<Bytes> CounterInputCodec::toInput(const ClientRequest& request) {
  const std::string json(reinterpret_cast<const char*>(request.body.data()), request.body.size());
  const std::optional<std::int64_t> delta = extractJsonIntField(json, "delta");
  if (!delta.has_value()) {
    return Result<Bytes>::Error("expected a JSON body of the form {\"delta\": <integer>}");
  }
  Bytes input(sizeof(std::int64_t));
  const std::int64_t value = *delta;
  std::memcpy(input.data(), &value, sizeof(value));
  return Result<Bytes>::Ok(std::move(input));
}

Bytes CounterInputCodec::toOutput(const Receipt& receipt,
                                   std::span<const Payload> designatedOutputs) {
  // Counter designates exactly one output -- the new running total --
  // so this takes the first. The empty case is real, not defensive:
  // a state machine may designate nothing, and specification.md §8.11
  // has the chassis hand this codec an empty span on a SessionStream
  // transport even when the state machine did designate. Reporting
  // total 0 there would be a lie, but the field is required by the
  // response shape, so it stays 0 and the sequence number -- which is
  // always present -- carries the meaning.
  std::int64_t total = 0;
  if (!designatedOutputs.empty() && designatedOutputs[0].size() == sizeof(total)) {
    std::memcpy(&total, designatedOutputs[0].data(), sizeof(total));
  }
  const std::string json = buildSequenceAndTotalJson(receipt.sequenceNumber, total);
  return Bytes(reinterpret_cast<const std::byte*>(json.data()),
               reinterpret_cast<const std::byte*>(json.data()) + json.size());
}

std::optional<Bytes> CounterInputCodec::onDisconnect(const SessionInfo& /*session*/) {
  return std::nullopt;
}

}  // namespace sequencer::examples::counter
