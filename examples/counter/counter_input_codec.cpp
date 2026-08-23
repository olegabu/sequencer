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

Bytes CounterInputCodec::toOutput(const Receipt& receipt, Payload designatedOutput) {
  std::int64_t total = 0;
  if (designatedOutput.size() == sizeof(total)) {
    std::memcpy(&total, designatedOutput.data(), sizeof(total));
  }
  const std::string json = buildSequenceAndTotalJson(receipt.sequenceNumber, total);
  return Bytes(reinterpret_cast<const std::byte*>(json.data()),
               reinterpret_cast<const std::byte*>(json.data()) + json.size());
}

std::optional<Bytes> CounterInputCodec::onDisconnect(const SessionInfo& /*session*/) {
  return std::nullopt;
}

}  // namespace sequencer::examples::counter
