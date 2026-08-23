#pragma once

// specification.md §10: "CounterInputCodec translates a small JSON body
// (for example {"delta": 5}) into the 8-byte input, and a receipt plus
// designated output back into a JSON response."
//
// Hand-rolled JSON encode/decode — deliberately minimal, matching only
// the two trivial shapes this example actually needs. No JSON library
// appears anywhere in this repository's dependency list (specification.md
// §9), and these shapes don't warrant adding one.

#include <sequencer/input_codec.hpp>

namespace sequencer::examples::counter {

class CounterInputCodec : public sequencer::InputCodec {
 public:
  // {"delta": <integer>} -> the 8-byte signed delta CounterStateMachine
  // expects. Any other shape is rejected (Result::Error), never reaches
  // Propose.
  Result<Bytes> toInput(const ClientRequest& request) override;

  // -> {"sequence_number": <uint64>, "total": <int64>}. The submitting
  // client is the designated output's only interested party
  // (specification.md §10), so this is the whole response.
  Bytes toOutput(const Receipt& receipt, Payload designatedOutput) override;

  // Counter is stateless request/response — no session, so nothing to
  // propose on disconnect.
  std::optional<Bytes> onDisconnect(const SessionInfo& session) override;
};

}  // namespace sequencer::examples::counter
