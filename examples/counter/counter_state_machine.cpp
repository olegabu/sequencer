#include "counter_state_machine.hpp"

#include <cstring>
#include <stdexcept>

namespace sequencer::examples::counter {

void CounterStateMachine::apply(std::uint64_t /*sequenceNumber*/, Payload input,
                                 OutputCollector& outputs) {
  if (input.size() != sizeof(std::int64_t)) {
    throw std::runtime_error("CounterStateMachine::apply: input must be exactly 8 bytes");
  }
  std::int64_t delta;
  std::memcpy(&delta, input.data(), sizeof(delta));
  total_ += delta;

  outputs.emit(Payload(reinterpret_cast<const std::byte*>(&total_), sizeof(total_)));
  outputs.designateOutput(0);
}

void CounterStateMachine::snapshotSave(SnapshotWriter& writer) { writer.write(&total_, sizeof(total_)); }

void CounterStateMachine::snapshotLoad(SnapshotReader& reader) { reader.read(&total_, sizeof(total_)); }

}  // namespace sequencer::examples::counter
