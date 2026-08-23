#include "counter_output_codec.hpp"

#include <cstring>
#include <string>

#include "json_util.hpp"

namespace sequencer::examples::counter {

void CounterOutputCodec::toOutput(const journal::RecordView& record, Fanout& fanout) {
  std::int64_t total = 0;
  if (record.outputCount() > 0) {
    const Payload output0 = record.output(0);
    if (output0.size() == sizeof(total)) {
      std::memcpy(&total, output0.data(), sizeof(total));
    }
  }
  const std::string json = buildSequenceAndTotalJson(record.sequenceNumber(), total);
  fanout.broadcast("totals", Bytes(reinterpret_cast<const std::byte*>(json.data()),
                                    reinterpret_cast<const std::byte*>(json.data()) + json.size()));
}

}  // namespace sequencer::examples::counter
