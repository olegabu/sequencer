#include "counter_output_codec.hpp"

#include <cstring>
#include <string>

#include "counter_client_id.hpp"
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
  // Published to the SUBMITTING client's own topic, recovered from the
  // delta's high bits (counter_client_id.hpp), so a subscriber receives
  // only the totals its own requests produced.
  //
  // This used to be one shared "totals" topic every client joined,
  // which made delivery load (rate x subscribers): a five-client run at
  // 100k had the gateway pushing 500k messages/sec for 100k of offered
  // load, and every client parsing five times the traffic it cared
  // about. The FIX codec already routed per client, so comparing the
  // two was measuring the fan-out difference as much as the transport.
  std::int64_t submitted = 0;
  const Payload in = record.input();
  if (in.size() == sizeof(submitted)) {
    std::memcpy(&submitted, in.data(), sizeof(submitted));
  }
  const std::string json = buildSequenceAndTotalJson(record.sequenceNumber(), total);
  fanout.broadcast(counterTotalsTopicFor(counterClientIdOf(submitted)),
                   Bytes(reinterpret_cast<const std::byte*>(json.data()),
                          reinterpret_cast<const std::byte*>(json.data()) + json.size()));
}

}  // namespace sequencer::examples::counter
