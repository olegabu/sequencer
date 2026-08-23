#pragma once

// specification.md §10: "CounterOutputCodec translates each journal
// record into a JSON message (sequence number and new total) published
// over WebSocket." The WebSocket delivery itself is purely a transport
// concern (see output_gateway_main.cpp's WebSocketTransport) — this
// codec only ever produces bytes and hands them to Fanout::broadcast.

#include <sequencer/output_codec.hpp>

namespace sequencer::examples::counter {

class CounterOutputCodec : public sequencer::OutputCodec {
 public:
  // record -> {"sequence_number": <uint64>, "total": <int64>},
  // broadcast to the "totals" topic every connected client joins.
  void toOutput(const journal::RecordView& record, Fanout& fanout) override;
};

}  // namespace sequencer::examples::counter
