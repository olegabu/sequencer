#pragma once

// Per-transport visibility into the output side's batching, published
// as bvars on the gateway's own brpc /vars page.
//
// All three transports already batch identically -- each subscriber's
// reader drains [cursor, head) up to 1024 entries, filters by tag, and
// sends the result as ONE wire operation (one StreamWrite, one
// WebSocket binary frame, one OutputRecordBatch). But only gRPC's
// batching is VISIBLE from outside, because it is declared in that
// transport's wire schema as a repeated field; brpc and WebSocket
// batch inside length-prefixed framing, where a subscriber genuinely
// cannot tell one frame of four records from four frames of one. So
// the input side could be measured (input_gateway_batch_size) and the
// output side could not.
//
// One instance per transport, not one shared: bvar names are
// process-global, and examples/counter's combined output gateway runs
// all three in a single process.
//
// COST. recordBatch() runs once per WIRE WRITE, never per record and
// never in the caught-up spin loop -- the callers only reach it when a
// drain actually produced something. A bvar update is a per-thread
// lock-free append; a socket write is microseconds. Measured on this
// machine at 21 ns per recordBatch() call against ~1-10 us for the
// write it accompanies, so under 1% of the send path and nothing at
// all on the idle path.

#include <bvar/bvar.h>

#include <cstdint>
#include <string>

namespace sequencer::gateway::output::detail {

class OutputBatchMetrics {
 public:
  explicit OutputBatchMetrics(const char* transport)
      : size_(std::string("output_batch_size_") + transport, &sizeRaw_, -1),
        writes_(std::string("output_wire_writes_") + transport),
        records_(std::string("output_records_sent_") + transport) {}

  OutputBatchMetrics(const OutputBatchMetrics&) = delete;
  OutputBatchMetrics& operator=(const OutputBatchMetrics&) = delete;

  // `count` is how many records went out in this one wire operation.
  // Callers must not call this with 0 -- an empty drain sends nothing,
  // and counting it would both skew the average toward zero and put a
  // bvar update inside the idle spin.
  void recordBatch(int count) {
    sizeRaw_ << count;
    writes_ << 1;
    records_ << count;
  }

 private:
  // Windowed, NOT a bare IntRecorder: that publishes a LIFETIME
  // average, so under a rising load sweep every reading is dominated
  // by the quieter traffic that came before it. The same trap cost a
  // full measurement run on the input side (see
  // gateway/input/src/node_proposer.hpp's ProposerMetrics).
  bvar::IntRecorder sizeRaw_;
  bvar::Window<bvar::IntRecorder> size_;
  // The count batching is meant to reduce, and the count it is meant
  // to preserve. records_/writes_ is the realised batch factor.
  bvar::Adder<std::uint64_t> writes_;
  bvar::Adder<std::uint64_t> records_;
};

inline OutputBatchMetrics& brpcBatchMetrics() {
  static OutputBatchMetrics metrics("brpc");
  return metrics;
}

inline OutputBatchMetrics& grpcBatchMetrics() {
  static OutputBatchMetrics metrics("grpc");
  return metrics;
}

inline OutputBatchMetrics& websocketBatchMetrics() {
  static OutputBatchMetrics metrics("websocket");
  return metrics;
}

}  // namespace sequencer::gateway::output::detail
