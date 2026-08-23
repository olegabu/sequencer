#pragma once

// specification.md §8.5: the output side's generic chassis.

#include <memory>

#include <sequencer/output_codec.hpp>

namespace sequencer {

// Tails a journal from a durably-tracked resume position, calling
// `codec->toOutput(record, fanout)` once per record in order
// (specification.md §8.3), and serves a brpc::Stream-based Subscribe
// endpoint that lets clients register for `fanout`'s `toSession`/
// `broadcast` deliveries.
//
// Reads its configuration from argv: --data_dir (required — the
// journal to tail; this phase reads it directly via a colocated
// memory-map, per specification.md §3's "colocated consumers may
// instead memory-map the journal file directly" — a relay-fed, remote
// Subscribe-client mode is future work, once gateway/relay exists),
// --resume_file (required — where the durable resume position is
// persisted, so a restart resumes exactly where it left off), and
// --listen_port (required — this gateway's own client-facing port).
int RunOutputGateway(int argc, char** argv, std::unique_ptr<OutputCodec> codec);

}  // namespace sequencer
