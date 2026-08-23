#pragma once

// specification.md §8.5: the output side's generic chassis.

#include <functional>
#include <memory>

#include <sequencer/output_codec.hpp>
#include <sequencer/output_transport.hpp>

namespace sequencer {

// Tails a journal from a durably-tracked resume position, calling
// `codec->toOutput(record, transport)` once per record in order
// (specification.md §8.3). Delivery uses the chassis's built-in
// transport: brpc's own Streaming RPC (specification.md §8.7).
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

// The same chassis, but with a caller-supplied transport instead of the
// built-in brpc one — for an application needing something else (e.g.
// WebSocket, via Boost.Beast — specification.md §8.7 and
// examples/counter's output_gateway_main.cpp, "the one place the
// example depends on something beyond brpc"). `transportFactory` is
// called once, after argv parsing, so it may itself depend on parsed
// flags if the application defines its own.
int RunOutputGateway(int argc, char** argv, std::unique_ptr<OutputCodec> codec,
                      std::function<std::unique_ptr<OutputTransport>()> transportFactory);

}  // namespace sequencer
