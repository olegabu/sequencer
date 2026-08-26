#pragma once

// specification.md §8.5: the output side's generic chassis.

#include <functional>
#include <memory>
#include <vector>

#include <sequencer/output_codec.hpp>
#include <sequencer/output_transport.hpp>

namespace sequencer {

// One transport and the port it should listen on. A gateway may serve
// several at once — see the three-argument-with-list overload below.
struct OutputTransportBinding {
  std::function<std::unique_ptr<OutputTransport>()> factory;
  int listenPort = 0;
};

// The chassis's own built-in transport — brpc Streaming RPC
// (specification.md §8.7) — as a transport an application can put in a
// binding list alongside others. It lives in gateway/output/src and is
// otherwise not reachable from application code; the two overloads
// below that default to it don't need this, but an application
// enabling brpc *and* something else does.
std::unique_ptr<OutputTransport> MakeBrpcStreamTransport();

// Tails a journal from a durably-tracked resume position, calling
// `codec->toOutput(record, fanout)` once per record in order
// (specification.md §8.3), and publishing each result into a
// BroadcastRing that every connected subscriber drains through its own
// cursor (see gateway/output/README.md).
//
// Serves one or more transports, each on its own port — one process
// can serve, say, brpc and real gRPC and WebSocket subscribers
// simultaneously from a single journal tail.
//
// This costs the producer nothing extra: the codec runs once per
// record and publishes once into one ring regardless of how many
// transports are attached, and each transport's per-subscriber readers
// drain that same ring independently. Three separate single-transport
// processes would instead tail the journal three times, run the codec
// three times, and keep three resume positions.
//
// Reads --data_dir and --resume_file from argv (both required — the
// journal to tail, read directly via a colocated memory-map per
// specification.md §3, and where the durable resume position is
// persisted so a restart resumes exactly where it left off).
//
// Ports are NOT read from a chassis flag: a port belongs to a
// transport, and this chassis serves a list of them. `bindingsFactory`
// is called once, after argv parsing, so an application defines
// whatever port flags it likes and decides from them which transports
// to enable. Each returned binding needs a nonzero port, and at least
// one binding is required.
//
// (There is a second reason the chassis cannot own a --listen_port
// flag: gateway/input/'s chassis already defines one, and gflags flags
// are process-global symbols, so any binary linking both libraries
// would fail to link.)
//
// Note this is one process on N ports, not brpc's own trick of several
// protocols on ONE port: brpc can sniff a connection's first bytes
// because it implements all those protocols itself, whereas these are
// three independent libraries each owning its own acceptor.
int RunOutputGateway(int argc, char** argv, std::unique_ptr<OutputCodec> codec,
                      std::function<std::vector<OutputTransportBinding>()> bindingsFactory);

}  // namespace sequencer
