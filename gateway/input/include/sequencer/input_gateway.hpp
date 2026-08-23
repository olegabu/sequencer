#pragma once

// specification.md §8.5: the input side's generic chassis.

#include <memory>

#include <sequencer/input_codec.hpp>

namespace sequencer {

// Terminates client requests (baidu_std/gRPC natively via brpc, plus
// HTTP with an arbitrary body — see input_gateway.proto), translates
// each one via `codec`, verifies the client signature (currently the
// explicit placeholder in signature_verifier.hpp — see there for why),
// forwards to the raft group's current leader, follows redirects, and
// relays the receipt back in the client's own protocol
// (specification.md §8.1).
//
// Reads its configuration from argv: --node_peers (required —
// comma-separated "ip:port" addresses of the raft group's nodes) and
// --listen_port (required — this gateway's own client-facing port).
int RunInputGateway(int argc, char** argv, std::unique_ptr<InputCodec> codec);

}  // namespace sequencer
