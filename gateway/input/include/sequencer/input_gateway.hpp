#pragma once

// specification.md §8.5: the input side's generic chassis.

// specification.md §8.11: a gateway delivers each output to a given
// client exactly once, by the path its transport shape dictates, and
// never by both. This chassis enforces that -- see TransportShape in
// <sequencer/input_codec.hpp> and the guard in
// src/request_pipeline.hpp, which every transport passes through.

#include <functional>
#include <memory>

#include <sequencer/input_codec.hpp>
#include <sequencer/input_transport.hpp>

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

// The same, over a transport of the application's choosing --
// specification.md §8.10's choice (b), mirroring RunOutputGateway's
// transport-factory overload.
//
// The built-in brpc transport is what the overload above uses, and it
// is an ordinary implementation of the same interface rather than a
// privileged path, so a FIX session gateway (gateway/fix/) composes
// with any InputCodec exactly as brpc does.
//
// The transport's own shape() decides whether designated outputs may
// be returned synchronously (§8.11); the chassis reads it and enforces
// the consequence, so a SessionStream transport cannot be configured
// into double-delivering.
int RunInputGateway(int argc, char** argv, std::unique_ptr<InputCodec> codec,
                     std::function<std::unique_ptr<InputTransport>()> transportFactory);

}  // namespace sequencer
