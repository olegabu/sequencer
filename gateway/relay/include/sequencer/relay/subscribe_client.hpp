#pragma once

// Reference client for gateway/relay's Subscribe RPC (specification.md
// §8.2): connects to a relay, requests everything from
// `fromSequenceNumber` onward, and invokes `onRecord` once per record —
// each callback's bytes are byte-identical to what a colocated
// `journal::RecordView::rawBytes()` would return for that same
// sequence number — strictly in order, for as long as the subscription
// stays connected. Pimpl'd so this public header stays free of brpc's
// heavy headers (matching examples/counter/websocat_transport.hpp's
// same reasoning); a small compiled library (`sequencer::relay_client`),
// not application-linked like gateway/input and gateway/output's
// chassis — a relay client needs no InputCodec/OutputCodec, since it
// hands back raw, uninterpreted journal record bytes.

#include <sequencer/payload.hpp>

#include <cstdint>
#include <functional>
#include <memory>
#include <string>

namespace sequencer::relay {

using RecordCallback = std::function<void(Bytes rawRecordBytes)>;

class RelaySubscribeClient {
 public:
  // Connects to `relayEndpoint` ("ip:port") and subscribes from
  // `fromSequenceNumber` (0 means "from the beginning" — sequence
  // number 1). The subscription runs for the lifetime of this object;
  // `onRecord` is invoked from a background thread, once per record,
  // strictly in sequence-number order.
  RelaySubscribeClient(std::string relayEndpoint, std::uint64_t fromSequenceNumber, RecordCallback onRecord);
  ~RelaySubscribeClient();

  RelaySubscribeClient(const RelaySubscribeClient&) = delete;
  RelaySubscribeClient& operator=(const RelaySubscribeClient&) = delete;

  // False if the initial Subscribe handshake failed (channel init,
  // stream creation, or the relay itself rejected the request — see
  // errorMessage()). Once true, delivery has begun and this object
  // need not be checked again.
  bool ok() const;
  const std::string& errorMessage() const;

 private:
  struct Impl;
  std::unique_ptr<Impl> impl_;
};

}  // namespace sequencer::relay
