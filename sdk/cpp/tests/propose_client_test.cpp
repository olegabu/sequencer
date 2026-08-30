// ProposeClient (specification.md §9's "propose") is transport-
// agnostic — see propose_client.hpp's file comment for why. This test
// gives it a real transport in two different shapes to prove the
// design actually works end to end, not just in the abstract:
//
//  - ProposesThroughANodeDirectlyAndReceivesAReceipt: the transport
//    calls a real, in-process NodeImpl's ProposeService directly — the
//    "pass-through, no gateway at all" deployment shape.
//  - SignedEnvelopeIsAcceptedAndATamperedOneIsRejectedAtTheGateway:
//    the transport goes through a real InputGatewayImpl configured
//    with sdk::makeEnvelopeSignatureVerifier — proving client_signer.hpp
//    and gateway/input's SignatureVerifier hook (currently only wired
//    to the explicit acceptAllSignatures placeholder anywhere else in
//    this repository) actually fit together.

#include "input_gateway_impl.hpp"
#include "node_impl.hpp"

#include <sequencer/journal/reader.hpp>
#include <sequencer/sdk/client_signer.hpp>
#include <sequencer/sdk/propose_client.hpp>

#include <brpc/channel.h>
#include <brpc/controller.h>

#include <chrono>
#include <cstring>
#include <filesystem>
#include <memory>
#include <string>
#include <thread>

#include <gtest/gtest.h>

#include "node.pb.h"

namespace sequencer::sdk {
namespace {

class SumStateMachine : public sequencer::StateMachine {
 public:
  void apply(std::uint64_t, Payload input, OutputCollector& outputs) override {
    std::int64_t delta;
    std::memcpy(&delta, input.data(), sizeof(delta));
    total_ += delta;
    outputs.emit(Payload(reinterpret_cast<const std::byte*>(&total_), sizeof(total_)));
    outputs.designateOutput(0);
  }
  void snapshotSave(sequencer::SnapshotWriter&) override {}
  void snapshotLoad(sequencer::SnapshotReader&) override {}

 private:
  std::int64_t total_ = 0;
};

// Accepts whatever bytes it's given verbatim — used by the gateway
// fixture below so a signature envelope (signature ‖ payload) passes
// through untouched for the SignatureVerifier hook to check, exactly
// as an application adopting client_signer.hpp's scheme would wire it.
class PassThroughCodec : public sequencer::InputCodec {
 public:
  Result<Bytes> toInput(const ClientRequest& request) override {
    return Result<Bytes>::Ok(Bytes(request.body.begin(), request.body.end()));
  }
  Bytes toOutput(const Receipt& receipt, Payload designatedOutput) override {
    Bytes out(sizeof(receipt.sequenceNumber) + designatedOutput.size());
    std::memcpy(out.data(), &receipt.sequenceNumber, sizeof(receipt.sequenceNumber));
    std::memcpy(out.data() + sizeof(receipt.sequenceNumber), designatedOutput.data(), designatedOutput.size());
    return out;
  }
  std::optional<Bytes> onDisconnect(const SessionInfo&) override { return std::nullopt; }
};

std::filesystem::path makeTempDir() {
  std::string tmpl = (std::filesystem::temp_directory_path() / "propose_client_test_XXXXXX").string();
  if (::mkdtemp(tmpl.data()) == nullptr) {
    throw std::runtime_error("mkdtemp failed");
  }
  return tmpl;
}

Payload payloadOf(const std::int64_t& v) {
  return Payload(reinterpret_cast<const std::byte*>(&v), sizeof(v));
}

Ed25519PrivateKey testPrivateKey() {
  Ed25519PrivateKey key{};
  key.fill(std::byte{0x11});
  return key;
}

Ed25519PublicKey derivePublicKey(const Ed25519PrivateKey& privateKey) {
  EVP_PKEY* key = EVP_PKEY_new_raw_private_key(EVP_PKEY_ED25519, nullptr,
                                                reinterpret_cast<const unsigned char*>(privateKey.data()),
                                                privateKey.size());
  Ed25519PublicKey publicKey{};
  std::size_t len = publicKey.size();
  EVP_PKEY_get_raw_public_key(key, reinterpret_cast<unsigned char*>(publicKey.data()), &len);
  EVP_PKEY_free(key);
  return publicKey;
}

class ProposeClientTest : public ::testing::Test {
 protected:
  void SetUp() override {
    dir_ = makeTempDir();

    node::detail::NodeConfig nodeConfig;
    nodeConfig.groupId = "propose-client-test";
    nodeConfig.peerId = "127.0.0.1:28971:0";
    nodeConfig.initialPeers = nodeConfig.peerId;
    nodeConfig.dataDir = dir_;
    nodeConfig.electionTimeoutMs = 300;

    node_ = std::make_unique<node::detail::NodeImpl>(nodeConfig, std::make_unique<SumStateMachine>());
    node_->start();

    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
    while (!node_->isLeader() && std::chrono::steady_clock::now() < deadline) {
      std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
    ASSERT_TRUE(node_->isLeader());

    ASSERT_EQ(nodeChannel_.Init("127.0.0.1:28971", nullptr), 0);
  }

  void TearDown() override {
    if (gateway_) {
      gateway_->stop();
    }
    node_->stop();
    std::filesystem::remove_all(dir_);
  }

  // The "pass-through, no gateway" transport: one Propose RPC directly
  // against the node. Single-node and always-leader by the time SetUp
  // finishes, so no redirect-following is exercised here — see
  // three_node_smoke_test.cpp / gateway/input/src/node_proposer.hpp for
  // the fuller multi-node version of this same idea.
  ProposeOutcome sendDirectlyToNode(Payload requestBytes) {
    sequencer::node::proto::ProposeService_Stub stub(&nodeChannel_);
    sequencer::node::proto::ProposeRequest request;
    request.set_input(requestBytes.data(), requestBytes.size());
    sequencer::node::proto::ProposeResponse response;
    brpc::Controller cntl;
    stub.Propose(&cntl, &request, &response, nullptr);

    ProposeOutcome outcome;
    if (cntl.Failed() || response.redirect() || !response.error_message().empty()) {
      outcome.ok = false;
      outcome.errorMessage = cntl.Failed() ? cntl.ErrorText() : response.error_message();
      return outcome;
    }
    outcome.ok = true;
    outcome.receipt.sequenceNumber = response.sequence_number();
    const std::string& designated = response.designated_output();
    outcome.designatedOutput.assign(reinterpret_cast<const std::byte*>(designated.data()),
                                     reinterpret_cast<const std::byte*>(designated.data()) + designated.size());
    return outcome;
  }

  void startGatewayWithVerifier(sequencer::SignatureVerifier verifier) {
    gateway::input::detail::InputGatewayConfig gatewayConfig;
    gatewayConfig.nodeEndpoints = {"127.0.0.1:28971"};
    gatewayConfig.listenPort = 28972;
    gateway_ = std::make_unique<gateway::input::detail::InputGatewayImpl>(
        gatewayConfig, std::make_unique<PassThroughCodec>(), verifier);
    gateway_->start();
    ASSERT_EQ(gatewayChannel_.Init("127.0.0.1:28972", nullptr), 0);
  }

  ProposeOutcome sendThroughGateway(Payload requestBytes) {
    sequencer::gateway::input::proto::SubmitService_Stub stub(&gatewayChannel_);
    sequencer::gateway::input::proto::SubmitRequest request;
    sequencer::gateway::input::proto::SubmitResponse response;
    brpc::Controller cntl;
    cntl.request_attachment().append(requestBytes.data(), requestBytes.size());
    stub.Submit(&cntl, &request, &response, nullptr);

    ProposeOutcome outcome;
    if (cntl.Failed()) {
      outcome.ok = false;
      outcome.errorMessage = cntl.ErrorText();
      return outcome;
    }
    const std::string body = cntl.response_attachment().to_string();
    outcome.ok = true;
    std::memcpy(&outcome.receipt.sequenceNumber, body.data(), sizeof(outcome.receipt.sequenceNumber));
    outcome.designatedOutput.assign(
        reinterpret_cast<const std::byte*>(body.data() + sizeof(outcome.receipt.sequenceNumber)),
        reinterpret_cast<const std::byte*>(body.data() + body.size()));
    return outcome;
  }

  std::filesystem::path dir_;
  std::unique_ptr<node::detail::NodeImpl> node_;
  std::unique_ptr<gateway::input::detail::InputGatewayImpl> gateway_;
  brpc::Channel nodeChannel_;
  brpc::Channel gatewayChannel_;
};

TEST_F(ProposeClientTest, ProposesThroughANodeDirectlyAndReceivesAReceipt) {
  ProposeClient client([this](Payload bytes) { return sendDirectlyToNode(bytes); });

  const ProposeOutcome first = client.propose(payloadOf(5));
  ASSERT_TRUE(first.ok) << first.errorMessage;
  EXPECT_EQ(first.receipt.sequenceNumber, 1u);
  ASSERT_EQ(first.designatedOutput.size(), sizeof(std::int64_t));
  std::int64_t total1;
  std::memcpy(&total1, first.designatedOutput.data(), sizeof(total1));
  EXPECT_EQ(total1, 5);

  const ProposeOutcome second = client.propose(payloadOf(-2));
  ASSERT_TRUE(second.ok) << second.errorMessage;
  EXPECT_EQ(second.receipt.sequenceNumber, 2u);
  std::int64_t total2;
  std::memcpy(&total2, second.designatedOutput.data(), sizeof(total2));
  EXPECT_EQ(total2, 3);
}

TEST_F(ProposeClientTest, SignedEnvelopeIsAcceptedAndATamperedOneIsRejectedAtTheGateway) {
  const Ed25519PrivateKey privateKey = testPrivateKey();
  const Ed25519PublicKey publicKey = derivePublicKey(privateKey);
  startGatewayWithVerifier(makeEnvelopeSignatureVerifier(publicKey));

  ProposeClient client([this](Payload bytes) { return sendThroughGateway(bytes); });
  client.setSigningKey(privateKey);

  const ProposeOutcome accepted = client.propose(payloadOf(7));
  ASSERT_TRUE(accepted.ok) << accepted.errorMessage;
  EXPECT_EQ(accepted.receipt.sequenceNumber, 1u);

  journal::JournalReader reader(dir_ / "journal");
  EXPECT_EQ(reader.committedCount(), 1u);
  EXPECT_TRUE(verifyEnvelopeSignature(reader.record(1).input(), publicKey))
      << "the signature must be persisted inside the journaled input (specification.md §7), "
      << "not stripped before proposing";

  // A hand-tampered envelope: correct signature, different payload —
  // exactly specification.md §7's "a journaled entry lacking a valid
  // client signature is standalone proof of fabrication" scenario, but
  // caught before it ever reaches the journal.
  const Bytes signed7 = signPayload(payloadOf(7), privateKey);
  Bytes tampered = signed7;
  const std::int64_t swapped = 700;
  std::memcpy(tampered.data() + kSignatureEnvelopeOverhead, &swapped, sizeof(swapped));

  brpc::Controller cntl;
  sequencer::gateway::input::proto::SubmitService_Stub stub(&gatewayChannel_);
  sequencer::gateway::input::proto::SubmitRequest request;
  sequencer::gateway::input::proto::SubmitResponse response;
  cntl.request_attachment().append(tampered.data(), tampered.size());
  stub.Submit(&cntl, &request, &response, nullptr);
  EXPECT_TRUE(cntl.Failed()) << "a tampered signature must be rejected, never proposed";

  journal::JournalReader reader2(dir_ / "journal");
  EXPECT_EQ(reader2.committedCount(), 1u) << "the tampered submission must never reach Propose";
}

}  // namespace
}  // namespace sequencer::sdk
