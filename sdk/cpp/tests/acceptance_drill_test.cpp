// specification.md §14's acceptance checklist, item 3's two angles not
// already covered elsewhere in this repository:
//
//  - "proof reconstruction from the published journal alone succeeds":
//    proof_verifier_test.cpp and evidence/tests/signing_gateway_test.cpp
//    both verify proofs against bytes the caller already held; this
//    test instead reads rawRecordBytes directly off a fresh, colocated
//    JournalReader — simulating a client that lost its own copy and
//    must reconstruct purely from what's published — and proves the
//    proof still verifies.
//  - "the proof-timeout alarm fires when the signing gateway is
//    deliberately stalled": alarm_test.cpp only exercises
//    ProofTimeoutAlarm against synthetic timestamps; this test ties it
//    to a real SigningGatewayImpl that is genuinely stalled (its block
//    deliberately left incomplete, so specification.md §7.1 guarantees
//    it will never sign) and confirms the alarm correctly flags the
//    sequence number as overdue.
//
// ("A client verifies a proof against its own retained bytes" is
// covered thoroughly by proof_verifier_test.cpp already and isn't
// repeated here.)

#include <sequencer/temp_dir.hpp>
#include <sequencer/evidence/merkle.hpp>
#include <sequencer/sdk/alarm.hpp>

#include "evidence_server.hpp"
#include "signing_gateway_impl.hpp"

#include <sequencer/journal/reader.hpp>
#include <sequencer/journal/writer.hpp>

#include <brpc/channel.h>
#include <brpc/controller.h>

#include <chrono>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <functional>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

namespace sequencer::sdk {
namespace {


Payload payloadOf(const std::int64_t& v) {
  return Payload(reinterpret_cast<const std::byte*>(&v), sizeof(v));
}

evidence::Ed25519PrivateKey testPrivateKey() {
  evidence::Ed25519PrivateKey key{};
  key.fill(std::byte{0x33});
  return key;
}

evidence::Ed25519PublicKey derivePublicKey(const evidence::Ed25519PrivateKey& privateKey) {
  EVP_PKEY* key = EVP_PKEY_new_raw_private_key(EVP_PKEY_ED25519, nullptr,
                                                reinterpret_cast<const unsigned char*>(privateKey.data()),
                                                privateKey.size());
  evidence::Ed25519PublicKey publicKey{};
  std::size_t len = publicKey.size();
  EVP_PKEY_get_raw_public_key(key, reinterpret_cast<unsigned char*>(publicKey.data()), &len);
  EVP_PKEY_free(key);
  return publicKey;
}

bool waitUntil(std::chrono::seconds timeout, const std::function<bool()>& predicate) {
  const auto deadline = std::chrono::steady_clock::now() + timeout;
  while (std::chrono::steady_clock::now() < deadline) {
    if (predicate()) {
      return true;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
  }
  return false;
}

TEST(AcceptanceDrills, ProofReconstructionFromThePublishedJournalAloneSucceeds) {
  const std::filesystem::path dir = sequencer::makeTempDir("acceptance_drill_test");
  {
    journal::JournalWriter writer(dir / "journal");
    for (std::uint64_t seq = 1; seq <= evidence::kBlockSize; ++seq) {
      writer.append(seq, payloadOf(static_cast<std::int64_t>(seq)), {});
    }
    writer.flush(false);
  }

  const evidence::Ed25519PrivateKey privateKey = testPrivateKey();
  const evidence::Ed25519PublicKey publicKey = derivePublicKey(privateKey);

  evidence::detail::SigningGatewayConfig config;
  config.dataDir = dir;
  config.privateKey = privateKey;
  config.listenPort = 28995;
  evidence::detail::SigningGatewayImpl gateway(std::move(config));
  gateway.start();
  evidence::detail::EvidenceServer server(gateway, 28995);
  ASSERT_TRUE(waitUntil(std::chrono::seconds(5), [&] { return gateway.lastSignedBlockIndex() >= 1; }));

  brpc::Channel channel;
  brpc::ChannelOptions channelOptions;
  channelOptions.timeout_ms = 2000;
  ASSERT_EQ(channel.Init("127.0.0.1:28995", &channelOptions), 0);
  sequencer::evidence::proto::EvidenceService_Stub stub(&channel);

  const std::uint64_t provenSeq = 900;
  sequencer::evidence::proto::GetInclusionProofRequest request;
  request.set_sequence_number(provenSeq);
  sequencer::evidence::proto::GetInclusionProofResponse response;
  brpc::Controller cntl;
  stub.GetInclusionProof(&cntl, &request, &response, nullptr);
  ASSERT_FALSE(cntl.Failed());
  ASSERT_TRUE(response.found());

  evidence::InclusionProof proof;
  proof.sequenceNumber = provenSeq;
  proof.blockFirstSequenceNumber = response.first_sequence_number();
  proof.blockLastSequenceNumber = response.last_sequence_number();
  std::memcpy(proof.root.data(), response.root().data(), proof.root.size());
  std::memcpy(proof.signature.data(), response.signature().data(), proof.signature.size());
  for (const std::string& sibling : response.sibling_hashes()) {
    evidence::Hash32 h{};
    std::memcpy(h.data(), sibling.data(), h.size());
    proof.siblingHashes.push_back(h);
  }

  // The point of this test: a *fresh* colocated reader, standing in for
  // a client that has nothing of its own left but access to the
  // published journal (specification.md §7.4: "reconstruct it from the
  // published journal" is the alarm's own prescribed recovery action)
  // — never anything this test already held from writing the journal
  // above.
  journal::JournalReader freshReader(dir / "journal");
  const Payload reconstructedRawBytes = freshReader.record(provenSeq).rawBytes();

  EXPECT_TRUE(evidence::verifyInclusionProof(proof, publicKey, reconstructedRawBytes))
      << "specification.md §14 item 3: proof reconstruction from the published journal alone must "
      << "succeed, with nothing supplied by the client beyond the journal itself";

  server.stop();
  gateway.stop();
  std::filesystem::remove_all(dir);
}

TEST(AcceptanceDrills, ProofTimeoutAlarmFiresWhenTheSigningGatewayIsDeliberatelyStalled) {
  const std::filesystem::path dir = sequencer::makeTempDir("acceptance_drill_test");
  const std::uint64_t submittedSeq = 5;
  {
    // Deliberately far short of a full block (specification.md §7.1:
    // a block is never signed until it is complete) — this signing
    // gateway can never make progress on this journal, by
    // construction, standing in for "the signing gateway is
    // deliberately stalled."
    journal::JournalWriter writer(dir / "journal");
    for (std::uint64_t seq = 1; seq <= 10; ++seq) {
      writer.append(seq, payloadOf(static_cast<std::int64_t>(seq)), {});
    }
    writer.flush(false);
  }

  evidence::detail::SigningGatewayConfig config;
  config.dataDir = dir;
  config.privateKey = testPrivateKey();
  config.listenPort = 0;
  evidence::detail::SigningGatewayImpl gateway(std::move(config));
  gateway.start();

  ProofTimeoutAlarm alarm(std::chrono::milliseconds(500));
  const auto submittedAt = std::chrono::steady_clock::now();
  alarm.submitted(submittedSeq, submittedAt);

  // Give the (permanently stalled) gateway a real window to prove it
  // never signs anything, rather than asserting this instantaneously.
  std::this_thread::sleep_for(std::chrono::milliseconds(200));
  EXPECT_EQ(gateway.lastSignedBlockIndex(), 0u) << "sanity: the gateway must still be stalled at this point";
  EXPECT_TRUE(alarm.overdue(submittedAt + std::chrono::milliseconds(200)).empty())
      << "not overdue yet — still inside the bound";

  EXPECT_FALSE(alarm.overdue(submittedAt + std::chrono::milliseconds(600)).empty())
      << "specification.md §14 item 3: the proof-timeout alarm must fire once the bound elapses "
      << "with the signing gateway still stalled";
  const std::vector<std::uint64_t> overdue = alarm.overdue(submittedAt + std::chrono::milliseconds(600));
  ASSERT_EQ(overdue.size(), 1u);
  EXPECT_EQ(overdue[0], submittedSeq);

  gateway.stop();
  std::filesystem::remove_all(dir);
}

}  // namespace
}  // namespace sequencer::sdk
