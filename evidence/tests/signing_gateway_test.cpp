// End-to-end test of the signing gateway (specification.md §8.4): a
// real SigningGatewayImpl tailing a directly-synthesized journal (no
// node needed — matching gateway/output/tests/output_gateway_test.cpp's
// pattern), signing real blocks, served over a real brpc client via
// EvidenceService. Also covers specification.md §8.4's "run at least
// two instances" redundancy claim directly: two independent gateways
// reading the same journal must produce byte-identical signed roots.

#include "evidence_server.hpp"
#include "signing_gateway_impl.hpp"

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

namespace sequencer::evidence::detail {
namespace {

std::filesystem::path makeTempDir() {
  std::string tmpl = (std::filesystem::temp_directory_path() / "signing_gateway_test_XXXXXX").string();
  if (::mkdtemp(tmpl.data()) == nullptr) {
    throw std::runtime_error("mkdtemp failed");
  }
  return tmpl;
}

Payload payloadOf(const std::int64_t& v) {
  return Payload(reinterpret_cast<const std::byte*>(&v), sizeof(v));
}

void appendRecords(const std::filesystem::path& dataDir, std::uint64_t count) {
  journal::JournalWriter writer(dataDir / "journal.data", dataDir / "journal.index");
  for (std::uint64_t i = 0; i < count; ++i) {
    const std::int64_t value = static_cast<std::int64_t>(i);
    writer.append(i + 1, payloadOf(value), {});
  }
  writer.flush(false);
}

Ed25519PrivateKey testPrivateKey(std::byte seedByte = std::byte{0x42}) {
  Ed25519PrivateKey key{};
  key.fill(seedByte);
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

TEST(SigningGateway, SignsACompleteBlockAndServesAVerifiableInclusionProof) {
  const std::filesystem::path dir = makeTempDir();
  appendRecords(dir, kBlockSize);  // exactly one complete block

  const Ed25519PrivateKey privateKey = testPrivateKey();
  const Ed25519PublicKey publicKey = derivePublicKey(privateKey);

  SigningGatewayConfig config;
  config.dataDir = dir;
  config.privateKey = privateKey;
  config.listenPort = 28991;
  SigningGatewayImpl gateway(std::move(config));
  gateway.start();
  EvidenceServer server(gateway, 28991);

  ASSERT_TRUE(waitUntil(std::chrono::seconds(5), [&] { return gateway.lastSignedBlockIndex() >= 1; }));

  brpc::Channel channel;
  brpc::ChannelOptions channelOptions;
  channelOptions.timeout_ms = 2000;
  ASSERT_EQ(channel.Init("127.0.0.1:28991", &channelOptions), 0);
  sequencer::evidence::proto::EvidenceService_Stub stub(&channel);

  {
    sequencer::evidence::proto::GetSignedRootRequest request;
    request.set_block_index(1);
    sequencer::evidence::proto::GetSignedRootResponse response;
    brpc::Controller cntl;
    stub.GetSignedRoot(&cntl, &request, &response, nullptr);
    ASSERT_FALSE(cntl.Failed());
    ASSERT_TRUE(response.found());
    EXPECT_EQ(response.first_sequence_number(), 1u);
    EXPECT_EQ(response.last_sequence_number(), kBlockSize);
    ASSERT_EQ(response.root().size(), 32u);
    ASSERT_EQ(response.signature().size(), 64u);

    Hash32 root{};
    std::memcpy(root.data(), response.root().data(), root.size());
    Signature64 signature{};
    std::memcpy(signature.data(), response.signature().data(), signature.size());
    EXPECT_TRUE(verifyRootSignature(publicKey, root, 1, kBlockSize, signature));
  }

  // Pick a sequence number in the middle of the block and prove it,
  // reconstructing rawRecordBytes exactly as journal::JournalWriter
  // wrote it — mirroring how sdk/'s ProofVerifier is meant to be used
  // (specification.md §7.4: verify against locally retained bytes).
  const std::uint64_t provenSeq = 501;
  sequencer::evidence::proto::GetInclusionProofRequest request;
  request.set_sequence_number(provenSeq);
  sequencer::evidence::proto::GetInclusionProofResponse response;
  brpc::Controller cntl;
  stub.GetInclusionProof(&cntl, &request, &response, nullptr);
  ASSERT_FALSE(cntl.Failed());
  ASSERT_TRUE(response.found());
  EXPECT_EQ(static_cast<std::size_t>(response.sibling_hashes_size()), 10u) << "log2(1024)";

  InclusionProof proof;
  proof.sequenceNumber = provenSeq;
  proof.blockFirstSequenceNumber = response.first_sequence_number();
  proof.blockLastSequenceNumber = response.last_sequence_number();
  std::memcpy(proof.root.data(), response.root().data(), proof.root.size());
  std::memcpy(proof.signature.data(), response.signature().data(), proof.signature.size());
  for (const std::string& sibling : response.sibling_hashes()) {
    Hash32 h{};
    std::memcpy(h.data(), sibling.data(), h.size());
    proof.siblingHashes.push_back(h);
  }

  const std::int64_t provenValue = static_cast<std::int64_t>(provenSeq - 1);
  const std::size_t rawSize = journal::recordEncodedSize(payloadOf(provenValue), {});
  std::vector<std::byte> rawRecordBytes(rawSize);
  journal::encodeRecord(rawRecordBytes.data(), provenSeq, payloadOf(provenValue), {});

  EXPECT_TRUE(verifyInclusionProof(proof, publicKey, Payload(rawRecordBytes.data(), rawRecordBytes.size())));

  const std::int64_t wrongValue = provenValue + 1;
  std::vector<std::byte> wrongBytes(rawSize);
  journal::encodeRecord(wrongBytes.data(), provenSeq, payloadOf(wrongValue), {});
  EXPECT_FALSE(verifyInclusionProof(proof, publicKey, Payload(wrongBytes.data(), wrongBytes.size())))
      << "specification.md §7.3: a substituted record fails verification at the client";

  server.stop();
  gateway.stop();
  std::filesystem::remove_all(dir);
}

TEST(SigningGateway, NeverSignsAnIncompleteBlock) {
  const std::filesystem::path dir = makeTempDir();
  appendRecords(dir, kBlockSize - 1);  // one short of a complete block

  SigningGatewayConfig config;
  config.dataDir = dir;
  config.privateKey = testPrivateKey();
  config.listenPort = 0;
  SigningGatewayImpl gateway(std::move(config));
  gateway.start();

  // A bounded wait that's expected to time out: nothing should ever
  // get signed while the block is incomplete (specification.md §7.1
  // never cuts a partial block).
  std::this_thread::sleep_for(std::chrono::milliseconds(500));
  EXPECT_EQ(gateway.lastSignedBlockIndex(), 0u);
  EXPECT_FALSE(gateway.signedBlock(1).has_value());

  gateway.stop();
  std::filesystem::remove_all(dir);
}

TEST(SigningGateway, TwoIndependentInstancesReadingTheSameJournalProduceIdenticalSignedRoots) {
  const std::filesystem::path dir = makeTempDir();
  appendRecords(dir, kBlockSize * 2);  // two complete blocks

  const Ed25519PrivateKey sharedKey = testPrivateKey();

  SigningGatewayConfig config1;
  config1.dataDir = dir;
  config1.privateKey = sharedKey;
  config1.listenPort = 0;
  SigningGatewayImpl gateway1(std::move(config1));

  SigningGatewayConfig config2;
  config2.dataDir = dir;
  config2.privateKey = sharedKey;
  config2.listenPort = 0;
  SigningGatewayImpl gateway2(std::move(config2));

  gateway1.start();
  gateway2.start();
  ASSERT_TRUE(waitUntil(std::chrono::seconds(5), [&] {
    return gateway1.lastSignedBlockIndex() >= 2 && gateway2.lastSignedBlockIndex() >= 2;
  }));

  for (std::uint64_t blockIndex : {1u, 2u}) {
    const std::optional<SignedBlockMeta> a = gateway1.signedBlock(blockIndex);
    const std::optional<SignedBlockMeta> b = gateway2.signedBlock(blockIndex);
    ASSERT_TRUE(a.has_value());
    ASSERT_TRUE(b.has_value());
    EXPECT_EQ(a->firstSequenceNumber, b->firstSequenceNumber);
    EXPECT_EQ(a->lastSequenceNumber, b->lastSequenceNumber);
    EXPECT_EQ(a->root, b->root) << "block " << blockIndex << ": determinism means every instance "
                                 << "produces an identical root (specification.md §8.4)";
    EXPECT_EQ(a->signature, b->signature) << "block " << blockIndex << ": same key, same message, "
                                           << "same deterministic Ed25519 signature";
  }

  gateway1.stop();
  gateway2.stop();
  std::filesystem::remove_all(dir);
}

}  // namespace
}  // namespace sequencer::evidence::detail
