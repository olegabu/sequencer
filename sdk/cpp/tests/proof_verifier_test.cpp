#include <sequencer/sdk/proof_verifier.hpp>

#include <sequencer/evidence/block.hpp>

#include <cstdint>
#include <cstring>
#include <vector>

#include <gtest/gtest.h>

namespace sequencer::sdk {
namespace {

Payload payloadOf(const std::int64_t& v) {
  return Payload(reinterpret_cast<const std::byte*>(&v), sizeof(v));
}

evidence::Ed25519PrivateKey testPrivateKey() {
  evidence::Ed25519PrivateKey key{};
  key.fill(std::byte{0x21});
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

// Builds a fully signed, real block (specification.md §7.2) the same
// way evidence/'s own signing gateway would, so this test exercises
// sdk::verifyInclusionProof against the exact byte layout the real
// signing path produces — not a hand-rolled substitute.
struct Block {
  std::vector<evidence::Hash32> leaves;
  evidence::Hash32 root;
  evidence::Signature64 signature;
};

Block buildSignedBlock(const evidence::Ed25519PrivateKey& privateKey, std::uint64_t first,
                        std::uint64_t last) {
  Block block;
  for (std::uint64_t seq = first; seq <= last; ++seq) {
    const std::int64_t value = static_cast<std::int64_t>(seq);
    const std::size_t rawSize = journal::recordEncodedSize(payloadOf(value), {});
    std::vector<std::byte> raw(rawSize);
    journal::encodeRecord(raw.data(), seq, payloadOf(value), {});
    block.leaves.push_back(evidence::leafHash(Payload(raw.data(), raw.size()), seq));
  }
  block.root = evidence::computeRoot(block.leaves);
  block.signature = evidence::signRoot(privateKey, block.root, first, last);
  return block;
}

TEST(ProofVerifier, AcceptsAGenuineRecordReconstructedFromInputAndOutput) {
  const evidence::Ed25519PrivateKey privateKey = testPrivateKey();
  const evidence::Ed25519PublicKey publicKey = derivePublicKey(privateKey);
  const Block block = buildSignedBlock(privateKey, 1, evidence::kBlockSize);

  const std::uint64_t provenSeq = 42;
  InclusionProof proof;
  proof.sequenceNumber = provenSeq;
  proof.blockFirstSequenceNumber = 1;
  proof.blockLastSequenceNumber = evidence::kBlockSize;
  proof.siblingHashes = evidence::buildProofPath(block.leaves, provenSeq - 1);
  proof.root = block.root;
  proof.signature = block.signature;

  const std::int64_t value = static_cast<std::int64_t>(provenSeq);
  EXPECT_TRUE(verifyInclusionProof(proof, publicKey, payloadOf(value), std::span<const Payload>{}));
}

TEST(ProofVerifier, RejectsAProofAgainstTheWrongLocallyRetainedInput) {
  const evidence::Ed25519PrivateKey privateKey = testPrivateKey();
  const evidence::Ed25519PublicKey publicKey = derivePublicKey(privateKey);
  const Block block = buildSignedBlock(privateKey, 1, evidence::kBlockSize);

  const std::uint64_t provenSeq = 42;
  InclusionProof proof;
  proof.sequenceNumber = provenSeq;
  proof.blockFirstSequenceNumber = 1;
  proof.blockLastSequenceNumber = evidence::kBlockSize;
  proof.siblingHashes = evidence::buildProofPath(block.leaves, provenSeq - 1);
  proof.root = block.root;
  proof.signature = block.signature;

  const std::int64_t wrongValue = 9999;
  EXPECT_FALSE(verifyInclusionProof(proof, publicKey, payloadOf(wrongValue), std::span<const Payload>{}))
      << "specification.md §7.4: verify against locally retained bytes — a caller with the wrong "
      << "bytes must never be told a proof is valid";
}

TEST(ProofVerifier, SingleOutputOverloadMatchesTheGeneralOverload) {
  const evidence::Ed25519PrivateKey privateKey = testPrivateKey();
  const evidence::Ed25519PublicKey publicKey = derivePublicKey(privateKey);

  const std::uint64_t provenSeq = 7;
  const std::int64_t input = 1;
  const std::int64_t output = 100;

  std::vector<evidence::Hash32> leaves;
  for (std::uint64_t seq = 1; seq <= evidence::kBlockSize; ++seq) {
    const std::int64_t inputValue = seq == provenSeq ? input : static_cast<std::int64_t>(seq);
    std::vector<Payload> outputs;
    Payload outputPayload = payloadOf(output);
    if (seq == provenSeq) {
      outputs.push_back(outputPayload);
    }
    const std::size_t rawSize = journal::recordEncodedSize(payloadOf(inputValue), outputs);
    std::vector<std::byte> raw(rawSize);
    journal::encodeRecord(raw.data(), seq, payloadOf(inputValue), outputs);
    leaves.push_back(evidence::leafHash(Payload(raw.data(), raw.size()), seq));
  }
  const evidence::Hash32 root = evidence::computeRoot(leaves);
  const evidence::Signature64 signature = evidence::signRoot(privateKey, root, 1, evidence::kBlockSize);

  InclusionProof proof;
  proof.sequenceNumber = provenSeq;
  proof.blockFirstSequenceNumber = 1;
  proof.blockLastSequenceNumber = evidence::kBlockSize;
  proof.siblingHashes = evidence::buildProofPath(leaves, provenSeq - 1);
  proof.root = root;
  proof.signature = signature;

  EXPECT_TRUE(verifyInclusionProof(proof, publicKey, payloadOf(input), payloadOf(output)));
}

}  // namespace
}  // namespace sequencer::sdk
