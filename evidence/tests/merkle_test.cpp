#include <sequencer/evidence/merkle.hpp>

#include <cstring>
#include <string>
#include <vector>

#include <gtest/gtest.h>

namespace sequencer::evidence {
namespace {

Payload payloadOf(const std::string& s) {
  return Payload(reinterpret_cast<const std::byte*>(s.data()), s.size());
}

Ed25519PrivateKey testPrivateKey() {
  Ed25519PrivateKey key{};
  for (std::size_t i = 0; i < key.size(); ++i) {
    key[i] = static_cast<std::byte>(i + 1);
  }
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

std::vector<Hash32> makeLeaves(std::size_t count) {
  std::vector<Hash32> leaves;
  leaves.reserve(count);
  for (std::size_t i = 0; i < count; ++i) {
    const std::string body = "record-" + std::to_string(i);
    leaves.push_back(leafHash(payloadOf(body), i + 1));
  }
  return leaves;
}

TEST(Merkle, LeafHashIsDeterministicAndPositionSensitive) {
  const std::string body = "same body";
  const Hash32 a = leafHash(payloadOf(body), 5);
  const Hash32 b = leafHash(payloadOf(body), 5);
  const Hash32 c = leafHash(payloadOf(body), 6);
  EXPECT_EQ(a, b);
  EXPECT_NE(a, c);
}

TEST(Merkle, EveryLeafInAFullBlockProvesAgainstTheSameRoot) {
  const std::vector<Hash32> leaves = makeLeaves(1024);
  const Hash32 root = computeRoot(leaves);

  for (std::size_t i = 0; i < leaves.size(); ++i) {
    const std::vector<Hash32> path = buildProofPath(leaves, i);
    EXPECT_EQ(path.size(), 10u) << "log2(1024) sibling hashes, index " << i;
    const Hash32 recomputed = recomputeRoot(leaves[i], i, path);
    EXPECT_EQ(recomputed, root) << "leaf index " << i;
  }
}

TEST(Merkle, ATamperedLeafFailsToProveAgainstTheOriginalRoot) {
  const std::vector<Hash32> leaves = makeLeaves(16);
  const Hash32 root = computeRoot(leaves);
  const std::vector<Hash32> path = buildProofPath(leaves, 3);

  const Hash32 tamperedLeaf = leafHash(payloadOf("not the real record"), 4);
  const Hash32 recomputed = recomputeRoot(tamperedLeaf, 3, path);
  EXPECT_NE(recomputed, root);
}

TEST(Merkle, RejectsNonPowerOfTwoLeafCounts) {
  const std::vector<Hash32> leaves = makeLeaves(5);
  EXPECT_THROW(computeRoot(leaves), std::invalid_argument);
  EXPECT_THROW(buildProofPath(leaves, 0), std::invalid_argument);
}

TEST(Merkle, RootSignatureRoundTrips) {
  const Ed25519PrivateKey privateKey = testPrivateKey();
  const Ed25519PublicKey publicKey = derivePublicKey(privateKey);

  const std::vector<Hash32> leaves = makeLeaves(1024);
  const Hash32 root = computeRoot(leaves);
  const Signature64 signature = signRoot(privateKey, root, 1, 1024);

  EXPECT_TRUE(verifyRootSignature(publicKey, root, 1, 1024, signature));
  EXPECT_FALSE(verifyRootSignature(publicKey, root, 1, 1023, signature)) << "tampered bounds";

  Hash32 differentRoot = root;
  differentRoot[0] ^= std::byte{0xff};
  EXPECT_FALSE(verifyRootSignature(publicKey, differentRoot, 1, 1024, signature)) << "tampered root";
}

TEST(Merkle, VerifyInclusionProofSucceedsForARealBlockAndFailsUnderTampering) {
  const Ed25519PrivateKey privateKey = testPrivateKey();
  const Ed25519PublicKey publicKey = derivePublicKey(privateKey);

  const std::vector<Hash32> leaves = makeLeaves(1024);
  const Hash32 root = computeRoot(leaves);
  const Signature64 signature = signRoot(privateKey, root, 1, 1024);

  const std::size_t indexInBlock = 777;
  InclusionProof proof;
  proof.sequenceNumber = indexInBlock + 1;
  proof.blockFirstSequenceNumber = 1;
  proof.blockLastSequenceNumber = 1024;
  proof.siblingHashes = buildProofPath(leaves, indexInBlock);
  proof.root = root;
  proof.signature = signature;

  const std::string realBody = "record-" + std::to_string(indexInBlock);
  EXPECT_TRUE(verifyInclusionProof(proof, publicKey, payloadOf(realBody)));

  EXPECT_FALSE(verifyInclusionProof(proof, publicKey, payloadOf("a substituted record")))
      << "specification.md §7.3: a substituted record fails verification at the client";

  InclusionProof outOfBounds = proof;
  outOfBounds.sequenceNumber = 2000;
  EXPECT_FALSE(verifyInclusionProof(outOfBounds, publicKey, payloadOf(realBody)))
      << "claimed sequence number must fall within the signed bounds";

  InclusionProof wrongSignature = proof;
  wrongSignature.signature[0] ^= std::byte{0xff};
  EXPECT_FALSE(verifyInclusionProof(wrongSignature, publicKey, payloadOf(realBody)));
}

}  // namespace
}  // namespace sequencer::evidence
