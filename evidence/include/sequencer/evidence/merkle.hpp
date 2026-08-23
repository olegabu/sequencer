#pragma once

// The Merkle math behind specification.md §7.2: leaf hashing, tree
// construction, inclusion-proof generation, and inclusion-proof
// verification (root recomputation plus the operator's Ed25519
// signature check). Header-only and dependent on nothing but OpenSSL
// (§9.1: "the verification-only slice of sdk/... needs only a
// cryptography library") — evidence/'s compiled signing gateway
// includes this to build proofs, and sdk/'s header-only verification
// slice includes the very same header to check them, so the two sides
// are byte-for-byte the same code, not two independent reimplementations
// that could quietly drift apart.
//
// A block is always exactly kBlockSize (evidence/block.hpp) records —
// specification.md §7.1 never cuts a partial block — so every tree
// built here has a power-of-two leaf count and is perfectly balanced.
// That is relied on directly: no "promote the odd node" logic exists
// because there is never an odd node to promote.

#include <array>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <stdexcept>
#include <vector>

#include <openssl/evp.h>

#include <sequencer/payload.hpp>

namespace sequencer::evidence {

using Hash32 = std::array<std::byte, 32>;
using Signature64 = std::array<std::byte, 64>;
using Ed25519PublicKey = std::array<std::byte, 32>;
using Ed25519PrivateKey = std::array<std::byte, 32>;  // the raw 32-byte seed, RFC 8032

namespace detail {

inline void putU64LittleEndian(std::byte* dst, std::uint64_t v) {
  for (int i = 0; i < 8; ++i) {
    dst[i] = static_cast<std::byte>((v >> (8 * i)) & 0xff);
  }
}

inline Hash32 sha256(std::initializer_list<Payload> parts) {
  EVP_MD_CTX* ctx = EVP_MD_CTX_new();
  if (ctx == nullptr) {
    throw std::runtime_error("EVP_MD_CTX_new failed");
  }
  Hash32 out{};
  unsigned int outLen = 0;
  const bool ok = EVP_DigestInit_ex(ctx, EVP_sha256(), nullptr) == 1 &&
                  [&] {
                    for (const Payload& part : parts) {
                      if (EVP_DigestUpdate(ctx, part.data(), part.size()) != 1) {
                        return false;
                      }
                    }
                    return true;
                  }() &&
                  EVP_DigestFinal_ex(ctx, reinterpret_cast<unsigned char*>(out.data()), &outLen) == 1;
  EVP_MD_CTX_free(ctx);
  if (!ok || outLen != out.size()) {
    throw std::runtime_error("sha256: OpenSSL digest failed");
  }
  return out;
}

}  // namespace detail

// specification.md §7.2: "hash(rawRecordBytes ‖ sequenceNumber)" — a
// leaf over one complete journal entry (input and outputs alike), with
// the sequence number bound in so a leaf can never be replayed at a
// different slot.
inline Hash32 leafHash(Payload rawRecordBytes, std::uint64_t sequenceNumber) {
  std::array<std::byte, 8> seqBytes{};
  detail::putU64LittleEndian(seqBytes.data(), sequenceNumber);
  return detail::sha256({rawRecordBytes, Payload(seqBytes.data(), seqBytes.size())});
}

// Domain-separated from leafHash (a leading 0x01 byte no leaf hash
// input ever has room to produce, since a leaf hash's own inputs are
// the record bytes and sequence number, not this function's output) so
// an internal node can never be mistaken for — or substituted as — a
// leaf.
inline Hash32 internalHash(const Hash32& left, const Hash32& right) {
  const std::byte tag{0x01};
  return detail::sha256(
      {Payload(&tag, 1), Payload(left.data(), left.size()), Payload(right.data(), right.size())});
}

// leaves.size() must be a power of two (see the file comment).
inline Hash32 computeRoot(const std::vector<Hash32>& leaves) {
  if (leaves.empty() || (leaves.size() & (leaves.size() - 1)) != 0) {
    throw std::invalid_argument("computeRoot: leaf count must be a nonzero power of two");
  }
  std::vector<Hash32> level = leaves;
  while (level.size() > 1) {
    std::vector<Hash32> next;
    next.reserve(level.size() / 2);
    for (std::size_t i = 0; i < level.size(); i += 2) {
      next.push_back(internalHash(level[i], level[i + 1]));
    }
    level = std::move(next);
  }
  return level.front();
}

// Sibling hashes for leaves[index], bottom-up — exactly what a
// verifier needs to fold leaves[index] up to the root one level at a
// time. leaves.size() must be a power of two.
inline std::vector<Hash32> buildProofPath(const std::vector<Hash32>& leaves, std::size_t index) {
  if (leaves.empty() || (leaves.size() & (leaves.size() - 1)) != 0) {
    throw std::invalid_argument("buildProofPath: leaf count must be a nonzero power of two");
  }
  if (index >= leaves.size()) {
    throw std::out_of_range("buildProofPath: index out of range");
  }
  std::vector<Hash32> level = leaves;
  std::vector<Hash32> path;
  std::size_t i = index;
  while (level.size() > 1) {
    const std::size_t siblingIndex = (i % 2 == 0) ? i + 1 : i - 1;
    path.push_back(level[siblingIndex]);
    std::vector<Hash32> next;
    next.reserve(level.size() / 2);
    for (std::size_t j = 0; j < level.size(); j += 2) {
      next.push_back(internalHash(level[j], level[j + 1]));
    }
    level = std::move(next);
    i /= 2;
  }
  return path;
}

// The verifier's half of buildProofPath: folds `leaf` up through
// `siblingPath` (bottom-up, as buildProofPath produced it) using
// `indexInBlock` to know, at each level, whether the running hash is
// the left or right child.
inline Hash32 recomputeRoot(Hash32 leaf, std::uint64_t indexInBlock,
                             const std::vector<Hash32>& siblingPath) {
  Hash32 current = leaf;
  std::uint64_t i = indexInBlock;
  for (const Hash32& sibling : siblingPath) {
    current = (i % 2 == 0) ? internalHash(current, sibling) : internalHash(sibling, current);
    i /= 2;
  }
  return current;
}

// A generic raw-Ed25519 sign/verify pair, factored out of
// signRoot/verifyRootSignature below so sdk/'s client-side signer
// (which signs a client request's payload directly, an entirely
// different message shape) can reuse the same OpenSSL plumbing instead
// of duplicating it — see sdk/cpp/include/sequencer/sdk/client_signer.hpp.
inline Signature64 signBytes(const Ed25519PrivateKey& privateKey, Payload message) {
  EVP_PKEY* key = EVP_PKEY_new_raw_private_key(EVP_PKEY_ED25519, nullptr,
                                                reinterpret_cast<const unsigned char*>(privateKey.data()),
                                                privateKey.size());
  if (key == nullptr) {
    throw std::runtime_error("signBytes: EVP_PKEY_new_raw_private_key failed");
  }

  Signature64 signature{};
  EVP_MD_CTX* ctx = EVP_MD_CTX_new();
  std::size_t sigLen = signature.size();
  const bool ok = ctx != nullptr && EVP_DigestSignInit(ctx, nullptr, nullptr, nullptr, key) == 1 &&
                  EVP_DigestSign(ctx, reinterpret_cast<unsigned char*>(signature.data()), &sigLen,
                                 reinterpret_cast<const unsigned char*>(message.data()), message.size()) == 1;
  if (ctx != nullptr) {
    EVP_MD_CTX_free(ctx);
  }
  EVP_PKEY_free(key);
  if (!ok || sigLen != signature.size()) {
    throw std::runtime_error("signBytes: OpenSSL Ed25519 signing failed");
  }
  return signature;
}

inline bool verifyBytesSignature(const Ed25519PublicKey& publicKey, Payload message,
                                  const Signature64& signature) {
  EVP_PKEY* key = EVP_PKEY_new_raw_public_key(EVP_PKEY_ED25519, nullptr,
                                               reinterpret_cast<const unsigned char*>(publicKey.data()),
                                               publicKey.size());
  if (key == nullptr) {
    return false;
  }

  EVP_MD_CTX* ctx = EVP_MD_CTX_new();
  const bool ok =
      ctx != nullptr && EVP_DigestVerifyInit(ctx, nullptr, nullptr, nullptr, key) == 1 &&
      EVP_DigestVerify(ctx, reinterpret_cast<const unsigned char*>(signature.data()), signature.size(),
                        reinterpret_cast<const unsigned char*>(message.data()), message.size()) == 1;
  if (ctx != nullptr) {
    EVP_MD_CTX_free(ctx);
  }
  EVP_PKEY_free(key);
  return ok;
}

// specification.md §7.2: "signature(root ‖ firstSequenceNumber ‖
// lastSequenceNumber)" — the bounds are folded into the signed message
// itself, so a proof is self-contained: a verifier checks the claimed
// sequence number against the *signed* bounds, never a value it had to
// derive on its own.
inline Signature64 signRoot(const Ed25519PrivateKey& privateKey, const Hash32& root,
                             std::uint64_t firstSequenceNumber, std::uint64_t lastSequenceNumber) {
  std::array<std::byte, 16> bounds{};
  detail::putU64LittleEndian(bounds.data(), firstSequenceNumber);
  detail::putU64LittleEndian(bounds.data() + 8, lastSequenceNumber);

  std::array<std::byte, 32 + 16> message{};
  std::memcpy(message.data(), root.data(), root.size());
  std::memcpy(message.data() + root.size(), bounds.data(), bounds.size());
  return signBytes(privateKey, Payload(message.data(), message.size()));
}

inline bool verifyRootSignature(const Ed25519PublicKey& publicKey, const Hash32& root,
                                 std::uint64_t firstSequenceNumber, std::uint64_t lastSequenceNumber,
                                 const Signature64& signature) {
  std::array<std::byte, 16> bounds{};
  detail::putU64LittleEndian(bounds.data(), firstSequenceNumber);
  detail::putU64LittleEndian(bounds.data() + 8, lastSequenceNumber);

  std::array<std::byte, 32 + 16> message{};
  std::memcpy(message.data(), root.data(), root.size());
  std::memcpy(message.data() + root.size(), bounds.data(), bounds.size());
  return verifyBytesSignature(publicKey, Payload(message.data(), message.size()), signature);
}

// specification.md §7's whole point, self-contained: everything a
// client needs to check one record's inclusion in a signed history,
// without trusting anything the operator says beyond this struct's own
// signature field.
struct InclusionProof {
  std::uint64_t sequenceNumber;
  std::uint64_t blockFirstSequenceNumber;
  std::uint64_t blockLastSequenceNumber;
  std::vector<Hash32> siblingHashes;  // bottom-up, from buildProofPath
  Hash32 root;
  Signature64 signature;
};

// specification.md §7.4: "verify a proof against locally retained
// submitted bytes, never against anything the venue echoes back" —
// `rawRecordBytes` must be the caller's own reconstruction of the
// complete journal entry (journal::encodeRecord over the exact input it
// submitted and the outputs it independently knows belong at this
// sequence number), never bytes taken from the proof or from the
// operator.
inline bool verifyInclusionProof(const InclusionProof& proof, const Ed25519PublicKey& publicKey,
                                  Payload rawRecordBytes) {
  if (proof.sequenceNumber < proof.blockFirstSequenceNumber ||
      proof.sequenceNumber > proof.blockLastSequenceNumber) {
    return false;
  }
  const Hash32 leaf = leafHash(rawRecordBytes, proof.sequenceNumber);
  const std::uint64_t indexInBlock = proof.sequenceNumber - proof.blockFirstSequenceNumber;
  const Hash32 recomputed = recomputeRoot(leaf, indexInBlock, proof.siblingHashes);
  if (recomputed != proof.root) {
    return false;
  }
  return verifyRootSignature(publicKey, proof.root, proof.blockFirstSequenceNumber,
                              proof.blockLastSequenceNumber, proof.signature);
}

}  // namespace sequencer::evidence
