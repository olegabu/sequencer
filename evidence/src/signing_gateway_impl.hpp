#pragma once

// The signing gateway (specification.md §8.4): an ordinary, colocated
// journal reader — "no different in kind from an output gateway" — that
// cuts each complete block, builds its Merkle tree, and signs the root.
// No InputCodec, no OutputCodec: nothing here is application-specific,
// which is why, like the relay gateway (§9's repository layout),
// evidence/ builds this as a ready-to-run stock binary rather than
// something an application links.
//
// Per-block memory footprint is O(1): only {bounds, root, signature}
// is retained (signedBlocks_ below) — a block's leaves are recomputed
// from the journal on demand when a proof is requested, not cached.
// This is a direct, working instance of specification.md §7.2's own
// claim: "signed roots are themselves published... so an inclusion
// proof is reconstructible from public data forever." It also means
// this gateway's memory use never grows with the number of blocks it
// has signed, however long it has been running.

#include <atomic>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <thread>

#include <sequencer/evidence/block.hpp>
#include <sequencer/evidence/merkle.hpp>
#include <sequencer/journal/reader.hpp>

namespace sequencer::evidence::detail {

struct SigningGatewayConfig {
  std::filesystem::path dataDir;  // a node's journal directory, colocated (§3)
  Ed25519PrivateKey privateKey;
  int listenPort = 0;
};

struct SignedBlockMeta {
  std::uint64_t firstSequenceNumber = 0;
  std::uint64_t lastSequenceNumber = 0;
  Hash32 root{};
  Signature64 signature{};
};

class SigningGatewayImpl {
 public:
  explicit SigningGatewayImpl(SigningGatewayConfig config) : config_(std::move(config)) {}

  SigningGatewayImpl(const SigningGatewayImpl&) = delete;
  SigningGatewayImpl& operator=(const SigningGatewayImpl&) = delete;

  ~SigningGatewayImpl() {
    if (started_) {
      stop();
    }
  }

  void start() {
    tailThread_ = std::thread([this] { tailLoop(); });
    started_ = true;
  }

  void stop() {
    stopRequested_.store(true, std::memory_order_relaxed);
    if (tailThread_.joinable()) {
      tailThread_.join();
    }
    started_ = false;
  }

  // specification.md §8.4: "expose its own lag as a first-class health
  // metric — its lag alone, among all consumers, is an alarm rather
  // than staleness." 0 means either nothing signed yet or the journal
  // isn't readable yet; callers wanting an actual alarm compare this
  // against blockIndexForSequence(committedCount) themselves (or, for
  // a colocated caller, read the journal directly — see
  // signing_gateway_test.cpp).
  std::uint64_t lastSignedBlockIndex() const { return lastSignedBlockIndex_.load(std::memory_order_relaxed); }

  std::optional<SignedBlockMeta> signedBlock(std::uint64_t blockIndex) const {
    std::lock_guard<std::mutex> lock(mutex_);
    const auto it = signedBlocks_.find(blockIndex);
    return it == signedBlocks_.end() ? std::nullopt : std::make_optional(it->second);
  }

  std::optional<InclusionProof> inclusionProof(std::uint64_t sequenceNumber) const {
    SignedBlockMeta meta;
    std::shared_ptr<journal::JournalReader> reader;
    {
      std::lock_guard<std::mutex> lock(mutex_);
      const auto it = signedBlocks_.find(blockIndexForSequence(sequenceNumber));
      if (it == signedBlocks_.end()) {
        return std::nullopt;
      }
      meta = it->second;
      reader = reader_;
    }

    std::vector<Hash32> leaves = leavesForBlock(*reader, meta.firstSequenceNumber, meta.lastSequenceNumber);
    const std::uint64_t indexInBlock = sequenceNumber - meta.firstSequenceNumber;

    InclusionProof proof;
    proof.sequenceNumber = sequenceNumber;
    proof.blockFirstSequenceNumber = meta.firstSequenceNumber;
    proof.blockLastSequenceNumber = meta.lastSequenceNumber;
    proof.siblingHashes = buildProofPath(leaves, indexInBlock);
    proof.root = meta.root;
    proof.signature = meta.signature;
    return proof;
  }

  int listenPort() const { return config_.listenPort; }

 private:
  static std::vector<Hash32> leavesForBlock(const journal::JournalReader& reader, std::uint64_t first,
                                             std::uint64_t last) {
    std::vector<Hash32> leaves;
    leaves.reserve(last - first + 1);
    for (std::uint64_t seq = first; seq <= last; ++seq) {
      leaves.push_back(leafHash(reader.record(seq).rawBytes(), seq));
    }
    return leaves;
  }

  void tailLoop() {
    std::uint64_t nextBlockIndex = 1;
    while (!stopRequested_.load(std::memory_order_relaxed)) {
      std::shared_ptr<journal::JournalReader> reader = currentReader();
      if (!reader) {
        try {
          reader = std::make_shared<journal::JournalReader>(config_.dataDir / "journal");
          std::lock_guard<std::mutex> lock(mutex_);
          reader_ = reader;
        } catch (const std::exception&) {
          // The journal file pair may not exist yet — retry.
          std::this_thread::sleep_for(std::chrono::milliseconds(50));
          continue;
        }
      }

      if (!blockIsComplete(nextBlockIndex, reader->committedCount())) {
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
        continue;
      }

      signBlock(*reader, nextBlockIndex);
      ++nextBlockIndex;
    }
  }

  std::shared_ptr<journal::JournalReader> currentReader() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return reader_;
  }

  void signBlock(const journal::JournalReader& reader, std::uint64_t blockIndex) {
    const BlockBounds bounds = blockBounds(blockIndex);
    const std::vector<Hash32> leaves = leavesForBlock(reader, bounds.firstSequenceNumber, bounds.lastSequenceNumber);
    const Hash32 root = computeRoot(leaves);
    const Signature64 signature =
        signRoot(config_.privateKey, root, bounds.firstSequenceNumber, bounds.lastSequenceNumber);

    SignedBlockMeta meta{.firstSequenceNumber = bounds.firstSequenceNumber,
                          .lastSequenceNumber = bounds.lastSequenceNumber,
                          .root = root,
                          .signature = signature};
    {
      std::lock_guard<std::mutex> lock(mutex_);
      signedBlocks_[blockIndex] = meta;
    }
    lastSignedBlockIndex_.store(blockIndex, std::memory_order_relaxed);
  }

  SigningGatewayConfig config_;
  mutable std::mutex mutex_;
  std::shared_ptr<journal::JournalReader> reader_;                // guarded by mutex_
  std::map<std::uint64_t, SignedBlockMeta> signedBlocks_;         // guarded by mutex_
  std::atomic<std::uint64_t> lastSignedBlockIndex_{0};
  std::thread tailThread_;
  std::atomic<bool> stopRequested_{false};
  bool started_ = false;
};

}  // namespace sequencer::evidence::detail
