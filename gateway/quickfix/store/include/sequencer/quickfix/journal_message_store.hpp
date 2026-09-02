#pragma once

// A FIX::MessageStore backed by the journal (specification.md §8.13).
//
// QuickFIX requires a message store per session so it can answer a
// ResendRequest. The stock implementations write every outbound message
// to a file, which would put a second copy of every execution report
// beside the one the journal already holds -- and then require the two
// to be reconciled after a crash. §8.12 reason 1 exists to avoid
// exactly that.
//
// So this store keeps no message bytes. Per outbound message it records
// one fixed-size row -- where in the journal the message came from, its
// MsgType, and the SendingTime QuickFIX stamped on it -- and rebuilds
// the message on demand by re-reading that journal record and re-running
// the output codec over it. The codec is a pure function of the record,
// so the reconstruction is deterministic.
//
// It must RECONSTRUCT rather than merely return a body, because
// QuickFIX's resend loop does not put get()'s output on the wire: it
// parses each string, reads its MsgType to choose between a gap fill and
// a replay, and rewrites PossDupFlag and OrigSendingTime into it
// (Session::nextResendRequest). A complete, parseable message carrying
// its ORIGINAL sequence number is the contract.

#include <quickfix/MessageStore.h>
#include <quickfix/SessionID.h>

#include <cstdint>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

namespace sequencer::journal {
class JournalReader;
}
namespace sequencer {
class OutputCodec;
}

namespace sequencer::quickfix {

// Where one outbound message came from. No bytes: everything here is
// enough to rebuild it, and nothing here is a copy of it.
struct SentRow {
  std::uint64_t journalSequenceNumber = 0;
  std::uint32_t outputIndex = 0;
  std::string msgType;
  std::string sendingTime;
};

// Supplies the body of a message that was produced from a journal
// record. Implemented over the output codec; an interface so the store
// is testable without a journal on disk.
class BodySource {
 public:
  virtual ~BodySource() = default;
  // Returns false if the record is gone or produced no such output.
  virtual bool bodyFor(std::uint64_t journalSequenceNumber, std::uint32_t outputIndex,
                        std::string& msgTypeOut, std::string& bodyOut) = 0;
};

// The persisted sequence-number pair, which is the only session state
// that must outlive the process (§8.12). Shared with the hffix gateway's
// own store rather than duplicated: both need exactly this.
class SequenceNumberStore {
 public:
  virtual ~SequenceNumberStore() = default;
  virtual void load(const std::string& sessionKey, int& nextSender, int& nextTarget) = 0;
  virtual void save(const std::string& sessionKey, int nextSender, int nextTarget) = 0;
};

class JournalMessageStore : public FIX::MessageStore {
 public:
  JournalMessageStore(std::string sessionKey, BodySource& bodies, SequenceNumberStore& sequences);

  // Called by the output side immediately before it asks QuickFIX to
  // send, so the set() that follows knows which journal record the
  // message it is being handed was built from.
  //
  // This coupling is deliberate and is the one awkward seam in the
  // design: FIX::MessageStore::set() receives only a sequence number and
  // bytes, and no interface QuickFIX offers carries provenance. The
  // alternative is storing the bytes, which is the thing being avoided.
  void noteOrigin(std::uint64_t journalSequenceNumber, std::uint32_t outputIndex);

  // --- FIX::MessageStore ---
  bool set(int seqNum, const std::string& message) QUICKFIX_THROW(FIX::IOException) override;
  void get(int begin, int end, std::vector<std::string>& out) const
      QUICKFIX_THROW(FIX::IOException) override;

  int getNextSenderMsgSeqNum() const QUICKFIX_THROW(FIX::IOException) override;
  int getNextTargetMsgSeqNum() const QUICKFIX_THROW(FIX::IOException) override;
  void setNextSenderMsgSeqNum(int value) QUICKFIX_THROW(FIX::IOException) override;
  void setNextTargetMsgSeqNum(int value) QUICKFIX_THROW(FIX::IOException) override;
  void incrNextSenderMsgSeqNum() QUICKFIX_THROW(FIX::IOException) override;
  void incrNextTargetMsgSeqNum() QUICKFIX_THROW(FIX::IOException) override;

  FIX::UtcTimeStamp getCreationTime() const QUICKFIX_THROW(FIX::IOException) override;
  void reset() QUICKFIX_THROW(FIX::IOException) override;
  void refresh() QUICKFIX_THROW(FIX::IOException) override;

 private:
  std::string sessionKey_;
  BodySource& bodies_;
  SequenceNumberStore& sequences_;

  mutable std::mutex mutex_;
  std::map<int, SentRow> sent_;
  std::uint64_t pendingJournalSequenceNumber_ = 0;
  std::uint32_t pendingOutputIndex_ = 0;
  int nextSender_ = 1;
  int nextTarget_ = 1;
  FIX::UtcTimeStamp created_;
};

class JournalMessageStoreFactory : public FIX::MessageStoreFactory {
 public:
  JournalMessageStoreFactory(BodySource& bodies, SequenceNumberStore& sequences)
      : bodies_(bodies), sequences_(sequences) {}

  FIX::MessageStore* create(const FIX::SessionID& sessionId) override;
  void destroy(FIX::MessageStore* store) override;

  // The store for a session, so the output side can call noteOrigin()
  // on it. QuickFIX owns the lifetime; this is a borrowed pointer.
  JournalMessageStore* storeFor(const FIX::SessionID& sessionId);

 private:
  BodySource& bodies_;
  SequenceNumberStore& sequences_;
  std::mutex mutex_;
  std::map<std::string, JournalMessageStore*> stores_;
};

}  // namespace sequencer::quickfix
