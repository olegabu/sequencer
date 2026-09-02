#include <sequencer/quickfix/journal_message_store.hpp>

#include <quickfix/Message.h>
#include <quickfix/Values.h>

namespace sequencer::quickfix {

JournalMessageStore::JournalMessageStore(std::string sessionKey, BodySource& bodies,
                                          SequenceNumberStore& sequences)
    : sessionKey_(std::move(sessionKey)), bodies_(bodies), sequences_(sequences) {
  sequences_.load(sessionKey_, nextSender_, nextTarget_);
}

void JournalMessageStore::noteOrigin(std::uint64_t journalSequenceNumber,
                                      std::uint32_t outputIndex) {
  std::lock_guard<std::mutex> lock(mutex_);
  pendingJournalSequenceNumber_ = journalSequenceNumber;
  pendingOutputIndex_ = outputIndex;
}

bool JournalMessageStore::set(int seqNum, const std::string& message)
    QUICKFIX_THROW(FIX::IOException) {
  // The bytes are deliberately dropped. What is kept is where they can
  // be rebuilt from, plus the two header fields a rebuild cannot infer:
  // the MsgType QuickFIX chose and the SendingTime it stamped.
  SentRow row;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    row.journalSequenceNumber = pendingJournalSequenceNumber_;
    row.outputIndex = pendingOutputIndex_;
  }

  FIX::Message parsed;
  try {
    parsed.setString(message, false);
    FIX::MsgType msgType;
    parsed.getHeader().getField(msgType);
    row.msgType = msgType.getValue();
    FIX::SendingTime sendingTime;
    if (parsed.getHeader().isSetField(sendingTime)) {
      parsed.getHeader().getField(sendingTime);
      row.sendingTime = sendingTime.getString();
    }
  } catch (const FIX::Exception&) {
    // A message we cannot parse is one we cannot rebuild. Record what
    // is known; get() will gap-fill over it rather than replay
    // something wrong.
    row.msgType.clear();
  }

  std::lock_guard<std::mutex> lock(mutex_);
  sent_[seqNum] = std::move(row);
  return true;
}

void JournalMessageStore::get(int begin, int end, std::vector<std::string>& out) const
    QUICKFIX_THROW(FIX::IOException) {
  std::vector<std::pair<int, SentRow>> rows;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    for (auto it = sent_.lower_bound(begin); it != sent_.end() && it->first <= end; ++it) {
      rows.emplace_back(it->first, it->second);
    }
  }

  for (const auto& [seqNum, row] : rows) {
    if (row.journalSequenceNumber == 0 || row.msgType.empty()) {
      // Nothing to rebuild from -- an administrative message, or one
      // whose provenance was never recorded. Omitting it from the
      // result is the right answer: QuickFIX gap-fills whatever the
      // store does not return, which is exactly what FIX 4.4 wants for
      // an admin message.
      continue;
    }
    std::string msgType;
    std::string body;
    if (!bodies_.bodyFor(row.journalSequenceNumber, row.outputIndex, msgType, body)) {
      continue;  // record gone; gap-fill covers it
    }

    // Rebuilt whole, not body-only: QuickFIX parses this string, reads
    // its MsgType to choose between gap fill and replay, and writes
    // PossDupFlag and OrigSendingTime into it before sending. It must
    // carry the ORIGINAL sequence number, which is the point of the
    // exercise.
    FIX::Message rebuilt;
    rebuilt.getHeader().setField(FIX::BeginString("FIX.4.4"));
    rebuilt.getHeader().setField(FIX::MsgType(row.msgType.empty() ? msgType : row.msgType));
    rebuilt.getHeader().setField(FIX::MsgSeqNum(seqNum));
    if (!row.sendingTime.empty()) {
      rebuilt.getHeader().setField(FIX::SendingTime(
          FIX::UtcTimeStampConvertor::convert(row.sendingTime)));
    }
    // The codec's body arrives as raw tag=value SOH pairs, the same
    // shape the hffix gateway pushes through appendRawFields.
    std::size_t pos = 0;
    while (pos < body.size()) {
      const std::size_t soh = body.find('\001', pos);
      if (soh == std::string::npos) {
        break;
      }
      const std::string field = body.substr(pos, soh - pos);
      const std::size_t eq = field.find('=');
      if (eq != std::string::npos) {
        rebuilt.setField(FIX::FieldBase(std::atoi(field.substr(0, eq).c_str()),
                                         field.substr(eq + 1)));
      }
      pos = soh + 1;
    }
    out.push_back(rebuilt.toString());
  }
}

int JournalMessageStore::getNextSenderMsgSeqNum() const QUICKFIX_THROW(FIX::IOException) {
  std::lock_guard<std::mutex> lock(mutex_);
  return nextSender_;
}

int JournalMessageStore::getNextTargetMsgSeqNum() const QUICKFIX_THROW(FIX::IOException) {
  std::lock_guard<std::mutex> lock(mutex_);
  return nextTarget_;
}

void JournalMessageStore::setNextSenderMsgSeqNum(int value) QUICKFIX_THROW(FIX::IOException) {
  {
    std::lock_guard<std::mutex> lock(mutex_);
    nextSender_ = value;
  }
  sequences_.save(sessionKey_, value, getNextTargetMsgSeqNum());
}

void JournalMessageStore::setNextTargetMsgSeqNum(int value) QUICKFIX_THROW(FIX::IOException) {
  {
    std::lock_guard<std::mutex> lock(mutex_);
    nextTarget_ = value;
  }
  sequences_.save(sessionKey_, getNextSenderMsgSeqNum(), value);
}

void JournalMessageStore::incrNextSenderMsgSeqNum() QUICKFIX_THROW(FIX::IOException) {
  int value = 0;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    value = ++nextSender_;
  }
  sequences_.save(sessionKey_, value, getNextTargetMsgSeqNum());
}

void JournalMessageStore::incrNextTargetMsgSeqNum() QUICKFIX_THROW(FIX::IOException) {
  int value = 0;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    value = ++nextTarget_;
  }
  sequences_.save(sessionKey_, getNextSenderMsgSeqNum(), value);
}

FIX::UtcTimeStamp JournalMessageStore::getCreationTime() const QUICKFIX_THROW(FIX::IOException) {
  return created_;
}

void JournalMessageStore::reset() QUICKFIX_THROW(FIX::IOException) {
  {
    std::lock_guard<std::mutex> lock(mutex_);
    sent_.clear();
    nextSender_ = 1;
    nextTarget_ = 1;
    created_ = FIX::UtcTimeStamp();
  }
  sequences_.save(sessionKey_, 1, 1);
}

void JournalMessageStore::refresh() QUICKFIX_THROW(FIX::IOException) {
  std::lock_guard<std::mutex> lock(mutex_);
  sequences_.load(sessionKey_, nextSender_, nextTarget_);
}

FIX::MessageStore* JournalMessageStoreFactory::create(const FIX::SessionID& sessionId) {
  auto* store = new JournalMessageStore(sessionId.toString(), bodies_, sequences_);
  std::lock_guard<std::mutex> lock(mutex_);
  stores_[sessionId.toString()] = store;
  return store;
}

void JournalMessageStoreFactory::destroy(FIX::MessageStore* store) {
  {
    std::lock_guard<std::mutex> lock(mutex_);
    for (auto it = stores_.begin(); it != stores_.end(); ++it) {
      if (it->second == store) {
        stores_.erase(it);
        break;
      }
    }
  }
  delete store;
}

JournalMessageStore* JournalMessageStoreFactory::storeFor(const FIX::SessionID& sessionId) {
  std::lock_guard<std::mutex> lock(mutex_);
  const auto it = stores_.find(sessionId.toString());
  return it == stores_.end() ? nullptr : it->second;
}

}  // namespace sequencer::quickfix
