#include <sequencer/fix/fix_session.hpp>

#include <hffix_fields.hpp>

#include <algorithm>
#include <cctype>
#include <charconv>
#include <chrono>
#include <cstring>
#include <stdexcept>

namespace sequencer::fix {
namespace {

// hffix writes fields; these read them back. A message_reader is a
// range of fields, so a lookup is a linear scan -- which is the right
// shape at FIX message sizes and avoids building an index per message
// (the allocation §8.12 reason 3 exists to avoid).

// Appends a pre-encoded FIX body -- "tag=value<SOH>" repeated -- field
// by field.
//
// hffix's message_writer deliberately exposes no raw append: every
// field goes through a push_back so that BodyLength and CheckSum stay
// correct, which is exactly the property we want it to keep. So a body
// handed over as bytes (specification.md §8.10: the codec builds the
// body, the transport adds the session fields) is split here and
// re-pushed. It is a scan of a few dozen bytes with no allocation, and
// it means a malformed body cannot corrupt the framing -- a field
// without '=' is skipped rather than written through.
void appendRawFields(hffix::message_writer& writer, std::string_view body) {
  std::size_t pos = 0;
  while (pos < body.size()) {
    const std::size_t soh = body.find('\001', pos);
    const std::size_t fieldEnd = (soh == std::string_view::npos) ? body.size() : soh;
    const std::string_view field = body.substr(pos, fieldEnd - pos);
    const std::size_t eq = field.find('=');
    if (eq != std::string_view::npos && eq > 0) {
      int tag = 0;
      const auto [ptr, ec] = std::from_chars(field.data(), field.data() + eq, tag);
      if (ec == std::errc() && ptr == field.data() + eq) {
        const std::string_view value = field.substr(eq + 1);
        writer.push_back_string(tag, value.data(), value.data() + value.size());
      }
    }
    if (soh == std::string_view::npos) {
      break;
    }
    pos = soh + 1;
  }
}

std::optional<std::string_view> field(const hffix::message_reader& message, int tag) {
  for (auto it = message.begin(); it != message.end(); ++it) {
    if (it->tag() == tag) {
      return std::string_view(it->value().begin(), it->value().size());
    }
  }
  return std::nullopt;
}

std::optional<std::uint64_t> uintField(const hffix::message_reader& message, int tag) {
  const std::optional<std::string_view> raw = field(message, tag);
  if (!raw.has_value() || raw->empty()) {
    return std::nullopt;
  }
  std::uint64_t value = 0;
  const auto [ptr, ec] = std::from_chars(raw->data(), raw->data() + raw->size(), value);
  if (ec != std::errc() || ptr != raw->data() + raw->size()) {
    return std::nullopt;
  }
  return value;
}

bool isTrue(const std::optional<std::string_view>& raw) {
  return raw.has_value() && !raw->empty() && ((*raw)[0] == 'Y' || (*raw)[0] == 'y');
}

// A message type is a single char for every type this session layer
// handles; application types may be multi-char (U1, U2), which is why
// the caller gets the raw view too.
std::string_view msgTypeView(const hffix::message_reader& message) {
  const auto type = message.message_type();
  if (type == message.end()) {
    return {};
  }
  return std::string_view(type->value().begin(), type->value().size());
}

// hffix's is_valid() checks that the message is STRUCTURALLY sound --
// BeginString, then BodyLength, then MsgType, with a CheckSum field
// where BodyLength says it should be -- but deliberately does not
// compare the checksum's VALUE. Its documentation hands the caller
// calculate_check_sum() and check_sum() to do that, which for this
// repository is right: specification.md §8.12 makes framing, BodyLength
// and checksum the session layer's own validation, and this is that.
//
// Worth stating plainly because a reader who sees is_valid() may assume
// otherwise: without this, a message with a corrupted checksum is
// accepted and acted upon.
bool checksumMatches(const hffix::message_reader& message) {
  // calculate_check_sum() is non-const and message_reader is documented
  // as immutable and cheap to copy, so this takes a copy rather than
  // casting away constness.
  hffix::message_reader copy(message);
  const std::string_view raw(copy.message_begin(), copy.message_size());

  // The declared value is parsed out of the trailer directly rather
  // than through check_sum(): hffix excludes the trailer from the
  // iterable field range exactly as it excludes the header, so
  // check_sum() compares equal to end() and a lookup finds nothing.
  // A message that reached here is_valid(), which means BodyLength
  // already located a CheckSum field at the end, so the tail is
  // "10=NNN<SOH>".
  constexpr std::size_t kTrailerSize = 7;  // "10=" + 3 digits + SOH
  if (raw.size() < kTrailerSize) {
    return false;
  }
  const std::string_view trailer = raw.substr(raw.size() - kTrailerSize);
  if (trailer.substr(0, 3) != "10=") {
    return false;
  }
  unsigned int declared = 0;
  const char* digits = trailer.data() + 3;
  const auto [ptr, ec] = std::from_chars(digits, digits + 3, declared);
  if (ec != std::errc() || ptr != digits + 3) {
    return false;
  }
  return static_cast<unsigned char>(declared) == copy.calculate_check_sum();
}

bool isSessionLevel(std::string_view msgType) {
  if (msgType.size() != 1) {
    return false;  // multi-char types are application messages (U1, U2, ...)
  }
  switch (msgType[0]) {
    case '0':  // Heartbeat
    case '1':  // TestRequest
    case '2':  // ResendRequest
    case '3':  // Reject
    case '4':  // SequenceReset
    case '5':  // Logout
    case 'A':  // Logon
      return true;
    default:
      return false;
  }
}

}  // namespace

FixSession::FixSession(SessionConfig config, SequenceStore& sequences, ClockFn clock,
                        WallClockFn wallClock)
    : config_(std::move(config)),
      sequences_store_(sequences),
      clock_(std::move(clock)),
      wallClock_(wallClock ? std::move(wallClock) : WallClockFn([] {
        return static_cast<std::uint64_t>(
            std::chrono::duration_cast<std::chrono::microseconds>(
                std::chrono::system_clock::now().time_since_epoch())
                .count());
      })) {
  // The key both sides of a session agree on, and what the sequence
  // store is keyed by. Direction matters: an acceptor's sender is the
  // initiator's target.
  //
  // An acceptor with no configured targetCompId does not yet know who
  // is calling, so the key -- and therefore the counters -- cannot be
  // resolved until its Logon arrives. Loading here anyway would key the
  // counters on an empty peer name and hand every reconnecting client
  // somebody else's sequence numbers, so it is deferred to
  // adoptIdentity().
  if (config_.role == Role::Acceptor && config_.targetCompId.empty()) {
    identityPending_ = true;
  } else {
    sessionKey_ = config_.senderCompId + "->" + config_.targetCompId;
    sequences_ = sequences_store_.load(sessionKey_);
  }
  // Reserved once; the message path must not grow these (§8.12 reason 3).
  receive_.reserve(64 * 1024);
  buffer_.reserve(8 * 1024);
}

FixSession::~FixSession() {
  // Best effort: a store that throws here would take the process down
  // during teardown, which is never the right trade for a counter file.
  try {
    persist();
  } catch (...) {
  }
}

void FixSession::start() {
  std::lock_guard<std::recursive_mutex> lock(mutex_);

  lastReceivedUs_ = clock_();
  lastSentUs_ = clock_();
  if (config_.role == Role::Initiator) {
    // The reset happens BEFORE the Logon goes out, not when the reply
    // comes back.
    //
    // FIX 4.4: with ResetSeqNumFlag=Y both sides restart at 1 and the
    // Logon exchange ITSELF is sequence 1, so the next message is 2.
    // Resetting on the reply instead -- which is what this did -- put
    // the Logon on the wire at 1 and then set the outbound counter back
    // to 1, so the very next message repeated sequence 1 and any
    // counterparty logged us out for a sequence number below its
    // expectation.
    //
    // It went unnoticed because both ends of this repository make the
    // same mistake symmetrically: our acceptor also reset to 1 after
    // the exchange, so our client and our gateway agreed with each
    // other and disagreed with everyone else. Pointing the load
    // generator at a real QuickFIX acceptor is what exposed it.
    if (config_.resetSeqNumOnLogon) {
      sequences_.nextInbound = 1;
      sequences_.nextOutbound = 1;
      persist();
    }
    sendLogon();
    state_ = State::LogonSent;
  } else {
    state_ = State::AwaitingLogon;
  }
}

void FixSession::onBytes(std::string_view bytes) {
  // Application messages are PARSED under the lock but DISPATCHED
  // without it.
  //
  // onApp_ is where the gateway runs the codec and starts a proposal,
  // and a 16KB read holds several messages. Holding the session lock
  // across all of that put the inbound reader in direct contention with
  // every propose-completion thread trying to answer inline: the
  // gateway stopped reading its client sockets, the load generator
  // could not push its offered rate, and throughput sat at ~12k/s with
  // the gateway's CPU IDLE and 2.3% of it in sched_yield. The lock was
  // never expensive -- it was held across the wrong work.
  std::vector<std::string> deferred;
  {
    std::lock_guard<std::recursive_mutex> lock(mutex_);
    deferredApp_.clear();

  receive_.append(bytes.data(), bytes.size());
  lastReceivedUs_ = clock_();

  std::size_t consumed = 0;
  while (consumed < receive_.size()) {
    hffix::message_reader reader(receive_.data() + consumed, receive_.size() - consumed);
    if (!reader.is_complete()) {
      break;  // wait for more bytes
    }
    if (!reader.is_valid() || !checksumMatches(reader)) {
      // Framing, BodyLength or CheckSum failure. FIX 4.4 says a
      // garbled message is not to be answered with a Reject (the
      // sequence number cannot be trusted); the session drops.
      disconnect(DisconnectReason::MalformedFraming);
      receive_.clear();
      deferredApp_.clear();
      return;
    }
    // message_size(), NOT buffer_size(): the latter is the size of the
    // whole buffer handed to the reader, so using it consumed
    // everything after the first message and silently dropped any
    // frame that arrived coalesced with another.
    const std::size_t messageSize = reader.message_size();
    handleMessage(reader);
    consumed += messageSize;
    if (state_ == State::Disconnected) {
      receive_.clear();
      deferredApp_.clear();
      return;
    }
  }

  // Keep only the incomplete tail. erase-from-front on a std::string is
  // a move of the remainder, which is bounded by one message.
  if (consumed > 0) {
    receive_.erase(0, consumed);
  }
    deferred.swap(deferredApp_);
  }

  if (!onApp_) {
    return;
  }
  for (const std::string& raw : deferred) {
    hffix::message_reader reader(raw.data(), raw.size());
    if (reader.is_complete() && reader.is_valid()) {
      onApp_(reader);
    }
  }
}

void FixSession::handleMessage(const hffix::message_reader& message) {
  const std::string_view type = msgTypeView(message);
  if (type.empty()) {
    disconnect(DisconnectReason::MalformedFraming);
    return;
  }

  // Logon is the one message allowed to arrive before logon completes.
  if (state_ != State::LoggedOn && state_ != State::LogoutSent && type == "A") {
    handleLogon(message);
    return;
  }
  if (state_ == State::Idle || state_ == State::AwaitingLogon ||
      state_ == State::Disconnected) {
    // Anything but Logon before the session is up is out of sequence.
    return;
  }

  const char typeChar = type.size() == 1 ? type[0] : '\0';

  // SequenceReset-with-GapFill=N is a reset and is processed WITHOUT
  // the sequence check, per FIX 4.4 -- that is the whole point of it.
  if (typeChar == '4') {
    handleSequenceReset(message);
    return;
  }

  if (!checkSequence(message, typeChar)) {
    return;
  }

  switch (typeChar) {
    case '0':  // Heartbeat
      testRequestOutstanding_ = false;
      return;
    case '1':
      handleTestRequest(message);
      return;
    case '2':
      handleResendRequest(message);
      return;
    case '3':  // Reject -- surfaced as an event; the peer rejected us
      return;
    case '5':
      handleLogout(message);
      return;
    default:
      break;
  }

  if (isSessionLevel(type)) {
    return;
  }
  // Queued rather than dispatched: onBytes runs these once it has let
  // go of the session lock. See its comment.
  if (onApp_) {
    deferredApp_.emplace_back(message.message_begin(), message.message_size());
  }
}

bool FixSession::checkSequence(const hffix::message_reader& message, char /*msgType*/) {
  const std::optional<std::uint64_t> seqNum = uintField(message, hffix::tag::MsgSeqNum);
  if (!seqNum.has_value()) {
    sendReject(0, 1, "missing MsgSeqNum");
    return false;
  }

  const bool possDup = isTrue(field(message, hffix::tag::PossDupFlag));

  if (*seqNum < sequences_.nextInbound) {
    // A PossDup below the expected number is a legitimate resend of
    // something already processed: ignore it, do not disconnect.
    if (possDup) {
      return false;
    }
    // Anything else is a sequence number too low, which FIX 4.4 treats
    // as unrecoverable: logout and drop.
    sendLogout("MsgSeqNum too low");
    disconnect(DisconnectReason::SequenceTooLow);
    return false;
  }

  if (*seqNum > sequences_.nextInbound) {
    // A gap. Ask the peer for the range and stop processing until it
    // is filled -- processing ahead would break ordering.
    sendResendRequest(sequences_.nextInbound, 0);
    return false;
  }

  sequences_.nextInbound = *seqNum + 1;
  // Throttled for the same reason as the journal position: this runs on
  // every inbound message, and a filesystem write there is what capped
  // the gateway near 750 messages/sec.
  persistThrottled();
  return true;
}

// An acceptor learns who its peer is from the Logon's SenderCompID and
// only then loads that session's persisted counters. This is what makes
// a reconnect resume the SAME FIX session: the counters are keyed by
// the CompID pair, which survives the socket, rather than by the
// connection, which does not.
void FixSession::adoptIdentity(const hffix::message_reader& message) {
  const std::optional<std::string_view> peer = field(message, hffix::tag::SenderCompID);
  if (peer.has_value() && !peer->empty()) {
    config_.targetCompId.assign(peer->data(), peer->size());
  }
  sessionKey_ = config_.senderCompId + "->" + config_.targetCompId;
  sequences_ = sequences_store_.load(sessionKey_);
  identityPending_ = false;
}

void FixSession::handleLogon(const hffix::message_reader& message) {
  const std::optional<std::string_view> username = field(message, hffix::tag::Username);
  const std::optional<std::string_view> password = field(message, hffix::tag::Password);
  if (!authenticate_(username.value_or(std::string_view{}),
                      password.value_or(std::string_view{}))) {
    sendLogout("authentication failed");
    disconnect(DisconnectReason::AuthenticationFailed);
    return;
  }

  // Identity first: everything below reads or writes the counters, and
  // until the peer is known those belong to no session in particular.
  if (identityPending_) {
    adoptIdentity(message);
  }

  // A second live connection claiming an identity that is already in
  // use would interleave two writers onto one pair of counters. Refuse
  // it rather than corrupt both.
  if (identityGuard_ && !identityGuard_(sessionKey_)) {
    sendLogout("session already connected");
    disconnect(DisconnectReason::DuplicateIdentity);
    return;
  }

  // Only an ACCEPTOR resets here. An initiator that asked for the reset
  // already did it before sending its Logon (see start()); resetting
  // again on the echoed reply would undo the sequence number its own
  // Logon consumed, and put the next message back at 1.
  if (config_.role == Role::Acceptor &&
      isTrue(field(message, hffix::tag::ResetSeqNumFlag))) {
    sequences_.nextInbound = 1;
    sequences_.nextOutbound = 1;
    persist();
    if (onEvent_) {
      onEvent_(SessionEvent::SequenceReset, DisconnectReason::None);
    }
  }

  const std::optional<std::uint64_t> heartBt = uintField(message, hffix::tag::HeartBtInt);
  if (heartBt.has_value()) {
    // FIX 4.4: the acceptor echoes the initiator's proposal, so after
    // this both sides hold the same interval whichever role we are.
    config_.heartBtInt = static_cast<int>(*heartBt);
  }

  // ACCEPT THE LOGON FIRST, then check the sequence. FIX 4.4 is
  // explicit that a Logon whose sequence number is HIGHER than expected
  // must still be processed, with a ResendRequest sent afterwards --
  // the session is established and the gap is filled inside it.
  //
  // Doing it the other way round, as this first did, deadlocks a
  // perfectly ordinary reconnect: a client returning with fresh
  // counters sees the acceptor's persisted outbound number, reports a
  // gap, and the logon never completes -- so the resend that would have
  // closed the gap can never be requested. checkSequence() still
  // rejects a number that is too LOW, which remains fatal.
  if (config_.role == Role::Acceptor) {
    sendLogon();  // the echo
  }
  state_ = State::LoggedOn;
  if (onEvent_) {
    onEvent_(SessionEvent::LogonComplete, DisconnectReason::None);
  }

  // Runs last: on a gap this sends the ResendRequest, and on a
  // too-low number it disconnects, both from an established session.
  checkSequence(message, 'A');
}

void FixSession::handleLogout(const hffix::message_reader& /*message*/) {
  if (state_ != State::LogoutSent) {
    sendLogout("responding to Logout");  // complete the handshake
  }
  if (onEvent_) {
    onEvent_(SessionEvent::LogoutComplete, DisconnectReason::PeerLogout);
  }
  disconnect(DisconnectReason::PeerLogout);
}

void FixSession::handleTestRequest(const hffix::message_reader& message) {
  const std::optional<std::string_view> id = field(message, hffix::tag::TestReqID);
  sendHeartbeat(id.value_or(std::string_view{}));
}

void FixSession::handleResendRequest(const hffix::message_reader& message) {
  const std::optional<std::uint64_t> begin = uintField(message, hffix::tag::BeginSeqNo);
  const std::optional<std::uint64_t> end = uintField(message, hffix::tag::EndSeqNo);
  if (!begin.has_value() || !end.has_value()) {
    sendReject(0, 1, "ResendRequest missing BeginSeqNo/EndSeqNo");
    return;
  }
  // EndSeqNo 0 means "everything from BeginSeqNo".
  const std::uint64_t last = (*end == 0) ? (sequences_.nextOutbound - 1) : *end;
  if (last < *begin) {
    sendReject(0, 5, "ResendRequest range is empty");
    return;
  }

  // specification.md §8.12 reason 1: there is no outbound message
  // store. What was sent is re-derived from the journal through this
  // hook. When it cannot serve the range -- administrative messages,
  // or a gateway with no journal behind it -- the correct FIX answer
  // is a gap fill, not silence.
  bool served = false;
  if (resendSource_ != nullptr) {
    std::uint64_t nextExpected = *begin;
    inResend_ = true;
    served = resendSource_->resend(*begin, last,
                                    [&](std::uint64_t seqNum, std::string_view msgType,
                                        std::string_view body,
                                        std::string_view origSendingTime) {
                                      if (seqNum > nextExpected) {
                                        // Administrative messages in the
                                        // range are gap-filled, not resent.
                                        sendGapFill(nextExpected, seqNum);
                                      }
                                      // Re-send at the ORIGINAL sequence
                                      // number, with the original body and
                                      // SendingTime, flagged PossDup -- FIX
                                      // 4.4's rules for a resend. inResend_
                                      // keeps emit() from renumbering.
                                      const std::uint64_t saved = sequences_.nextOutbound;
                                      sequences_.nextOutbound = seqNum;
                                      emit(msgType,
                                            [&](hffix::message_writer& writer) {
                                              appendRawFields(writer, body);
                                            },
                                            true, origSendingTime);
                                      sequences_.nextOutbound = saved;
                                      nextExpected = seqNum + 1;
                                    });
    inResend_ = false;
    if (served && nextExpected <= last) {
      sendGapFill(nextExpected, last + 1);
    }
  }
  if (!served) {
    sendGapFill(*begin, last + 1);
  }
}

void FixSession::handleSequenceReset(const hffix::message_reader& message) {
  const std::optional<std::uint64_t> newSeqNo = uintField(message, hffix::tag::NewSeqNo);
  if (!newSeqNo.has_value()) {
    sendReject(0, 1, "SequenceReset missing NewSeqNo");
    return;
  }
  if (*newSeqNo < sequences_.nextInbound) {
    sendReject(0, 5, "SequenceReset NewSeqNo is lower than expected");
    return;
  }
  sequences_.nextInbound = *newSeqNo;
  persist();
  if (onEvent_) {
    onEvent_(SessionEvent::SequenceReset, DisconnectReason::None);
  }
}

void FixSession::poll() {
  std::lock_guard<std::recursive_mutex> lock(mutex_);

  if (state_ != State::LoggedOn) {
    return;
  }
  const std::uint64_t now = clock_();
  const std::uint64_t intervalUs = static_cast<std::uint64_t>(config_.heartBtInt) * 1'000'000ULL;
  if (intervalUs == 0) {
    return;  // heartbeats disabled (HeartBtInt=0 is legal and means "none")
  }

  // Our own heartbeat, so the peer does not think us silent.
  if (now - lastSentUs_ >= intervalUs) {
    sendHeartbeat();
  }

  const std::uint64_t silence = now - lastReceivedUs_;
  const auto testAt = static_cast<std::uint64_t>(static_cast<double>(intervalUs) * config_.testRequestFactor);
  const auto dropAt = static_cast<std::uint64_t>(static_cast<double>(intervalUs) * config_.disconnectFactor);

  if (silence >= dropAt) {
    sendLogout("peer silent");
    disconnect(DisconnectReason::PeerSilence);
    return;
  }
  if (silence >= testAt && !testRequestOutstanding_) {
    sendTestRequest();
  }
}

std::uint64_t FixSession::sendApplication(std::string_view msgType, std::string_view body) {
  std::lock_guard<std::recursive_mutex> lock(mutex_);

  return emit(msgType, [&](hffix::message_writer& writer) {
    // The body arrives already encoded as FIX fields by the codec and
    // is re-pushed field by field between header and trailer -- see
    // appendRawFields. This is what keeps the transport free of
    // application knowledge (§8.5).
    appendRawFields(writer, body);
  });
}

void FixSession::logout(std::string_view text) {
  std::lock_guard<std::recursive_mutex> lock(mutex_);

  sendLogout(text);
  state_ = State::LogoutSent;
}

// --- emission -------------------------------------------------------

std::uint64_t FixSession::emit(std::string_view msgType,
                                const std::function<void(hffix::message_writer&)>& fill,
                                bool isPossDup, std::string_view origSendingTime) {
  buffer_.resize(buffer_.capacity());
  hffix::message_writer writer(buffer_.data(), buffer_.data() + buffer_.size());
  writer.push_back_header(config_.beginString.c_str());

  const std::uint64_t seqNum = sequences_.nextOutbound;
  if (!msgType.empty()) {
    writer.push_back_string(hffix::tag::MsgType, msgType.data(), msgType.data() + msgType.size());
  }
  writer.push_back_string(hffix::tag::SenderCompID, config_.senderCompId.c_str());
  writer.push_back_string(hffix::tag::TargetCompID, config_.targetCompId.c_str());
  writer.push_back_int(hffix::tag::MsgSeqNum, static_cast<int>(seqNum));
  const std::string now = timestampNow();
  writer.push_back_string(hffix::tag::SendingTime, now.c_str());
  if (isPossDup) {
    writer.push_back_string(hffix::tag::PossDupFlag, "Y");
    if (!origSendingTime.empty()) {
      writer.push_back_string(hffix::tag::OrigSendingTime, origSendingTime.data(),
                               origSendingTime.data() + origSendingTime.size());
    }
  }

  fill(writer);
  writer.push_back_trailer();

  if (send_) {
    send_(std::string_view(writer.message_begin(), writer.message_size()));
  }
  lastSentUs_ = clock_();
  if (!inResend_) {
    sequences_.nextOutbound = seqNum + 1;
    // Throttled, like the inbound counter and the journal position.
    // This is the OUTBOUND path and it was missed when the other two
    // were throttled -- so every message the gateway sent still did a
    // file write and a rename, and the gateway still folded at ~1000
    // messages/sec after the other two were fixed. Measuring the A/B
    // rather than assuming the first fix had worked is what found it.
    persistThrottled();
  }
  return seqNum;
}

void FixSession::sendLogon() {
  emit("A", [&](hffix::message_writer& writer) {
    writer.push_back_int(hffix::tag::EncryptMethod, 0);
    writer.push_back_int(hffix::tag::HeartBtInt, config_.heartBtInt);
    if (config_.resetSeqNumOnLogon) {
      writer.push_back_string(hffix::tag::ResetSeqNumFlag, "Y");
    }
  });
}

void FixSession::sendLogout(std::string_view text) {
  emit("5", [&](hffix::message_writer& writer) {
    if (!text.empty()) {
      writer.push_back_string(hffix::tag::Text, text.data(), text.data() + text.size());
    }
  });
}

void FixSession::sendHeartbeat(std::string_view testReqId) {
  emit("0", [&](hffix::message_writer& writer) {
    if (!testReqId.empty()) {
      writer.push_back_string(hffix::tag::TestReqID, testReqId.data(),
                               testReqId.data() + testReqId.size());
    }
  });
}

void FixSession::sendTestRequest() {
  const std::string id = std::to_string(++testRequestId_);
  emit("1", [&](hffix::message_writer& writer) {
    writer.push_back_string(hffix::tag::TestReqID, id.c_str());
  });
  testRequestOutstanding_ = true;
}

void FixSession::sendReject(std::uint64_t refSeqNum, int reason, std::string_view text) {
  emit("3", [&](hffix::message_writer& writer) {
    writer.push_back_int(hffix::tag::RefSeqNum, static_cast<int>(refSeqNum));
    writer.push_back_int(hffix::tag::SessionRejectReason, reason);
    if (!text.empty()) {
      writer.push_back_string(hffix::tag::Text, text.data(), text.data() + text.size());
    }
  });
}

void FixSession::sendResendRequest(std::uint64_t begin, std::uint64_t end) {
  emit("2", [&](hffix::message_writer& writer) {
    writer.push_back_int(hffix::tag::BeginSeqNo, static_cast<int>(begin));
    writer.push_back_int(hffix::tag::EndSeqNo, static_cast<int>(end));
  });
}

void FixSession::sendGapFill(std::uint64_t fromSeqNum, std::uint64_t newSeqNum) {
  const std::uint64_t saved = sequences_.nextOutbound;
  sequences_.nextOutbound = fromSeqNum;
  const bool wasInResend = inResend_;
  inResend_ = true;
  emit("4", [&](hffix::message_writer& writer) {
    writer.push_back_string(hffix::tag::GapFillFlag, "Y");
    writer.push_back_int(hffix::tag::NewSeqNo, static_cast<int>(newSeqNum));
  }, true);
  inResend_ = wasInResend;
  sequences_.nextOutbound = saved;
}

void FixSession::disconnect(DisconnectReason reason) {
  if (state_ == State::Disconnected) {
    return;
  }
  state_ = State::Disconnected;
  disconnectReason_ = reason;
  persist();
  if (onEvent_) {
    onEvent_(SessionEvent::Disconnected, reason);
  }
}

void FixSession::persist() {
  sequences_store_.store(sessionKey_, sequences_);
  lastPersistUs_ = clock_();
}

void FixSession::persistThrottled() {
  const std::uint64_t now = clock_();
  if (now - lastPersistUs_ < kPersistIntervalUs) {
    return;
  }
  persist();
}

std::string FixSession::timestampNow() const {
  // FIX UTCTimestamp with milliseconds, from the WALL clock. Not from
  // clock_, which is monotonic and whose epoch is arbitrary -- doing
  // that put 1970 on the wire and every message was rejected by a real
  // engine for SendingTime accuracy.
  const std::uint64_t us = wallClock_();
  const std::time_t seconds = static_cast<std::time_t>(us / 1'000'000ULL);
  const int millis = static_cast<int>((us % 1'000'000ULL) / 1000ULL);
  std::tm tm{};
  ::gmtime_r(&seconds, &tm);
  char out[32];
  const int n = std::snprintf(out, sizeof(out), "%04d%02d%02d-%02d:%02d:%02d.%03d",
                               tm.tm_year + 1900, tm.tm_mon + 1, tm.tm_mday, tm.tm_hour,
                               tm.tm_min, tm.tm_sec, millis);
  return std::string(out, static_cast<std::size_t>(n));
}

}  // namespace sequencer::fix
