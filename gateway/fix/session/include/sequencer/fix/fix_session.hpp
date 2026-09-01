#pragma once

// The FIX 4.4 session state machine — specification.md §8.12.
//
// Owned rather than taken from an engine, for the three reasons §8.12
// gives and gateway/fix/README.md restates: the journal is already the
// resend store, the load harness needs this same state machine in
// initiator role, and hffix leaves threading and allocation to us.
// Read that README before proposing QuickFIX here.
//
// DELIBERATELY HAS NO SOCKET. This class consumes bytes and produces
// bytes through callbacks; Asio lives one layer up, in the transports.
// Three things follow from that, all of them the reason it is built
// this way:
//
//   - it is unit-testable against a scripted peer, with no ports, no
//     timing races, and no listening socket per test case;
//   - the same instance serves as acceptor (gateway) and initiator
//     (bench/load_generator), which is §8.12's reason 2 -- the roles
//     differ in who sends Logon first and in nothing else structural;
//   - time is injected, so heartbeat and TestRequest behaviour is
//     tested by advancing a counter rather than by sleeping.
//
// Validation is deliberately minimal (§8.12): framing, BodyLength,
// CheckSum, and the required session-level fields. Application-message
// validation belongs to the codec or a typed layer, not here, and this
// class must not grow a data dictionary.

#include <hffix.hpp>

#include <cstddef>
#include <cstdint>
#include <functional>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

namespace sequencer::fix {

// specification.md §8.12: "two roles of one protocol". The difference
// is who initiates Logon and who echoes the negotiated heartbeat
// interval; everything else -- sequence rules, resends, heartbeats,
// logout -- is shared, which is the whole argument for owning this.
enum class Role {
  Acceptor,
  Initiator,
};

// What the session tells the layer above. Application messages are
// handed over separately (see FixSession::onApplicationMessage): these
// are the session-level transitions a transport must react to.
enum class SessionEvent {
  LogonComplete,     // both directions authenticated; app traffic may flow
  LogoutComplete,    // orderly Logout handshake finished
  Disconnected,      // peer silence, framing failure, or a fatal reject
  SequenceReset,     // counters reset (ResetSeqNumFlag or SequenceReset)
};

// Why a session ended, for logging and for distinguishing an orderly
// FIX Logout from a socket drop -- specification.md's §8.1 onDisconnect
// contract wants those told apart.
enum class DisconnectReason {
  None,
  PeerLogout,
  PeerSilence,
  MalformedFraming,
  SequenceTooLow,
  AuthenticationFailed,
  DuplicateIdentity,  // another live connection already holds this CompID pair
  LocalShutdown,
};

struct SessionConfig {
  Role role = Role::Acceptor;
  std::string beginString = "FIX.4.4";
  std::string senderCompId;
  // For an ACCEPTOR this may be left empty: the peer's identity is not
  // known until its Logon arrives, and the session adopts it from the
  // Logon's SenderCompID (tag 49). That adoption is what lets a client
  // reconnect to ITS OWN session -- the persisted sequence counters are
  // keyed by the CompID pair, so a reconnecting client resumes where it
  // left off instead of starting a fresh one.
  //
  // For an INITIATOR it is required: an initiator knows who it is
  // calling.
  std::string targetCompId;
  // Negotiated at Logon: an acceptor echoes what the initiator asked
  // for (FIX 4.4 §Logon), an initiator proposes this value.
  int heartBtInt = 30;
  // Multiplier on heartBtInt before a TestRequest is issued, and again
  // before the peer is declared silent. 1.2x/2.4x is the conventional
  // reading of "a reasonable transmission time".
  double testRequestFactor = 1.2;
  double disconnectFactor = 2.4;
  bool resetSeqNumOnLogon = false;
};

// The two counters that are the ONLY session state which must outlive a
// process (specification.md §8.12: "the only session state that must
// persist is a pair of sequence-number counters per session"). Every
// other recoverable fact -- notably what was sent, for resends -- comes
// from the journal.
struct SequenceNumbers {
  std::uint64_t nextOutbound = 1;  // MsgSeqNum to put on the next message we send
  std::uint64_t nextInbound = 1;   // MsgSeqNum we expect to receive next

  // The highest JOURNAL sequence number whose outputs this session has
  // been sent. Not a FIX concept -- it is what specification.md §8.12
  // calls "the session's last persisted position", and it is what makes
  // a returning client catchable-up.
  //
  // It is needed because a resend cannot do that job: an output
  // addressed to a disconnected session is never sent, so the outbound
  // MsgSeqNum never advances for it, so the client has no gap to detect
  // and ResendRequest -- which only replays numbers actually sent --
  // has nothing to offer. Catch-up re-reads the journal from here and
  // sends what was missed as NEW messages.
  std::uint64_t lastJournalSequence = 0;
};

// Persistence for SequenceNumbers. An interface rather than a concrete
// file because the gateway already owns a durable resume-position store
// and may prefer to put these beside it; the tests use an in-memory
// implementation.
class SequenceStore {
 public:
  virtual ~SequenceStore() = default;
  virtual SequenceNumbers load(const std::string& sessionKey) = 0;
  virtual void store(const std::string& sessionKey, const SequenceNumbers& numbers) = 0;
};

// Serves a ResendRequest. specification.md §8.12's reason 1: there is
// NO outbound message store -- a resend re-reads the journal. The
// session core does not know about journals, so it asks through this.
//
// `resend` must call `emit` once per message in [begin, end], in
// ascending sequence order, with the ORIGINAL body of that message; the
// session core adds PossDupFlag and preserves the original SendingTime.
// Returning false means "cannot serve this range", and the session
// answers with a gap fill instead, which is the correct FIX behaviour
// for administrative messages and for anything genuinely unavailable.
class ResendSource {
 public:
  virtual ~ResendSource() = default;
  using Emit = std::function<void(std::uint64_t seqNum, std::string_view msgType,
                                   std::string_view body,
                                   std::string_view originalSendingTime)>;
  virtual bool resend(std::uint64_t begin, std::uint64_t end, const Emit& emit) = 0;
};

// Credential check for Logon (tags 553/554). specification.md §8.12:
// "the session core calls the hook, it does not embed a credential
// store" -- so this is a function, and where the credentials actually
// live is the deployment's business.
using Authenticator = std::function<bool(std::string_view username, std::string_view password)>;

inline Authenticator acceptAnyCredentials() {
  return [](std::string_view, std::string_view) { return true; };
}

class FixSession {
 public:
  // Bytes to put on the wire. Called synchronously from within
  // onBytes()/poll(); the transport owns what happens next.
  using SendFn = std::function<void(std::string_view frame)>;
  // One complete, session-validated application message. The reader is
  // valid only for the duration of the call -- it points into the
  // receive buffer, which is reused, exactly like the arena rule in
  // specification.md §4.
  using AppMessageFn = std::function<void(const hffix::message_reader& message)>;
  using EventFn = std::function<void(SessionEvent event, DisconnectReason reason)>;
  // Monotonic microseconds, for INTERVALS -- heartbeats, TestRequest,
  // peer silence. Injected so tests advance time instead of sleeping;
  // the transports pass a steady_clock reader.
  using ClockFn = std::function<std::uint64_t()>;

  // UTC wall-clock microseconds, for the SendingTime field only.
  //
  // Deliberately a SECOND clock, and the distinction is not academic:
  // deriving SendingTime from the monotonic clock put 1970 on the wire,
  // because steady_clock's epoch is arbitrary (time since boot). Our
  // own initiator accepted it happily -- it shared the mistake -- and
  // QuickFIX rejected every message with "SendingTime accuracy
  // problem", which is what the conformance suite exists to catch.
  //
  // They cannot be one clock. Intervals need a monotonic source that
  // an NTP correction cannot move; SendingTime needs real UTC, which
  // NTP does move.
  using WallClockFn = std::function<std::uint64_t()>;

  // `wallClock` defaults to the system clock, which is what a
  // deployment wants; tests override it to put a fixed time on the
  // wire.
  FixSession(SessionConfig config, SequenceStore& sequences, ClockFn clock,
              WallClockFn wallClock = {});

  // Writes the counters through on the way out. Necessary since
  // persistence was throttled off the per-message path: a session that
  // ends inside the throttle window would otherwise lose its last
  // advance, and the next session for that identity would resume behind
  // where it actually got to -- which in FIX means re-sending sequence
  // numbers the peer has already seen and being disconnected for it.
  //
  // Transports that lose a connection WITHOUT destroying the session
  // must still call flushSequences() themselves; a destructor cannot
  // help there.
  ~FixSession();

  FixSession(const FixSession&) = delete;
  FixSession& operator=(const FixSession&) = delete;

  void setSendFn(SendFn fn) { send_ = std::move(fn); }
  void setAppMessageFn(AppMessageFn fn) { onApp_ = std::move(fn); }
  void setEventFn(EventFn fn) { onEvent_ = std::move(fn); }
  void setAuthenticator(Authenticator fn) { authenticate_ = std::move(fn); }
  void setResendSource(ResendSource* source) { resendSource_ = source; }

  // An initiator opens the session by sending Logon; an acceptor waits
  // for one. Calling this on an acceptor is a no-op, which keeps the
  // transport's startup path role-agnostic.
  void start();

  // Feed received bytes. Handles partial and coalesced messages: the
  // buffer accumulates until at least one complete message is present,
  // and every complete message in it is processed before returning.
  void onBytes(std::string_view bytes);

  // Drive time-based behaviour: heartbeat emission, TestRequest on
  // silence, and disconnect on continued silence. The transport calls
  // this on a timer; tests call it after advancing the clock.
  void poll();

  // Send one application message body. The session core prepends the
  // standard header (BeginString, BodyLength, MsgType, sender/target,
  // MsgSeqNum, SendingTime) and appends the CheckSum, so a caller --
  // an OutputCodec, via FixOutputTransport -- supplies only the
  // application fields.
  //
  // Returns the MsgSeqNum used, which is what the output side records
  // against a journal position for later resends (§8.12 reason 1).
  // `msgType` is a string, not a char: application types are commonly
  // two characters (the counter example's U1/U2, and every other
  // user-defined type in the 5000-9999 convention).
  std::uint64_t sendApplication(std::string_view msgType, std::string_view body);

  // Begin an orderly Logout handshake.
  void logout(std::string_view text = {});

  bool isLoggedOn() const noexcept {
    std::lock_guard<std::recursive_mutex> lock(mutex_);
    return state_ == State::LoggedOn;
  }

  // Writes the counters through unconditionally, bypassing the
  // throttle. A transport MUST call this when a connection ends by any
  // route that does not go through the session -- a socket drop, or a
  // gateway shutdown -- or the last interval's advance is lost and the
  // session resumes behind where it actually got to.
  //
  // This became load-bearing when persistence was throttled off the
  // per-message path: before that, every message wrote through, so a
  // drop could not lose anything.
  void flushSequences() {
    std::lock_guard<std::recursive_mutex> lock(mutex_);
    persist();
  }

  // Called by an acceptor before completing a Logon, to refuse a second
  // concurrent connection claiming an identity that is already live.
  // Returning false makes the session reject the Logon and disconnect.
  //
  // This is the counterpart of identity adoption: once counters are
  // keyed by CompID rather than by connection, two live connections
  // sharing an identity would interleave writes onto one pair of
  // counters and corrupt both.
  using IdentityGuard = std::function<bool(const std::string& sessionKey)>;
  void setIdentityGuard(IdentityGuard guard) { identityGuard_ = std::move(guard); }
  const SequenceNumbers& sequences() const noexcept { return sequences_; }

  // Snapshot, for callers on another thread than the session's reader.
  SequenceNumbers sequencesSnapshot() const {
    std::lock_guard<std::recursive_mutex> lock(mutex_);
    return sequences_;
  }

  // Records how far through the journal this session has been caught
  // up, and persists it. Called by the output side after a successful
  // send; see SequenceNumbers::lastJournalSequence.
  void setLastJournalSequence(std::uint64_t journalSequenceNumber) {
    std::lock_guard<std::recursive_mutex> lock(mutex_);
    if (journalSequenceNumber <= sequences_.lastJournalSequence) {
      return;  // out-of-order or replayed; the high-water mark only rises
    }
    sequences_.lastJournalSequence = journalSequenceNumber;
    persistThrottled();
  }
  const std::string& sessionKey() const noexcept { return sessionKey_; }
  DisconnectReason disconnectReason() const noexcept { return disconnectReason_; }

  // Extension points deliberately left unimplemented (§8.12
  // "Deferred"): FIXT/FIX 5.0 session semantics, and session schedules
  // (start/end times). Neither is stubbed with a silent no-op that
  // could be mistaken for working -- there is simply nothing here, and
  // this comment is where a future implementer starts.

 private:
  // Idle and Disconnected are deliberately distinct. They were one
  // value at first, and that was a bug: disconnect() guards against
  // running twice by checking for the terminal state, so a session that
  // failed during logon -- authentication rejected, framing garbled --
  // was still in the initial state, hit that guard, and recorded no
  // reason at all. A caller then could not tell "never started" from
  // "rejected your credentials".
  enum class State {
    Idle,           // constructed, start() not yet called
    AwaitingLogon,  // acceptor: listening for the peer's Logon
    LogonSent,      // initiator: Logon away, awaiting the echo
    LoggedOn,
    LogoutSent,
    Disconnected,   // terminal
  };

  // --- message handling ---
  // Serializes every mutation of session state.
  //
  // Two threads already reach emit() on the same session: the
  // connection's reader thread (heartbeats, TestRequest replies,
  // ResendRequest, Logout) and the output side's single ring reader
  // (deliver() -> sendApplication). emit() read-modify-writes
  // sequences_.nextOutbound and formats into the shared buffer_, so
  // without this two messages could take the SAME MsgSeqNum or tear
  // into one another's buffer -- both session-fatal in FIX. The window
  // is microseconds and heartbeats are 30s apart, which is why a
  // benchmark run never hit it.
  //
  // Recursive because the public entry points nest through the private
  // handlers (onBytes -> handleLogon -> emit -> persist -> sessionKey).
  mutable std::recursive_mutex mutex_;

  // Application messages parsed in the current onBytes() call, run
  // after the lock is released.
  std::vector<std::string> deferredApp_;

  void handleMessage(const hffix::message_reader& message);
  bool checkSequence(const hffix::message_reader& message, char msgType);
  void adoptIdentity(const hffix::message_reader& message);
  void handleLogon(const hffix::message_reader& message);
  void handleLogout(const hffix::message_reader& message);
  void handleTestRequest(const hffix::message_reader& message);
  void handleResendRequest(const hffix::message_reader& message);
  void handleSequenceReset(const hffix::message_reader& message);

  // --- emission ---
  void sendLogon();
  void sendLogout(std::string_view text);
  void sendHeartbeat(std::string_view testReqId = {});
  void sendTestRequest();
  void sendReject(std::uint64_t refSeqNum, int reason, std::string_view text);
  void sendResendRequest(std::uint64_t begin, std::uint64_t end);
  void sendGapFill(std::uint64_t fromSeqNum, std::uint64_t newSeqNum);

  // Builds the standard header into buffer_, runs `fill` for the body,
  // appends the trailer, and hands the frame to send_. One place, so
  // that MsgSeqNum allocation and SendingTime cannot drift between
  // message types.
  std::uint64_t emit(std::string_view msgType,
                      const std::function<void(hffix::message_writer&)>& fill,
                      bool isPossDup = false, std::string_view origSendingTime = {});

  void disconnect(DisconnectReason reason);

  // Writes the counters through to the store, unconditionally.
  void persist();

  // The same, but at most once every kPersistIntervalUs.
  //
  // The counters were persisted on EVERY message in both directions,
  // and the store writes a temp file and renames it -- two filesystem
  // round trips per message. Measured against examples/counter's FIX
  // gateway, that capped the whole path near 750 messages/sec with
  // latency climbing into seconds, while the sender alone manages ~77k
  // on loopback. It is the single worst bottleneck this gateway had.
  //
  // What a throttle costs is bounded and worth naming: an ungraceful
  // crash can lose up to one interval's advance, so a session may
  // resume slightly behind and re-deliver a few messages it had
  // already sent. FIX is built for exactly that -- they arrive as
  // ordinary messages the peer's own sequence check discards as
  // duplicates -- whereas the alternative is a gateway that cannot
  // carry a thousand messages a second.
  //
  // Logon, Logout, sequence resets and disconnect still persist
  // immediately: those are rare, and each one changes the counters in a
  // way a stale file would make unrecoverable rather than merely
  // duplicated.
  void persistThrottled();

  static constexpr std::uint64_t kPersistIntervalUs = 100'000;  // 100ms
  std::string timestampNow() const;

  SessionConfig config_;
  SequenceStore& sequences_store_;
  ClockFn clock_;
  WallClockFn wallClock_;
  SendFn send_;
  AppMessageFn onApp_;
  EventFn onEvent_;
  Authenticator authenticate_ = acceptAnyCredentials();
  IdentityGuard identityGuard_;
  ResendSource* resendSource_ = nullptr;

  State state_ = State::Idle;
  DisconnectReason disconnectReason_ = DisconnectReason::None;
  SequenceNumbers sequences_;
  std::string sessionKey_;

  // Receive accumulation and send scratch. Both are members and are
  // reserved once, so the steady-state message path does no free-store
  // allocation (specification.md §5.4's discipline, and §8.12 reason 3
  // -- it is a stated reason for owning this layer, so it is tested).
  std::string receive_;
  std::vector<char> buffer_;

  std::uint64_t lastReceivedUs_ = 0;
  std::uint64_t lastSentUs_ = 0;
  std::uint64_t lastPersistUs_ = 0;
  // Acceptor with no configured peer: the counters cannot be loaded
  // until the Logon names it. See adoptIdentity().
  bool identityPending_ = false;
  bool testRequestOutstanding_ = false;
  std::uint64_t testRequestId_ = 0;
  // Set while replaying a ResendRequest, so emit() does not renumber.
  bool inResend_ = false;
};

}  // namespace sequencer::fix
