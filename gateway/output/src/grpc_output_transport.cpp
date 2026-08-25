#include <sequencer/grpc_output_transport.hpp>

#include <grpcpp/ext/proto_server_reflection_plugin.h>
#include <grpcpp/grpcpp.h>

#include "output_grpc.grpc.pb.h"

#include <atomic>
#include <chrono>
#include <deque>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

// Built on gRPC's callback/reactor API (grpc::ServerWriteReactor), not
// the synchronous generated Service — see gateway/relay/README.md's
// "Batching the gRPC stream" section for why: even batched (one
// Write() per gathered batch rather than one per record — this file's
// own earlier design), a blocking synchronous Write() call still ties
// up a dedicated OS thread for the full completion-queue round trip of
// every single call. That earlier version measured ~4.3ms p50 at 100k
// msg/s on a live fleet — the relay's own gRPC Subscribe was at this
// same "batched, sync API" stage before its own reactor rewrite,
// which took it to 649us-1.3ms; this is the same rewrite, applied here
// for the same reason.
//
// Structurally different from the relay's own reactor
// (gateway/relay/src/relay_grpc_service_impl.hpp), which pulls from a
// journal on its own dedicated pump thread: this one is fed by
// *pushes* from OutputGatewayImpl::tailLoop(), via
// SessionRegistry::push() — there's nothing to poll, so there's no
// pump thread here at all. Whichever side currently holds "the write
// baton" — the pushing tailing thread (OutputSubscribeReactor::
// tryContinue(), called from push()) or this reactor's own
// OnWriteDone() — continues the chain directly; see
// OutputSubscribeReactor::gatherBatch()'s own comment for how they
// hand it off without a race.

namespace sequencer {

namespace {

class OutputSubscribeReactor;  // SessionRegistry::push()/closeAll() need the full type, defined below

// Registry of active sessions, shared by GrpcOutputTransport's
// toSession()/broadcast() (called from OutputGatewayImpl's own tailing
// thread) and every OutputSubscribeReactor. A plain struct, not
// GrpcOutputTransport::Impl itself, to sidestep the ordering
// chicken-and-egg between "the service needs the registry" and "the
// transport owns the service."
struct SessionRegistry {
  std::mutex registryMutex;
  std::unordered_map<SessionId, OutputSubscribeReactor*> sessionToReactor;
  std::unordered_map<std::string, std::unordered_set<SessionId>> topicToSessions;
  std::atomic<SessionId> nextSessionId{1};

  SessionId registerSession(const std::string& topic, OutputSubscribeReactor* reactor) {
    std::lock_guard<std::mutex> lock(registryMutex);
    const SessionId id = nextSessionId.fetch_add(1, std::memory_order_relaxed);
    sessionToReactor[id] = reactor;
    topicToSessions[topic].insert(id);
    return id;
  }

  void deregisterSession(SessionId id) {
    std::lock_guard<std::mutex> lock(registryMutex);
    sessionToReactor.erase(id);
    for (auto& [topic, ids] : topicToSessions) {
      ids.erase(id);
    }
  }

  // Defined after OutputSubscribeReactor — needs its full definition.
  void push(SessionId id, const std::shared_ptr<std::string>& message);

  // Wakes every open Subscribe() call so it finishes on its own —
  // called from stop(), belt and suspenders alongside
  // grpc::Server::Shutdown()'s own in-flight-call cancellation, same
  // role RelayGrpcServiceImpl::requestStop() plays for the relay.
  void closeAll();
};

// One instance per active Subscribe() call, owned by gRPC itself once
// returned from GenericOutputServiceImpl::Subscribe (self-deletes in
// OnDone(), the standard reactor-lifetime pattern — same as the
// relay's own RelaySubscribeReactor).
class OutputSubscribeReactor final
    : public ::grpc::ServerWriteReactor<gateway::output::grpc_proto::OutputRecordBatch> {
 public:
  OutputSubscribeReactor(SessionRegistry& registry, const std::string& topic) : registry_(registry) {
    sessionId_ = registry_.registerSession(topic, this);
  }

  // Called only by SessionRegistry::push(), on OutputGatewayImpl's own
  // tailing thread: appends to this session's own pending queue, then
  // tries to continue the write chain (see tryContinue()).
  void enqueueAndContinue(const std::shared_ptr<std::string>& message) {
    {
      std::lock_guard<std::mutex> lock(queueMutex_);
      pending_.push_back(message);
    }
    tryContinue();
  }

  // Starts a write if (and only if) nothing is currently in flight and
  // something is actually queued. Called from two places — push(), via
  // enqueueAndContinue(), and this reactor's own OnWriteDone() — and
  // exactly one of them ever actually starts a write for a given
  // "batch opportunity": whichever finds writeInFlight_ false gathers
  // and writes; the other finds it true and does nothing. OnWriteDone()
  // re-checking (not just handing back to whichever pushed last) is
  // what's load-bearing here: more data can queue *while* a write is
  // in flight, with no guarantee any further push() call ever arrives
  // to notice — this reactor has no journal to poll and re-discover
  // that data the way the relay's own pump thread would.
  void tryContinue() {
    std::unique_lock<std::mutex> lock(mutex_);
    if (writeInFlight_ || stopped_.load(std::memory_order_relaxed)) {
      return;
    }
    if (!gatherBatch()) {
      return;
    }
    writeInFlight_ = true;
    StartWrite(&batch_);
  }

  void OnWriteDone(bool ok) override {
    if (!ok) {
      stopped_.store(true, std::memory_order_relaxed);
      finishOnce();
      return;
    }
    std::unique_lock<std::mutex> lock(mutex_);
    writeInFlight_ = false;
    if (stopped_.load(std::memory_order_relaxed)) {
      lock.unlock();
      finishOnce();
      return;
    }
    if (!gatherBatch()) {
      return;  // idle — nothing queued right now; the next push() resumes this via tryContinue()
    }
    writeInFlight_ = true;
    StartWrite(&batch_);
  }

  void OnCancel() override {
    stopped_.store(true, std::memory_order_relaxed);
    finishOnce();
  }

  void requestStop() {
    stopped_.store(true, std::memory_order_relaxed);
    finishOnce();
  }

  // Standard reactor-lifetime pattern: OnDone() is gRPC's own signal
  // that Finish() has fully completed and this call is over.
  void OnDone() override {
    registry_.deregisterSession(sessionId_);
    delete this;
  }

 private:
  // Drains everything currently queued (up to a cap) into batch_.
  // Returns false (batch_ left empty) if nothing is queued right now.
  // Caller must hold mutex_ — see tryContinue()/OnWriteDone() for why
  // that's what actually keeps this race-free: gatherBatch() itself
  // only touches queueMutex_/pending_, but it's only ever called while
  // mutex_ is held, which is what serializes "who gets to start the
  // next write" between a pushing thread and a gRPC callback thread.
  bool gatherBatch() {
    // ~1024 messages per batch cap, matching the relay's own
    // FLAGS_relay_max_batch_records default and reasoning —
    // comfortably under gRPC's 4MB message cap for counter-sized
    // payloads.
    constexpr std::size_t kMaxBatch = 1024;
    std::lock_guard<std::mutex> qlock(queueMutex_);
    if (pending_.empty()) {
      return false;
    }
    batch_.Clear();
    while (!pending_.empty() && static_cast<std::size_t>(batch_.payloads_size()) < kMaxBatch) {
      batch_.add_payloads(*pending_.front());
      pending_.pop_front();
    }
    return true;
  }

  // Finish() must be called exactly once, however stopping was
  // triggered (OnCancel(), requestStop() from a gateway shutdown, or
  // OnWriteDone() itself seeing a failed write) — this exchange is
  // what guarantees that regardless of which path gets there first.
  // Safe to call with a write still technically outstanding: gRPC
  // still delivers that write's own OnWriteDone internally before
  // OnDone() fires, in the right order, without this needing to wait
  // for it first (same finding the relay's own reactor rewrite made).
  void finishOnce() {
    if (!finishCalled_.exchange(true, std::memory_order_relaxed)) {
      Finish(::grpc::Status::OK);
    }
  }

  SessionRegistry& registry_;
  SessionId sessionId_ = 0;
  std::atomic<bool> stopped_{false};
  std::atomic<bool> finishCalled_{false};

  std::mutex mutex_;
  bool writeInFlight_ = false;                            // guarded by mutex_
  gateway::output::grpc_proto::OutputRecordBatch batch_;  // touched only by whichever side holds the baton

  std::mutex queueMutex_;
  std::deque<std::shared_ptr<std::string>> pending_;  // guarded by queueMutex_
};

void SessionRegistry::push(SessionId id, const std::shared_ptr<std::string>& message) {
  OutputSubscribeReactor* reactor = nullptr;
  {
    std::lock_guard<std::mutex> lock(registryMutex);
    const auto it = sessionToReactor.find(id);
    if (it == sessionToReactor.end()) {
      return;  // session not currently connected; a fanout delivery is best-effort
    }
    reactor = it->second;
  }
  reactor->enqueueAndContinue(message);
}

void SessionRegistry::closeAll() {
  std::vector<OutputSubscribeReactor*> reactors;
  {
    std::lock_guard<std::mutex> lock(registryMutex);
    reactors.reserve(sessionToReactor.size());
    for (auto& [id, reactor] : sessionToReactor) {
      reactors.push_back(reactor);
    }
  }
  for (OutputSubscribeReactor* reactor : reactors) {
    reactor->requestStop();
  }
}

class GenericOutputServiceImpl final : public gateway::output::grpc_proto::GenericOutputService::CallbackService {
 public:
  explicit GenericOutputServiceImpl(SessionRegistry& registry) : registry_(registry) {}

  ::grpc::ServerWriteReactor<gateway::output::grpc_proto::OutputRecordBatch>* Subscribe(
      ::grpc::CallbackServerContext* /*context*/,
      const gateway::output::grpc_proto::SubscribeRequest* request) override {
    return new OutputSubscribeReactor(registry_, request->topic());
  }

 private:
  SessionRegistry& registry_;
};

}  // namespace

struct GrpcOutputTransport::Impl {
  SessionRegistry registry;
  GenericOutputServiceImpl service{registry};
  std::unique_ptr<grpc::Server> server;
};

GrpcOutputTransport::GrpcOutputTransport() : impl_(std::make_unique<Impl>()) {}
GrpcOutputTransport::~GrpcOutputTransport() = default;

void GrpcOutputTransport::start(int listenPort) {
  grpc::reflection::InitProtoReflectionServerBuilderPlugin();
  grpc::ServerBuilder builder;
  builder.AddListeningPort("0.0.0.0:" + std::to_string(listenPort), grpc::InsecureServerCredentials());
  builder.RegisterService(&impl_->service);
  impl_->server = builder.BuildAndStart();
}

void GrpcOutputTransport::stop() {
  impl_->registry.closeAll();
  if (impl_->server) {
    impl_->server->Shutdown();
    impl_->server->Wait();
  }
}

void GrpcOutputTransport::toSession(SessionId owner, Bytes bytes) {
  auto message = std::make_shared<std::string>(reinterpret_cast<const char*>(bytes.data()), bytes.size());
  impl_->registry.push(owner, message);
}

void GrpcOutputTransport::broadcast(const std::string& topic, Bytes bytes) {
  auto message = std::make_shared<std::string>(reinterpret_cast<const char*>(bytes.data()), bytes.size());
  std::vector<SessionId> targets;
  {
    std::lock_guard<std::mutex> lock(impl_->registry.registryMutex);
    const auto it = impl_->registry.topicToSessions.find(topic);
    if (it == impl_->registry.topicToSessions.end()) {
      return;
    }
    targets.assign(it->second.begin(), it->second.end());
  }
  for (SessionId sessionId : targets) {
    impl_->registry.push(sessionId, message);
  }
}

}  // namespace sequencer
