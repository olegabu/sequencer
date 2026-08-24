#include <sequencer/grpc_output_transport.hpp>

#include <grpcpp/ext/proto_server_reflection_plugin.h>
#include <grpcpp/grpcpp.h>

#include "output_grpc.grpc.pb.h"

#include <chrono>
#include <condition_variable>
#include <deque>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <vector>

// gRPC's synchronous server-streaming API runs each Subscribe() call on
// its own dedicated thread for the call's entire lifetime — unlike
// WebSocketOutputTransport's single-io-thread design, there is no
// shared-object thread-safety rule to work around here, since each
// session's grpc::ServerWriter is only ever touched from its own
// call's thread. What IS shared across threads is the per-session
// message queue broadcast()/toSession() push onto (from the output
// gateway's own tailing thread) and each Subscribe() call's thread
// drains — a plain mutex/condition_variable queue, not gRPC-specific
// machinery.

namespace sequencer {

namespace {

struct WriteQueue {
  std::mutex mutex;
  std::condition_variable cv;
  std::deque<std::shared_ptr<std::string>> queue;
  bool closed = false;
};

// Registry operations used by GenericOutputServiceImpl below — split
// out from GrpcOutputTransport::Impl so the service class can be fully
// defined before Impl (which owns one), while Impl's own definition
// can still see this type. A plain struct of the shared state, not
// GrpcOutputTransport::Impl itself, to sidestep the ordering
// chicken-and-egg between "the service needs Impl" and "Impl owns the
// service."
struct SessionRegistry {
  std::mutex registryMutex;
  std::unordered_map<SessionId, std::shared_ptr<WriteQueue>> sessionToQueue;
  std::unordered_map<std::string, std::unordered_set<SessionId>> topicToSessions;
  std::atomic<SessionId> nextSessionId{1};

  SessionId registerSession(const std::string& topic, const std::shared_ptr<WriteQueue>& queue) {
    std::lock_guard<std::mutex> lock(registryMutex);
    const SessionId id = nextSessionId.fetch_add(1, std::memory_order_relaxed);
    sessionToQueue[id] = queue;
    topicToSessions[topic].insert(id);
    return id;
  }

  void deregisterSession(SessionId id) {
    std::lock_guard<std::mutex> lock(registryMutex);
    sessionToQueue.erase(id);
    for (auto& [topic, ids] : topicToSessions) {
      ids.erase(id);
    }
  }

  void push(SessionId id, const std::shared_ptr<std::string>& message) {
    std::shared_ptr<WriteQueue> queue;
    {
      std::lock_guard<std::mutex> lock(registryMutex);
      const auto it = sessionToQueue.find(id);
      if (it == sessionToQueue.end()) {
        return;
      }
      queue = it->second;
    }
    {
      std::lock_guard<std::mutex> lock(queue->mutex);
      queue->queue.push_back(message);
    }
    queue->cv.notify_one();
  }

  // Wakes every open Subscribe() call's thread so it returns on its
  // own — called from stop(), belt and suspenders alongside
  // grpc::Server::Shutdown()'s own in-flight-call cancellation.
  void closeAll() {
    std::lock_guard<std::mutex> lock(registryMutex);
    for (auto& [id, queue] : sessionToQueue) {
      std::lock_guard<std::mutex> qlock(queue->mutex);
      queue->closed = true;
      queue->cv.notify_all();
    }
  }
};

class GenericOutputServiceImpl final : public gateway::output::grpc_proto::GenericOutputService::Service {
 public:
  explicit GenericOutputServiceImpl(SessionRegistry& registry) : registry_(registry) {}

  ::grpc::Status Subscribe(::grpc::ServerContext* context,
                            const gateway::output::grpc_proto::SubscribeRequest* request,
                            ::grpc::ServerWriter<gateway::output::grpc_proto::OutputRecord>* writer) override {
    auto queue = std::make_shared<WriteQueue>();
    const SessionId sessionId = registry_.registerSession(request->topic(), queue);

    while (!context->IsCancelled()) {
      std::shared_ptr<std::string> message;
      {
        std::unique_lock<std::mutex> lock(queue->mutex);
        // Bounded wait, not a plain wait(): needs to periodically
        // recheck context->IsCancelled() too — gRPC gives no direct
        // "wake me on cancel" primitive for a plain condition_variable.
        queue->cv.wait_for(lock, std::chrono::milliseconds(200),
                            [&] { return !queue->queue.empty() || queue->closed; });
        if (queue->queue.empty()) {
          if (queue->closed) {
            break;
          }
          continue;
        }
        message = queue->queue.front();
        queue->queue.pop_front();
      }
      gateway::output::grpc_proto::OutputRecord record;
      record.set_payload(*message);
      if (!writer->Write(record)) {
        break;  // client gone
      }
    }

    registry_.deregisterSession(sessionId);
    return ::grpc::Status::OK;
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
