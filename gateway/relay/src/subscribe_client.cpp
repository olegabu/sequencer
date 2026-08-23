#include <sequencer/relay/subscribe_client.hpp>

#include <brpc/channel.h>
#include <brpc/controller.h>
#include <brpc/stream.h>

#include "relay.pb.h"

#include <chrono>
#include <condition_variable>
#include <mutex>

namespace sequencer::relay {

struct RelaySubscribeClient::Impl : public brpc::StreamInputHandler {
  int on_received_messages(brpc::StreamId, butil::IOBuf* const messages[], size_t size) override {
    for (size_t i = 0; i < size; ++i) {
      const std::string encoded = messages[i]->to_string();
      Bytes rawRecordBytes(reinterpret_cast<const std::byte*>(encoded.data()),
                            reinterpret_cast<const std::byte*>(encoded.data()) + encoded.size());
      onRecord(std::move(rawRecordBytes));
    }
    return 0;
  }
  void on_idle_timeout(brpc::StreamId) override {}
  void on_closed(brpc::StreamId) override {
    std::lock_guard<std::mutex> lock(mutex);
    closed = true;
    closedCv.notify_all();
  }

  RecordCallback onRecord;
  brpc::Channel channel;
  brpc::Controller cntl;
  brpc::StreamId streamId = brpc::INVALID_STREAM_ID;
  bool ok = false;
  std::string errorMessage;
  std::mutex mutex;
  std::condition_variable closedCv;
  bool closed = false;
};

RelaySubscribeClient::RelaySubscribeClient(std::string relayEndpoint, std::uint64_t fromSequenceNumber,
                                            RecordCallback onRecord)
    : impl_(std::make_unique<Impl>()) {
  impl_->onRecord = std::move(onRecord);

  brpc::ChannelOptions channelOptions;
  channelOptions.timeout_ms = 2000;
  if (impl_->channel.Init(relayEndpoint.c_str(), &channelOptions) != 0) {
    impl_->errorMessage = "failed to initialize channel to " + relayEndpoint;
    return;
  }

  brpc::StreamOptions streamOptions;
  streamOptions.handler = impl_.get();
  if (brpc::StreamCreate(&impl_->streamId, impl_->cntl, &streamOptions) != 0) {
    impl_->errorMessage = "StreamCreate failed";
    return;
  }

  sequencer::gateway::relay::proto::RelayService_Stub stub(&impl_->channel);
  sequencer::gateway::relay::proto::RelaySubscribeRequest request;
  request.set_from_sequence_number(fromSequenceNumber);
  sequencer::gateway::relay::proto::RelaySubscribeResponse response;
  impl_->cntl.set_timeout_ms(2000);
  stub.Subscribe(&impl_->cntl, &request, &response, nullptr);
  if (impl_->cntl.Failed() || !response.error_message().empty()) {
    impl_->errorMessage = impl_->cntl.Failed() ? impl_->cntl.ErrorText() : response.error_message();
    return;
  }

  impl_->ok = true;
}

RelaySubscribeClient::~RelaySubscribeClient() {
  if (impl_->streamId != brpc::INVALID_STREAM_ID) {
    // Same StreamClose()/on_closed() gap documented in
    // gateway/output/README.md and its client-side mirror in
    // gateway/output/tests/collecting_stream_client.hpp: wait for
    // confirmed closure before this handler (impl_) is freed.
    brpc::StreamClose(impl_->streamId);
    std::unique_lock<std::mutex> lock(impl_->mutex);
    impl_->closedCv.wait_for(lock, std::chrono::seconds(5), [this] { return impl_->closed; });
  }
}

bool RelaySubscribeClient::ok() const { return impl_->ok; }
const std::string& RelaySubscribeClient::errorMessage() const { return impl_->errorMessage; }

}  // namespace sequencer::relay
