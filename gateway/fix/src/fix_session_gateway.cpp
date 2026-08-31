#include <sequencer/fix/fix_session_gateway.hpp>

#include <atomic>
#include <chrono>
#include <csignal>
#include <thread>

#include <sequencer/fix/fix_input_transport.hpp>
#include <sequencer/fix/fix_output_transport.hpp>

#include "../../input/src/input_gateway_impl.hpp"
#include "../../output/src/output_gateway_impl.hpp"

namespace sequencer::fix {
namespace {

std::atomic<bool> gStopRequested{false};
void handleStopSignal(int /*signum*/) { gStopRequested.store(true, std::memory_order_relaxed); }

}  // namespace

int RunFixSessionGateway(SessionGatewayConfig config,
                          std::unique_ptr<sequencer::InputCodec> inputCodec,
                          std::unique_ptr<sequencer::OutputCodec> outputCodec) {
  // Clear any stop request left over from a previous call. The flag is
  // process-global because a signal handler is the only thing that can
  // set it, and without this a second gateway in one process would
  // inherit the first one's SIGTERM and exit before it started --
  // leaving its threads joinable and taking the process down with
  // "terminate called without an active exception".
  gStopRequested.store(false, std::memory_order_relaxed);

  // The input transport is built FIRST and held by raw pointer, because
  // both halves need it: the input chassis owns it as its transport,
  // and the output half consumes it as a SessionSource. That is what
  // makes this one session core rather than two (§8.12 "Shape") -- the
  // output side writes onto the very sockets the orders arrived on.
  FixInputConfig inputConfig;
  inputConfig.senderCompId = config.senderCompId;
  inputConfig.heartBtInt = config.heartBtInt;
  inputConfig.sequenceStoreDir = config.sequenceStoreDir.string();
  // shared_ptr, not unique_ptr: the transport factory below is stored
  // in a std::function, which requires a COPYABLE callable, and a
  // captured unique_ptr is move-only. The shared holder is released
  // into the chassis on the single call the factory receives.
  auto ownedInput = std::make_shared<std::unique_ptr<FixInputTransport>>(
      std::make_unique<FixInputTransport>(inputConfig));
  FixInputTransport* input = ownedInput->get();

  // The output half, reading the journal and delivering onto those
  // sessions.
  sequencer::gateway::output::detail::OutputGatewayConfig outputConfig;
  outputConfig.dataDir = config.dataDir;
  outputConfig.resumeFile = config.resumeFile;
  // The transport needs the codec for resends -- it re-runs it over a
  // journal record to reproduce exactly what was sent (§8.12 reason 1)
  // -- and the chassis needs to own it, so the raw pointer is taken
  // before the move. The chassis outlives the transport, so this
  // stays valid.
  sequencer::OutputCodec* codecForResends = outputCodec.get();
  auto ownedOutput = std::make_unique<FixOutputTransport>(*input);
  FixOutputTransport* output = ownedOutput.get();
  sequencer::gateway::output::detail::OutputGatewayImpl outputGateway(
      outputConfig, std::move(outputCodec),
      std::unique_ptr<sequencer::OutputTransport>(std::move(ownedOutput)),
      // No port of its own: the sockets belong to the input half.
      0);
  output->attachJournal(config.dataDir, *codecForResends);

  // The input half. Its transport factory hands over the instance built
  // above rather than constructing a new one.
  sequencer::gateway::input::detail::InputGatewayConfig inputGatewayConfig;
  inputGatewayConfig.nodeEndpoints = config.nodeEndpoints;
  inputGatewayConfig.listenPort = config.listenPort;
  sequencer::gateway::input::detail::InputGatewayImpl inputGateway(
      inputGatewayConfig, std::move(inputCodec), sequencer::acceptAllSignatures,
      [ownedInput]() {
        return std::unique_ptr<sequencer::InputTransport>(std::move(*ownedInput));
      });

  // Output first: a subscriber's reader must be draining the ring
  // before any order can be admitted, or the first execution report
  // would be published with nobody reading.
  outputGateway.start();
  inputGateway.start();

  std::signal(SIGINT, handleStopSignal);
  std::signal(SIGTERM, handleStopSignal);
  while (!gStopRequested.load(std::memory_order_relaxed)) {
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
  }

  // Input first on the way down: stop admitting orders before the
  // thing that delivers their consequences goes away.
  inputGateway.stop();
  outputGateway.stop();
  return 0;
}

}  // namespace sequencer::fix
