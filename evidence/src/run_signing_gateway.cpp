// The signing gateway's standalone binary (specification.md §8.4).
// Unlike RunInputGateway/RunOutputGateway, this needs no application
// codec — a signing gateway only ever reads the journal, cuts blocks,
// and signs — so, like the relay gateway (specification.md §9's
// repository layout), it ships as a ready-to-run stock binary rather
// than a library an application links.

#include <gflags/gflags.h>
#include <glog/logging.h>

#include <atomic>
#include <chrono>
#include <csignal>
#include <cstdint>
#include <stdexcept>
#include <thread>

#include <sequencer/stop_signal.hpp>

#include "evidence_server.hpp"
#include "signing_gateway_impl.hpp"

DEFINE_string(data_dir, "", "A node's journal directory to sign over, colocated (required)");
DEFINE_string(private_key_hex, "",
              "64 hex characters: this signing key's raw 32-byte Ed25519 seed (required). "
              "Every instance signing the same journal MUST use the same key for their roots to "
              "agree byte-for-byte (specification.md §8.4: \"run at least two instances\").");
DEFINE_int32(listen_port, 0, "This gateway's own client-facing port (required)");

namespace sequencer::evidence {
namespace {


int hexDigit(char c) {
  if (c >= '0' && c <= '9') return c - '0';
  if (c >= 'a' && c <= 'f') return c - 'a' + 10;
  if (c >= 'A' && c <= 'F') return c - 'A' + 10;
  return -1;
}

Ed25519PrivateKey parsePrivateKeyHex(const std::string& hex) {
  if (hex.size() != 64) {
    throw std::invalid_argument("--private_key_hex must be exactly 64 hex characters (32 bytes)");
  }
  Ed25519PrivateKey key{};
  for (std::size_t i = 0; i < key.size(); ++i) {
    const int hi = hexDigit(hex[2 * i]);
    const int lo = hexDigit(hex[2 * i + 1]);
    if (hi < 0 || lo < 0) {
      throw std::invalid_argument("--private_key_hex contains a non-hex character");
    }
    key[i] = static_cast<std::byte>((hi << 4) | lo);
  }
  return key;
}

}  // namespace
}  // namespace sequencer::evidence

int main(int argc, char** argv) {
  google::InitGoogleLogging(argv[0]);
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  if (FLAGS_data_dir.empty() || FLAGS_private_key_hex.empty() || FLAGS_listen_port == 0) {
    LOG(ERROR) << "RunSigningGateway: --data_dir, --private_key_hex, and --listen_port are required";
    return 1;
  }

  sequencer::evidence::Ed25519PrivateKey privateKey;
  try {
    privateKey = sequencer::evidence::parsePrivateKeyHex(FLAGS_private_key_hex);
  } catch (const std::exception& e) {
    LOG(ERROR) << "RunSigningGateway: " << e.what();
    return 1;
  }

  sequencer::evidence::detail::SigningGatewayConfig config;
  config.dataDir = FLAGS_data_dir;
  config.privateKey = privateKey;
  config.listenPort = FLAGS_listen_port;

  sequencer::evidence::detail::SigningGatewayImpl gateway(std::move(config));
  gateway.start();
  sequencer::evidence::detail::EvidenceServer server(gateway, FLAGS_listen_port);
  LOG(INFO) << "signing gateway started: listen_port=" << FLAGS_listen_port << " data_dir=" << FLAGS_data_dir;

  sequencer::waitForStopSignal();

  LOG(INFO) << "signing gateway stopping";
  server.stop();
  gateway.stop();
  return 0;
}
