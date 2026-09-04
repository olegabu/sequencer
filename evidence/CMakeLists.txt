# evidence/ — specification.md §7: fixed block-cutting, the Merkle
# builder, and the signing gateway. block.hpp and merkle.hpp are
# header-only (§9.1: needs only a cryptography library, not
# braft/brpc/protobuf) so sdk/'s header-only proof-verification slice
# can include them directly without linking anything heavier.

add_library(sequencer_evidence INTERFACE)
add_library(sequencer::evidence ALIAS sequencer_evidence)

target_include_directories(sequencer_evidence INTERFACE
  "$<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>"
)
target_link_libraries(sequencer_evidence INTERFACE
  sequencer::journal
  OpenSSL::Crypto
  sequencer::common
)
target_compile_features(sequencer_evidence INTERFACE cxx_std_20)

# The signing gateway (specification.md §8.4): colocated journal
# tailing, block-cutting, signing, and the EvidenceService RPC that
# serves signed roots and inclusion proofs. Needs no application
# knowledge (no InputCodec/OutputCodec), so — like gateway/relay's
# sequencer_relay — this is a ready-to-run stock binary built directly
# in this repository, not a library an application links.
protobuf_generate(
  LANGUAGE cpp
  OUT_VAR SEQUENCER_EVIDENCE_PROTO_SOURCES
  PROTOS proto/evidence.proto
  IMPORT_DIRS proto
  PROTOC_OUT_DIR "${CMAKE_CURRENT_BINARY_DIR}/generated"
)

add_library(sequencer_evidence_proto STATIC
  ${SEQUENCER_EVIDENCE_PROTO_SOURCES}
)
add_library(sequencer::evidence_proto ALIAS sequencer_evidence_proto)

target_include_directories(sequencer_evidence_proto
  PUBLIC
    # src/ (SigningGatewayImpl, EvidenceServer) is not part of the
    # public interface, but tests need it directly.
    "$<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/src>"
    "$<BUILD_INTERFACE:${CMAKE_CURRENT_BINARY_DIR}/generated>"
)
target_link_libraries(sequencer_evidence_proto
  PUBLIC
    sequencer::evidence
  PRIVATE
    unofficial::brpc::brpc-static
    protobuf::libprotobuf
)
target_compile_features(sequencer_evidence_proto PUBLIC cxx_std_20)

add_executable(sequencer_signing_gateway src/run_signing_gateway.cpp)
target_link_libraries(sequencer_signing_gateway PRIVATE
  sequencer::evidence_proto
  unofficial::brpc::brpc-static
  gflags::gflags_static
  glog::glog
)

if(SEQUENCER_BUILD_TESTS)
  add_subdirectory(tests)
endif()
