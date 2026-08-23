# gateway/relay/ — specification.md §8.2, §9: the relay gateway. Reads
# a colocated replica's journal via local memory-map and re-serves
# Subscribe(fromSequenceNumber) over brpc's own Streaming RPC, byte-
# identical, to any number of remote subscribers. Needs no application
# knowledge (no InputCodec/OutputCodec), so — like evidence/'s signing
# gateway — this builds a complete, ready-to-run stock binary
# (`sequencer_relay`) directly in this repository, not a library an
# application links.

protobuf_generate(
  LANGUAGE cpp
  OUT_VAR SEQUENCER_RELAY_PROTO_SOURCES
  PROTOS proto/relay.proto
  IMPORT_DIRS proto
  PROTOC_OUT_DIR "${CMAKE_CURRENT_BINARY_DIR}/generated"
)

add_library(sequencer_relay_proto STATIC
  ${SEQUENCER_RELAY_PROTO_SOURCES}
)
add_library(sequencer::relay_proto ALIAS sequencer_relay_proto)

target_include_directories(sequencer_relay_proto
  PUBLIC
    # src/ (RelayGatewayImpl, RelaySession, RelayServer) is not part of
    # the public interface, but tests need it directly.
    "$<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/src>"
    "$<BUILD_INTERFACE:${CMAKE_CURRENT_BINARY_DIR}/generated>"
)
target_link_libraries(sequencer_relay_proto
  PUBLIC
    sequencer::journal
  PRIVATE
    unofficial::brpc::brpc-static
    protobuf::libprotobuf
)
target_compile_features(sequencer_relay_proto PUBLIC cxx_std_20)

# The reference Subscribe client (specification.md §8.2) — a small
# compiled library, Pimpl'd to keep brpc's headers out of the public
# one. Unlike gateway/input and gateway/output's chassis, this is not
# application-linked: a relay client needs no codec, since it hands
# back raw, uninterpreted journal record bytes.
add_library(sequencer_relay_client STATIC
  src/subscribe_client.cpp
)
add_library(sequencer::relay_client ALIAS sequencer_relay_client)

target_include_directories(sequencer_relay_client PUBLIC
  "$<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>"
)
target_link_libraries(sequencer_relay_client
  PUBLIC
    sequencer::journal
  PRIVATE
    sequencer::relay_proto
    unofficial::brpc::brpc-static
    protobuf::libprotobuf
)
target_compile_features(sequencer_relay_client PUBLIC cxx_std_20)

add_executable(sequencer_relay src/run_relay_gateway.cpp)
target_link_libraries(sequencer_relay PRIVATE
  sequencer::relay_proto
  unofficial::brpc::brpc-static
  gflags::gflags_static
  glog::glog
)

if(SEQUENCER_BUILD_TESTS)
  add_subdirectory(tests)
endif()
