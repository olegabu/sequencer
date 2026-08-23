# node/ — the harness: RunNode, the apply thread, sequence-number
# minting, deferred acknowledgement (specification.md §5). Depends on
# journal/ publicly and, privately, on braft/brpc via src/raft/ — no
# braft type appears in node/include/ (specification.md §9).

protobuf_generate(
  LANGUAGE cpp
  OUT_VAR SEQUENCER_NODE_PROTO_SOURCES
  PROTOS proto/node.proto
  IMPORT_DIRS proto
  PROTOC_OUT_DIR "${CMAKE_CURRENT_BINARY_DIR}/generated"
)

add_library(sequencer_node STATIC
  src/run_node.cpp
  src/node_impl.cpp
  ${SEQUENCER_NODE_PROTO_SOURCES}
)
add_library(sequencer::node ALIAS sequencer_node)

target_include_directories(sequencer_node
  PUBLIC
    "$<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>"
    # node/src/ (the ring, the apply loop, and the private raft/
    # adapter) is not part of the *application-facing* interface, but
    # tests need to reach it directly to exercise it without a full
    # braft/brpc round trip — and anything under src/ that reaches the
    # generated protobuf header needs that path too.
    "$<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/src>"
    "$<BUILD_INTERFACE:${CMAKE_CURRENT_BINARY_DIR}/generated>"
)

target_link_libraries(sequencer_node
  PUBLIC
    sequencer::journal
  PRIVATE
    unofficial::braft::braft-static
    unofficial::brpc::brpc-static
    protobuf::libprotobuf
    gflags::gflags_static
)

target_compile_features(sequencer_node PUBLIC cxx_std_20)

if(SEQUENCER_BUILD_TESTS)
  add_subdirectory(tests)
endif()
