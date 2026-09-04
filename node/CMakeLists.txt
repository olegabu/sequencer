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

# The generated Propose wire types on their own, separate from the
# harness that serves them.
#
# Several things want node.pb.h WITHOUT wanting a node: gateway/input's
# NodeProposer speaks this wire format, and the counter load generator
# submits straight to a node's ProposeService. Linking all of
# sequencer::node for that also links src/run_node.cpp, whose
# DEFINE_string(data_dir, ...) is a program-level gflag at namespace
# scope -- and gateway/output and evidence/ define a flag of the same
# name for their own binaries. Any target that reached two of them got
# one symbol with two definitions.
#
# That is not theoretical: it is why CI was red from 2026-08-26 to
# 2026-08-31. It never reproduced locally because binutils 2.34 merges
# such duplicates silently while 2.38, on CI's ubuntu-22.04, rejects
# them -- so a green local build was never evidence about CI.
add_library(sequencer_node_proto STATIC ${SEQUENCER_NODE_PROTO_SOURCES})
add_library(sequencer::node_proto ALIAS sequencer_node_proto)
target_include_directories(sequencer_node_proto PUBLIC
  "$<BUILD_INTERFACE:${CMAKE_CURRENT_BINARY_DIR}/generated>"
)
target_link_libraries(sequencer_node_proto PUBLIC protobuf::libprotobuf)
target_compile_features(sequencer_node_proto PUBLIC cxx_std_20)

add_library(sequencer_node STATIC
  src/run_node.cpp
  src/node_impl.cpp
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
    sequencer::node_proto
  PRIVATE
    unofficial::braft::braft-static
    unofficial::brpc::brpc-static
    protobuf::libprotobuf
    gflags::gflags_static
    sequencer::common
)

target_compile_features(sequencer_node PUBLIC cxx_std_20)

if(SEQUENCER_BUILD_TESTS)
  add_subdirectory(tests)
endif()
