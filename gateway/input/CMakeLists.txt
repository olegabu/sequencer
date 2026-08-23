# gateway/input/ — specification.md §8.5: RunInputGateway chassis +
# InputCodec interface. An application links this and supplies a codec.

protobuf_generate(
  LANGUAGE cpp
  OUT_VAR SEQUENCER_GATEWAY_INPUT_PROTO_SOURCES
  PROTOS proto/input_gateway.proto
  IMPORT_DIRS proto
  PROTOC_OUT_DIR "${CMAKE_CURRENT_BINARY_DIR}/generated"
)

add_library(sequencer_gateway_input STATIC
  src/run_input_gateway.cpp
  ${SEQUENCER_GATEWAY_INPUT_PROTO_SOURCES}
)
add_library(sequencer::gateway_input ALIAS sequencer_gateway_input)

target_include_directories(sequencer_gateway_input
  PUBLIC
    "$<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>"
    # src/ (InputGatewayImpl, NodeProposer) is not part of the
    # application-facing interface, but tests need it directly.
    "$<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/src>"
    "$<BUILD_INTERFACE:${CMAKE_CURRENT_BINARY_DIR}/generated>"
)

target_link_libraries(sequencer_gateway_input
  PUBLIC
    sequencer::journal
    # Reuses node/'s already-generated Propose proto types (node.pb.h)
    # rather than duplicating that wire format in a second .proto —
    # see node_proposer.hpp.
    sequencer::node
  PRIVATE
    unofficial::brpc::brpc-static
    protobuf::libprotobuf
    gflags::gflags_static
    glog::glog
)

target_compile_features(sequencer_gateway_input PUBLIC cxx_std_20)

if(SEQUENCER_BUILD_TESTS)
  add_subdirectory(tests)
endif()
