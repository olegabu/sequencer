# gateway/output/ — specification.md §8.5: RunOutputGateway chassis +
# OutputCodec interface. An application links this and supplies a codec.

protobuf_generate(
  LANGUAGE cpp
  OUT_VAR SEQUENCER_GATEWAY_OUTPUT_PROTO_SOURCES
  PROTOS proto/output_gateway.proto
  IMPORT_DIRS proto
  PROTOC_OUT_DIR "${CMAKE_CURRENT_BINARY_DIR}/generated"
)

add_library(sequencer_gateway_output STATIC
  src/run_output_gateway.cpp
  ${SEQUENCER_GATEWAY_OUTPUT_PROTO_SOURCES}
)
add_library(sequencer::gateway_output ALIAS sequencer_gateway_output)

target_include_directories(sequencer_gateway_output
  PUBLIC
    "$<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>"
    # src/ (OutputGatewayImpl, StreamFanout) is not part of the
    # application-facing interface, but tests need it directly.
    "$<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/src>"
    "$<BUILD_INTERFACE:${CMAKE_CURRENT_BINARY_DIR}/generated>"
)

target_link_libraries(sequencer_gateway_output
  PUBLIC
    sequencer::journal
  PRIVATE
    unofficial::brpc::brpc-static
    protobuf::libprotobuf
    gflags::gflags_static
    glog::glog
)

target_compile_features(sequencer_gateway_output PUBLIC cxx_std_20)

# specification.md §8.7: brpc has no native WebSocket support, so a
# WebSocket OutputTransport needs a separate library — Boost.Beast, per
# §8.7's suggested choice. A separate target (not folded into
# sequencer_gateway_output itself) so an application that doesn't want
# WebSocket never pulls in Boost.Beast/Asio just by linking the
# chassis. Originally examples/counter's own file; moved here once it
# became clear nothing about it was actually counter-specific (see
# README.md's "Topic routing" note for the one thing that had to change
# to make that true) and every other application would otherwise have
# to reimplement the same ~230 lines of Beast plumbing.
find_package(boost_beast CONFIG REQUIRED)

add_library(sequencer_gateway_output_websocket STATIC
  src/websocket_output_transport.cpp
)
add_library(sequencer::gateway_output_websocket ALIAS sequencer_gateway_output_websocket)

target_link_libraries(sequencer_gateway_output_websocket
  PUBLIC
    sequencer::gateway_output
  PRIVATE
    Boost::beast
)
target_compile_features(sequencer_gateway_output_websocket PUBLIC cxx_std_20)

if(SEQUENCER_BUILD_TESTS)
  add_subdirectory(tests)
endif()
