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

if(SEQUENCER_BUILD_TESTS)
  add_subdirectory(tests)
endif()
