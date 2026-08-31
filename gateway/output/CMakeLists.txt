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
  src/brpc_output_transport.cpp
  src/run_output_gateway.cpp
  ${SEQUENCER_GATEWAY_OUTPUT_PROTO_SOURCES}
)
add_library(sequencer::gateway_output ALIAS sequencer_gateway_output)

# The application-facing INTERFACE an output codec is written against:
# OutputCodec is a pure-virtual class in a header, so implementing one
# needs no compiled code at all.
#
# It is separate from sequencer::gateway_output because that library
# also contains run_output_gateway.cpp, whose DEFINE_string(data_dir,
# ...) is a program-level gflag at namespace scope. node/ defines the
# same flag for its own binary, so any target that pulled in both
# libraries got two definitions of one symbol. That is exactly what
# examples/counter's codec unit test did -- it needs both codecs, so it
# transitively linked both gateways -- and it broke CI's linker
# (binutils 2.38 on ubuntu-22.04 rejects the duplicate; 2.34 silently
# merged it, which is why local builds kept passing).
add_library(sequencer_output_codec_api INTERFACE)
add_library(sequencer::output_codec_api ALIAS sequencer_output_codec_api)
target_include_directories(sequencer_output_codec_api INTERFACE
  "$<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>"
)
target_link_libraries(sequencer_output_codec_api INTERFACE sequencer::journal)
target_compile_features(sequencer_output_codec_api INTERFACE cxx_std_20)

target_include_directories(sequencer_gateway_output
  PUBLIC
    "$<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>"
    # src/ (OutputGatewayImpl, BrpcStreamFanout) is not part of the
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

# specification.md §8.7: a real, standard-gRPC-compatible
# OutputTransport — brpc does not implement genuine gRPC streaming (see
# proto/output_grpc.proto's file comment), so this uses the real gRPC
# C++ library instead. A separate target for the same reason as the
# WebSocket one above: an application that doesn't want gRPC never
# pulls in the gRPC C++ library just by linking the chassis.
find_package(gRPC CONFIG REQUIRED)
# vcpkg's grpc port embeds its own copy of a few abseil translation units
# (e.g. inproc_transport.cc.o duplicates absl::Status::kMovedFromString)
# inside libgrpc.a, which collides with the standalone libabsl_status.a
# also on the link line. Both definitions are byte-identical (same abseil
# version), so this is a packaging duplicate, not an ODR violation — safe
# to tell the linker to keep the first one it sees.
set_property(TARGET gRPC::grpc++ APPEND PROPERTY
  INTERFACE_LINK_OPTIONS "-Wl,--allow-multiple-definition")

protobuf_generate(
  LANGUAGE cpp
  OUT_VAR SEQUENCER_GATEWAY_OUTPUT_GRPC_PROTO_SOURCES
  PROTOS proto/output_grpc.proto
  IMPORT_DIRS proto
  PROTOC_OUT_DIR "${CMAKE_CURRENT_BINARY_DIR}/generated"
)
protobuf_generate(
  LANGUAGE grpc
  GENERATE_EXTENSIONS .grpc.pb.h .grpc.pb.cc
  OUT_VAR SEQUENCER_GATEWAY_OUTPUT_GRPC_SERVICE_SOURCES
  PROTOS proto/output_grpc.proto
  IMPORT_DIRS proto
  PROTOC_OUT_DIR "${CMAKE_CURRENT_BINARY_DIR}/generated"
  PLUGIN "protoc-gen-grpc=$<TARGET_FILE:gRPC::grpc_cpp_plugin>"
)

add_library(sequencer_gateway_output_grpc STATIC
  src/grpc_output_transport.cpp
  ${SEQUENCER_GATEWAY_OUTPUT_GRPC_PROTO_SOURCES}
  ${SEQUENCER_GATEWAY_OUTPUT_GRPC_SERVICE_SOURCES}
)
add_library(sequencer::gateway_output_grpc ALIAS sequencer_gateway_output_grpc)

target_include_directories(sequencer_gateway_output_grpc PUBLIC
  "$<BUILD_INTERFACE:${CMAKE_CURRENT_BINARY_DIR}/generated>"
)
target_link_libraries(sequencer_gateway_output_grpc
  PUBLIC
    sequencer::gateway_output
  PRIVATE
    gRPC::grpc++
    gRPC::grpc++_reflection
    protobuf::libprotobuf
)
target_compile_features(sequencer_gateway_output_grpc PUBLIC cxx_std_20)

if(SEQUENCER_BUILD_TESTS)
  add_subdirectory(tests)
endif()
