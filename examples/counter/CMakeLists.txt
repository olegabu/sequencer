# examples/counter/ — specification.md §10: the one example in this
# repository. State machine, node_main (§15 item 3), replay_main (§15
# item 4), and now the codecs (§15 item 6). Gateway mains and the load
# generator land alongside the codecs in this same phase.

add_library(counter_state_machine STATIC
  counter_state_machine.cpp
)
add_library(counter::state_machine ALIAS counter_state_machine)

target_include_directories(counter_state_machine PUBLIC
  "$<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>"
)
target_link_libraries(counter_state_machine PUBLIC sequencer::node)
target_compile_features(counter_state_machine PUBLIC cxx_std_20)

# One library per side, not one "codecs" library for both. A combined
# target made every output-gateway binary link gateway_input too, and
# both chassis define a --listen_port gflag — process-global symbols,
# so the two collide the moment a binary pulls in both translation
# units ("multiple definition of fLI::FLAGS_listen_port"). Splitting
# them means an output gateway links the output side only, which is
# also just accurate about what it depends on.
add_library(counter_input_codec STATIC counter_input_codec.cpp)
add_library(counter::input_codec ALIAS counter_input_codec)
target_include_directories(counter_input_codec PUBLIC
  "$<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>"
)
target_link_libraries(counter_input_codec PUBLIC sequencer::gateway_input)
target_compile_features(counter_input_codec PUBLIC cxx_std_20)

add_library(counter_output_codec STATIC counter_output_codec.cpp)
add_library(counter::output_codec ALIAS counter_output_codec)
target_include_directories(counter_output_codec PUBLIC
  "$<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>"
)
target_link_libraries(counter_output_codec PUBLIC sequencer::gateway_output)
target_compile_features(counter_output_codec PUBLIC cxx_std_20)

# Convenience for anything that genuinely wants both sides (the codec
# unit test, the input gateway mains).
add_library(counter_codecs INTERFACE)
add_library(counter::codecs ALIAS counter_codecs)
target_link_libraries(counter_codecs INTERFACE counter::input_codec counter::output_codec)

add_executable(counter_node_main node_main.cpp)
target_link_libraries(counter_node_main PRIVATE counter::state_machine sequencer::node)
set_target_properties(counter_node_main PROPERTIES OUTPUT_NAME counter_node)

add_executable(counter_replay_main replay_main.cpp)
target_link_libraries(counter_replay_main PRIVATE counter::state_machine sequencer::replay)
set_target_properties(counter_replay_main PROPERTIES OUTPUT_NAME counter_replay)

add_executable(counter_input_gateway_main input_gateway_main.cpp)
target_link_libraries(counter_input_gateway_main PRIVATE counter::input_codec sequencer::gateway_input)
set_target_properties(counter_input_gateway_main PROPERTIES OUTPUT_NAME counter_input_gateway)

# specification.md §8.7's WebSocket OutputTransport (brpc has no native
# WebSocket support) now lives in gateway/output/ — see that
# component's CMakeLists.txt and README.md's "Topic routing" note for
# why nothing here was actually counter-specific.
#
# Named for its flavor (websocket_), like the brpc and gRPC ones below:
# this used to be the unmarked `output_gateway_main.cpp` /
# `counter_output_gateway` purely because it was the first flavor to
# exist, which left the one binary whose transport was hardest to guess
# from its name as the one that didn't say.
add_executable(counter_websocket_output_gateway_main websocket_output_gateway_main.cpp)
target_link_libraries(counter_websocket_output_gateway_main PRIVATE counter::output_codec sequencer::gateway_output_websocket sequencer::gateway_output gflags::gflags_static)
set_target_properties(counter_websocket_output_gateway_main PROPERTIES OUTPUT_NAME counter_websocket_output_gateway)

# The brpc flavor: the chassis's own built-in transport
# (BrpcOutputTransport), needing no override and so no extra transport
# library beyond the base sequencer::gateway_output — see
# brpc_output_gateway_main.cpp's own file comment.
add_executable(counter_brpc_output_gateway_main brpc_output_gateway_main.cpp)
target_link_libraries(counter_brpc_output_gateway_main PRIVATE counter::output_codec sequencer::gateway_output gflags::gflags_static)
set_target_properties(counter_brpc_output_gateway_main PROPERTIES OUTPUT_NAME counter_brpc_output_gateway)

# The combined flavor: one process serving any subset of the three
# transports, each on its own port (output_gateway_main.cpp's own file
# comment for why that is strictly less work than running the
# single-transport binaries side by side). Links all three transport
# libraries, which is exactly what the separate library targets in
# gateway/output/ exist to make optional for applications that don't.
add_executable(counter_output_gateway_main output_gateway_main.cpp)
target_link_libraries(counter_output_gateway_main PRIVATE counter::output_codec
                                                            sequencer::gateway_output
                                                            sequencer::gateway_output_grpc
                                                            sequencer::gateway_output_websocket
                                                            gflags::gflags_static)
set_target_properties(counter_output_gateway_main PROPERTIES OUTPUT_NAME counter_output_gateway)

add_executable(counter_load_generator_main load_generator_main.cpp)
# sequencer::node for node.pb.h: --node_addr submits straight to a
# node's own ProposeService, bypassing the input gateway.
target_link_libraries(counter_load_generator_main PRIVATE sequencer::gateway_input sequencer::node
                                                            sequencer::bench_load_generator
                                                            sequencer::bench_relay_observer
                                                            sequencer::bench_output_observer
                                                            unofficial::brpc::brpc-static gflags::gflags_static glog::glog)
set_target_properties(counter_load_generator_main PROPERTIES OUTPUT_NAME counter_load_generator)

# specification.md §8.7: real gRPC entry points, alongside the existing
# brpc/REST-via-curl input gateway and WebSocket-via-websocat output
# gateway — see examples/counter/README.md for the pattern each one
# demonstrates.
find_package(gRPC CONFIG REQUIRED)
# See gateway/output/CMakeLists.txt's identical block: vcpkg's grpc port
# embeds a duplicate of a few abseil translation units inside libgrpc.a,
# colliding with the standalone libabsl_status.a also on the link line.
set_property(TARGET gRPC::grpc++ APPEND PROPERTY
  INTERFACE_LINK_OPTIONS "-Wl,--allow-multiple-definition")

# Output: reuses the shared, generic sequencer::GrpcOutputTransport
# (gateway/output/) with the existing CounterOutputCodec, unchanged —
# "just like" the WebSocket output gateway above. No counter-specific
# proto or codegen needed for this half.
add_executable(counter_grpc_output_gateway_main grpc_output_gateway_main.cpp)
target_link_libraries(counter_grpc_output_gateway_main PRIVATE counter::output_codec sequencer::gateway_output_grpc sequencer::gateway_output gflags::gflags_static)
set_target_properties(counter_grpc_output_gateway_main PROPERTIES OUTPUT_NAME counter_grpc_output_gateway)

# Input: a fully-typed, counter-specific gRPC service (its own proto),
# not a retrofit of the byte-opaque RunInputGateway chassis — see
# proto/counter_input_grpc.proto's file comment for why.
protobuf_generate(
  LANGUAGE cpp
  OUT_VAR COUNTER_INPUT_GRPC_PROTO_SOURCES
  PROTOS proto/counter_input_grpc.proto
  IMPORT_DIRS proto
  PROTOC_OUT_DIR "${CMAKE_CURRENT_BINARY_DIR}/generated"
)
protobuf_generate(
  LANGUAGE grpc
  GENERATE_EXTENSIONS .grpc.pb.h .grpc.pb.cc
  OUT_VAR COUNTER_INPUT_GRPC_SERVICE_SOURCES
  PROTOS proto/counter_input_grpc.proto
  IMPORT_DIRS proto
  PROTOC_OUT_DIR "${CMAKE_CURRENT_BINARY_DIR}/generated"
  PLUGIN "protoc-gen-grpc=$<TARGET_FILE:gRPC::grpc_cpp_plugin>"
)

add_executable(counter_grpc_input_gateway_main
  grpc_input_gateway_main.cpp
  ${COUNTER_INPUT_GRPC_PROTO_SOURCES}
  ${COUNTER_INPUT_GRPC_SERVICE_SOURCES}
)
target_include_directories(counter_grpc_input_gateway_main PRIVATE
  "$<BUILD_INTERFACE:${CMAKE_CURRENT_BINARY_DIR}/generated>"
)
target_link_libraries(counter_grpc_input_gateway_main PRIVATE
  sequencer::gateway_input
  gRPC::grpc++
  gRPC::grpc++_reflection
  protobuf::libprotobuf
  gflags::gflags_static
  glog::glog
)
set_target_properties(counter_grpc_input_gateway_main PROPERTIES OUTPUT_NAME counter_grpc_input_gateway)

# find_package(... CONFIG) targets are scoped to the calling directory
# and its subdirectories, not the whole build — gateway/output/'s own
# find_package(boost_beast) doesn't reach this sibling directory tree.
# Still needed here directly: tests/end_to_end_test.cpp has its own
# small Beast-based WebSocket test client (deliberately not shared with
# gateway/output/tests/'s — see that file's comment for why).
find_package(boost_beast CONFIG REQUIRED)

if(SEQUENCER_BUILD_TESTS)
  add_subdirectory(tests)
endif()
