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
target_link_libraries(counter_input_codec PUBLIC sequencer::input_codec_api)
target_compile_features(counter_input_codec PUBLIC cxx_std_20)

add_library(counter_output_codec STATIC counter_output_codec.cpp)
add_library(counter::output_codec ALIAS counter_output_codec)
target_include_directories(counter_output_codec PUBLIC
  "$<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>"
)
target_link_libraries(counter_output_codec PUBLIC sequencer::output_codec_api)
target_compile_features(counter_output_codec PUBLIC cxx_std_20)

# Convenience for anything that genuinely wants both sides -- in
# practice only the codec unit test, which exercises both directions.
#
# This is safe now only because the two codec libraries depend on the
# codec INTERFACES rather than on the compiled gateway libraries. When
# they linked the latter, this aggregate transitively pulled in both
# run_output_gateway.cpp and run_node.cpp, each defining a
# program-level DEFINE_string(data_dir, ...) -- one symbol, two
# definitions, which CI's linker rejected.
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
# ONE output gateway binary, serving any subset of the three transports,
# each on its own port and each enabled by its own --*_port flag
# (output_gateway_main.cpp's file comment).
#
# There were once three more beside it, one per transport. They were
# deleted: each was ~37 lines differing only in which transport it
# constructed, so every chassis change had to be made four times, and
# the combined binary already demonstrates all three. What they bought
# was a slimmer link -- the WebSocket-only binary was 8.0 MB against
# this one's 24.1 MB, since this links brpc AND gRPC AND Beast -- which
# is a real saving for a deployment but not the job of an example.
#
# An application that wants exactly one transport still gets that
# slimmer link: link only the transport library it needs (the separate
# targets in gateway/output/ exist for this) and construct that one
# transport, which is what each deleted main did. This example does not
# need to show three copies of it.
add_executable(counter_output_gateway_main output_gateway_main.cpp)
target_link_libraries(counter_output_gateway_main PRIVATE counter::output_codec
                                                            sequencer::gateway_output
                                                            sequencer::gateway_output_grpc
                                                            sequencer::gateway_output_websocket
                                                            gflags::gflags_static)
set_target_properties(counter_output_gateway_main PROPERTIES OUTPUT_NAME counter_output_gateway)

add_executable(counter_load_generator_main load_generator_main.cpp)
# sequencer::node_proto for node.pb.h: --node_addr submits straight to
# a node's own ProposeService, bypassing the input gateway. The proto
# target, not sequencer::node -- this wants the wire format, not a node
# harness (see node/CMakeLists.txt on why that distinction is load-bearing).
target_link_libraries(counter_load_generator_main PRIVATE sequencer::gateway_input sequencer::node_proto
                                                            sequencer::bench_fix_requester
                                                            counter::fix_codecs
                                                            sequencer::bench_load_generator
                                                            sequencer::bench_relay_observer
                                                            sequencer::bench_output_observer
                                                            unofficial::brpc::brpc-static gflags::gflags_static glog::glog
  sequencer::common
)
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

# The generated stubs as their own library rather than compiled
# straight into the gateway: tests/grpc_input_gateway_test.cpp needs
# the same client stubs to drive that gateway, and duplicating the
# generated sources into a second target compiles them twice and
# invites the two copies to drift.
add_library(counter_input_grpc_proto STATIC
  ${COUNTER_INPUT_GRPC_PROTO_SOURCES}
  ${COUNTER_INPUT_GRPC_SERVICE_SOURCES}
)
add_library(counter::input_grpc_proto ALIAS counter_input_grpc_proto)
target_include_directories(counter_input_grpc_proto PUBLIC
  "$<BUILD_INTERFACE:${CMAKE_CURRENT_BINARY_DIR}/generated>"
)
target_link_libraries(counter_input_grpc_proto PUBLIC
  gRPC::grpc++
  protobuf::libprotobuf
)

add_executable(counter_grpc_input_gateway_main
  grpc_input_gateway_main.cpp
)
target_link_libraries(counter_grpc_input_gateway_main PRIVATE
  counter::input_grpc_proto
  sequencer::gateway_input
  gRPC::grpc++
  gRPC::grpc++_reflection
  protobuf::libprotobuf
  gflags::gflags_static
  glog::glog
  sequencer::common
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

# specification.md §10, §8.12: the counter over FIX. One binary, one
# port, order entry and execution reports on the same session.
add_library(counter_fix_codecs STATIC counter_fix_codecs.cpp)
add_library(counter::fix_codecs ALIAS counter_fix_codecs)
target_include_directories(counter_fix_codecs PUBLIC
  "$<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>"
)
target_link_libraries(counter_fix_codecs PUBLIC
  sequencer::input_codec_api
  sequencer::output_codec_api
  sequencer::fix_session
)
target_compile_features(counter_fix_codecs PUBLIC cxx_std_20)

add_executable(counter_quickfix_gateway_main quickfix_gateway_main.cpp)
set_target_properties(counter_quickfix_gateway_main PROPERTIES OUTPUT_NAME counter_quickfix_gateway)
target_link_libraries(counter_quickfix_gateway_main PRIVATE
  counter::fix_codecs
  sequencer::quickfix_gateway
  gflags::gflags
  glog::glog
  sequencer::common
)

add_executable(counter_fix_gateway_main fix_gateway_main.cpp)
target_link_libraries(counter_fix_gateway_main PRIVATE counter::fix_codecs
                                                        sequencer::fix_gateway
                                                        gflags::gflags_static glog::glog
  sequencer::common
)
set_target_properties(counter_fix_gateway_main PROPERTIES OUTPUT_NAME counter_fix_gateway)
