# bench/load_generator/ — a reusable open/closed-loop load-testing
# harness (see include/sequencer/bench/load_generator.hpp's own file
# comment for why this lives here rather than in sdk/cpp/). Header-only,
# but not added to the root CMakeLists.txt's shared find_package() set:
# brpc's bvar and hdr-histogram are needed only here, matching the same
# scoping this repository already uses for grpc (found locally in each
# of the few CMakeLists.txt that actually link it, not at the root).

find_package(unofficial-brpc CONFIG REQUIRED)
find_package(hdr_histogram CONFIG REQUIRED)

add_library(sequencer_bench_load_generator INTERFACE)
add_library(sequencer::bench_load_generator ALIAS sequencer_bench_load_generator)

target_include_directories(sequencer_bench_load_generator INTERFACE
  "$<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>"
)
target_link_libraries(sequencer_bench_load_generator INTERFACE
  unofficial::brpc::brpc-static
  $<IF:$<TARGET_EXISTS:hdr_histogram::hdr_histogram>,hdr_histogram::hdr_histogram,hdr_histogram::hdr_histogram_static>
)
target_compile_features(sequencer_bench_load_generator INTERFACE cxx_std_20)

# sequencer_bench_relay_observer — a separate, opt-in target (see
# include/sequencer/bench/relay_observer.hpp's own file comment): pulls
# in gRPC and gateway/relay's generated proto code, which an
# application not comparing against the relay path has no reason to
# link just for sequencer_bench_load_generator above.
find_package(gRPC CONFIG REQUIRED)
# See gateway/output/CMakeLists.txt's identical block: vcpkg's grpc port
# embeds a duplicate of a few abseil translation units inside libgrpc.a,
# colliding with the standalone libabsl_status.a also on the link line.
set_property(TARGET gRPC::grpc++ APPEND PROPERTY
  INTERFACE_LINK_OPTIONS "-Wl,--allow-multiple-definition")

add_library(sequencer_bench_relay_observer INTERFACE)
add_library(sequencer::bench_relay_observer ALIAS sequencer_bench_relay_observer)

target_include_directories(sequencer_bench_relay_observer INTERFACE
  "$<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>"
)
target_link_libraries(sequencer_bench_relay_observer INTERFACE
  sequencer::journal
  sequencer::relay_grpc_proto
  gRPC::grpc++
  $<IF:$<TARGET_EXISTS:hdr_histogram::hdr_histogram>,hdr_histogram::hdr_histogram,hdr_histogram::hdr_histogram_static>
)
target_compile_features(sequencer_bench_relay_observer INTERFACE cxx_std_20)

# sequencer_bench_output_observer — the output-gateway counterpart to
# sequencer_bench_relay_observer above (see include/sequencer/bench/
# sequence_correlator.hpp's own file comment): one target covering all
# three transport observers (brpc/gRPC/WebSocket), not three separate
# opt-in ones — unlike relay_observer's single-transport scoping, an
# app using this wants all three available behind one runtime flag
# (examples/counter/load_generator_main.cpp's --output_observer), so
# splitting the link surface further would help no one.
find_package(boost_beast CONFIG REQUIRED)

add_library(sequencer_bench_output_observer INTERFACE)
add_library(sequencer::bench_output_observer ALIAS sequencer_bench_output_observer)

target_include_directories(sequencer_bench_output_observer INTERFACE
  "$<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>"
)
target_link_libraries(sequencer_bench_output_observer INTERFACE
  sequencer::gateway_output
  sequencer::gateway_output_grpc
  gRPC::grpc++
  Boost::beast
  $<IF:$<TARGET_EXISTS:hdr_histogram::hdr_histogram>,hdr_histogram::hdr_histogram,hdr_histogram::hdr_histogram_static>
)
target_compile_features(sequencer_bench_output_observer INTERFACE cxx_std_20)
