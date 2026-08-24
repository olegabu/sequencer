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
