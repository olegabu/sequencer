cmake_minimum_required(VERSION 3.24)

project(sequencer LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

option(SEQUENCER_BUILD_TESTS "Build sequencer unit tests" ON)
option(SEQUENCER_BUILD_BENCHMARKS "Build sequencer micro-benchmarks" ON)

if(CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
  add_compile_options(-Wall -Wextra -Wpedantic -Werror)
endif()

# Link-time optimization for Release builds of the compiled components
# (node/, gateway/, evidence/) — see specification.md §9.1. journal/ is
# header-only and unaffected by this setting.
include(CheckIPOSupported)
check_ipo_supported(RESULT _sequencer_ipo_supported OUTPUT _sequencer_ipo_error)
if(_sequencer_ipo_supported AND CMAKE_BUILD_TYPE STREQUAL "Release")
  set(CMAKE_INTERPROCEDURAL_OPTIMIZATION ON)
endif()

# Strip Release binaries at link time. Every C++ dependency here is
# linked statically (this repository's vcpkg triplet), and at least one
# of them — vcpkg's own build of brpc — embeds full DWARF debug info
# even in its "release" variant (verified directly: the release
# libbrpc.a archive itself is 650 MB, not something this repository's
# own build controls); with nothing to strip it back out, a plain
# Release build of counter_node came to 301 MB against raft-tests/
# braft's own equivalent (atomic_server, also fully statically linked)
# at 16 MB, stripped. -s only affects Release — Debug and tsan keep
# their symbols, needed for exactly the kind of gdb/backtrace debugging
# this repository's own README accounts document doing.
if(CMAKE_BUILD_TYPE STREQUAL "Release")
  add_link_options(-s)
endif()

if(SEQUENCER_BUILD_TESTS OR SEQUENCER_BUILD_BENCHMARKS)
  enable_testing()
endif()

if(SEQUENCER_BUILD_TESTS)
  find_package(GTest CONFIG REQUIRED)
endif()

if(SEQUENCER_BUILD_BENCHMARKS)
  find_package(benchmark CONFIG REQUIRED)
endif()

find_package(Protobuf CONFIG REQUIRED)
find_package(unofficial-brpc CONFIG REQUIRED)
find_package(unofficial-braft CONFIG REQUIRED)
find_package(gflags CONFIG REQUIRED)
find_package(glog CONFIG REQUIRED)
# specification.md §9.1: the cryptography library evidence/'s Merkle
# signing and sdk/'s header-only proof verification both need. Shared
# by more than one component, unlike Boost.Beast (scoped to
# examples/counter's CMakeLists.txt alone), so it's found here.
find_package(OpenSSL REQUIRED)

add_subdirectory(journal)
add_subdirectory(node)
add_subdirectory(tools/replay)
add_subdirectory(tools/dumper)
add_subdirectory(gateway/input)
add_subdirectory(gateway/output)
add_subdirectory(gateway/relay)
add_subdirectory(evidence)
add_subdirectory(sdk/cpp)
# After gateway/relay: sequencer_bench_relay_observer links its
# generated sequencer::relay_grpc_proto.
add_subdirectory(bench/load_generator)
add_subdirectory(examples/counter)
