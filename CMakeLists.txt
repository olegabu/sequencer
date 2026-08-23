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

if(SEQUENCER_BUILD_TESTS OR SEQUENCER_BUILD_BENCHMARKS)
  enable_testing()
endif()

if(SEQUENCER_BUILD_TESTS)
  find_package(GTest CONFIG REQUIRED)
endif()

if(SEQUENCER_BUILD_BENCHMARKS)
  find_package(benchmark CONFIG REQUIRED)
endif()

add_subdirectory(journal)
