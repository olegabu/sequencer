# tools/replay/ — specification.md §11: determinism certification.
# A library, not a standalone binary: it needs a concrete StateMachine
# to replay against (§9), so an application links it and exposes its
# own replay executable — examples/counter/replay_main.cpp does this.

add_library(sequencer_replay STATIC
  src/replay_check.cpp
  src/run_replay_check.cpp
)
add_library(sequencer::replay ALIAS sequencer_replay)

target_include_directories(sequencer_replay
  PUBLIC
    "$<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>"
    # src/ (replay_check.hpp) is not part of the application-facing
    # interface, but tests need it to drive replay directly without a
    # process or gflags.
    "$<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/src>"
)

target_link_libraries(sequencer_replay
  PUBLIC
    sequencer::journal
  PRIVATE
    gflags::gflags_static
    glog::glog
    sequencer::common
)

target_compile_features(sequencer_replay PUBLIC cxx_std_20)

if(SEQUENCER_BUILD_TESTS)
  add_subdirectory(tests)
endif()
