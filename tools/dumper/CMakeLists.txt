# tools/dumper/ — a genuinely standalone binary (specification.md §9):
# no StateMachine, no application dependency, unlike tools/replay.

add_executable(dumper main.cpp)
target_link_libraries(dumper PRIVATE sequencer::journal gflags::gflags_static glog::glog)
