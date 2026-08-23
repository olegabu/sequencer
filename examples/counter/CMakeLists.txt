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

add_library(counter_codecs STATIC
  counter_input_codec.cpp
  counter_output_codec.cpp
)
add_library(counter::codecs ALIAS counter_codecs)

target_include_directories(counter_codecs PUBLIC
  "$<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>"
)
target_link_libraries(counter_codecs PUBLIC sequencer::gateway_input sequencer::gateway_output)
target_compile_features(counter_codecs PUBLIC cxx_std_20)

add_executable(counter_node_main node_main.cpp)
target_link_libraries(counter_node_main PRIVATE counter::state_machine sequencer::node)
set_target_properties(counter_node_main PROPERTIES OUTPUT_NAME counter_node)

add_executable(counter_replay_main replay_main.cpp)
target_link_libraries(counter_replay_main PRIVATE counter::state_machine sequencer::replay)
set_target_properties(counter_replay_main PROPERTIES OUTPUT_NAME counter_replay)

add_executable(counter_input_gateway_main input_gateway_main.cpp)
target_link_libraries(counter_input_gateway_main PRIVATE counter::codecs sequencer::gateway_input)
set_target_properties(counter_input_gateway_main PROPERTIES OUTPUT_NAME counter_input_gateway)

# specification.md §8.7: brpc has no native WebSocket support, so this
# is the one place in the whole repository that depends on something
# beyond brpc for transport — scoped to exactly this target, not the
# top-level CMakeLists.txt, since nothing else needs it.
find_package(boost_beast CONFIG REQUIRED)

add_library(counter_websocket_transport STATIC
  websocket_transport.cpp
)
add_library(counter::websocket_transport ALIAS counter_websocket_transport)
target_include_directories(counter_websocket_transport PUBLIC
  "$<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>"
)
target_link_libraries(counter_websocket_transport
  PUBLIC sequencer::gateway_output
  PRIVATE Boost::beast
)
target_compile_features(counter_websocket_transport PUBLIC cxx_std_20)

add_executable(counter_output_gateway_main output_gateway_main.cpp)
target_link_libraries(counter_output_gateway_main PRIVATE counter::codecs counter::websocket_transport sequencer::gateway_output)
set_target_properties(counter_output_gateway_main PROPERTIES OUTPUT_NAME counter_output_gateway)

add_executable(counter_load_generator_main load_generator_main.cpp)
target_link_libraries(counter_load_generator_main PRIVATE sequencer::gateway_input unofficial::brpc::brpc-static gflags::gflags_static glog::glog)
set_target_properties(counter_load_generator_main PROPERTIES OUTPUT_NAME counter_load_generator)

if(SEQUENCER_BUILD_TESTS)
  add_subdirectory(tests)
endif()
