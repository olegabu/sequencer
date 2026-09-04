# gateway/fix/ — the FIX session gateway (specification.md §8.10–§8.12).
# See README.md for why this directory owns a session layer instead of
# using a FIX engine; that question is asked often enough to deserve an
# answer beside the code.

# hffix is header-only and comes from the pinned vcpkg baseline
# (7e21420f, hffix 1.4.1) -- verified present rather than assumed, per
# specification.md §8.12's "via its vcpkg port if present in the pinned
# baseline, otherwise vendored".
find_path(HFFIX_INCLUDE_DIR NAMES hffix.hpp REQUIRED)

add_library(sequencer_fix_session STATIC
  session/src/fix_session.cpp
)
add_library(sequencer::fix_session ALIAS sequencer_fix_session)

target_include_directories(sequencer_fix_session
  PUBLIC
    "$<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/session/include>"
    ${HFFIX_INCLUDE_DIR}
)
target_compile_features(sequencer_fix_session PUBLIC cxx_std_20)

# The session directory's HEADERS, with none of its compiled code.
#
# session_source.hpp defines the vocabulary both FIX gateways share --
# SessionSource, and the SubscriptionAction a MarketDataRequest carries
# -- and gateway/quickfix/ needs exactly that and nothing else: it runs
# QuickFIX's own session layer, so linking sequencer_fix_session would
# pull in a FixSession implementation it never calls.
#
# Same reasoning as sequencer::output_codec_api next door, and the same
# discipline: depend on the narrowest target that carries the
# declarations you need, so a library's object files travel only to the
# binaries that actually want them.
add_library(sequencer_fix_session_api INTERFACE)
add_library(sequencer::fix_session_api ALIAS sequencer_fix_session_api)
target_include_directories(sequencer_fix_session_api
  INTERFACE "$<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/session/include>")
target_compile_features(sequencer_fix_session_api INTERFACE cxx_std_20)

find_package(Boost REQUIRED COMPONENTS system)

# The FIX input transport: the session core plus Asio sockets, as an
# InputTransport the generic chassis can serve (specification.md §8.10).
add_library(sequencer_fix_input STATIC
  input/src/fix_input_transport.cpp
)
add_library(sequencer::fix_input ALIAS sequencer_fix_input)

target_include_directories(sequencer_fix_input PUBLIC
  "$<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/input/include>"
)
target_link_libraries(sequencer_fix_input
  PUBLIC
    sequencer::fix_session
    sequencer::gateway_input
  PRIVATE
    Boost::system
)
target_compile_features(sequencer_fix_input PUBLIC cxx_std_20)

# The FIX output transport: delivery onto live FIX sessions, sharing a
# session core with the input side in the order-entry shape.
add_library(sequencer_fix_output STATIC
  output/src/fix_output_transport.cpp
)
add_library(sequencer::fix_output ALIAS sequencer_fix_output)

target_include_directories(sequencer_fix_output PUBLIC
  "$<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/output/include>"
)
target_link_libraries(sequencer_fix_output
  PUBLIC
    sequencer::fix_session
    sequencer::gateway_output
)
target_compile_features(sequencer_fix_output PUBLIC cxx_std_20)

# The order-entry session gateway: both halves over one session core.
add_library(sequencer_fix_gateway STATIC
  src/fix_session_gateway.cpp
)
add_library(sequencer::fix_gateway ALIAS sequencer_fix_gateway)

target_include_directories(sequencer_fix_gateway PUBLIC
  "$<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>"
)
target_link_libraries(sequencer_fix_gateway
  PUBLIC
    sequencer::fix_input
    sequencer::fix_output
    sequencer::common
)
target_compile_features(sequencer_fix_gateway PUBLIC cxx_std_20)

if(SEQUENCER_BUILD_TESTS)
  add_subdirectory(tests)
endif()
