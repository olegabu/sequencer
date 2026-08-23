# sdk/cpp/ — specification.md §9: "the reference client library, per
# language: propose, verify proofs, reconcile against the journal, raise
# the proof-timeout alarm." Entirely header-only: every piece here needs
# only journal/ (header-only) and evidence/ (header-only) plus OpenSSL
# — see client_signer.hpp's and propose_client.hpp's file comments for
# why this stays off of gateway/ and node/ (specification.md §9's
# dependency arrows), which is what keeps the whole library this light.

add_library(sequencer_sdk INTERFACE)
add_library(sequencer::sdk ALIAS sequencer_sdk)

target_include_directories(sequencer_sdk INTERFACE
  "$<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>"
)
target_link_libraries(sequencer_sdk INTERFACE
  sequencer::journal
  sequencer::evidence
)
target_compile_features(sequencer_sdk INTERFACE cxx_std_20)

if(SEQUENCER_BUILD_TESTS)
  add_subdirectory(tests)
endif()
