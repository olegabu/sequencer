#pragma once

#include <cstddef>
#include <span>

namespace sequencer {

// Pointer + length, zero-copy. See specification.md §4: the state
// machine's apply() and the journal's records are both expressed in
// terms of this type. Defined here, in journal/ (the component with no
// dependencies of its own — specification.md §9), so every other
// component can share one definition without a dependency cycle.
using Payload = std::span<const std::byte>;

}  // namespace sequencer
