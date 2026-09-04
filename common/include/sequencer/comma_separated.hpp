#pragma once

// Splitting a comma-separated flag value, which four binaries were each
// doing with their own byte-identical copy of this function
// (counter_fix_gateway, counter_quickfix_gateway, counter_load_generator
// and RunInputGateway -- the only difference between them was whether
// the parameter was called `value` or `s`).

#include <sstream>
#include <string>
#include <vector>

namespace sequencer {

// Splits on ',' and drops empty fields, so "a,,b," yields {"a","b"}.
// Every caller passes a gflags string where a trailing or doubled comma
// is a typo rather than a request for an empty endpoint.
inline std::vector<std::string> splitCommaSeparated(const std::string& value) {
  std::vector<std::string> parts;
  std::stringstream stream(value);
  std::string part;
  while (std::getline(stream, part, ',')) {
    if (!part.empty()) {
      parts.push_back(part);
    }
  }
  return parts;
}

}  // namespace sequencer
