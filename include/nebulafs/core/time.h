#pragma once

#include <string>

namespace nebulafs::core {

/// @brief Returns the current time formatted as ISO8601 UTC.
std::string NowIso8601();
/// @brief Returns a UTC ISO8601 timestamp offset from now by delta seconds.
std::string NowIso8601WithOffsetSeconds(int delta_seconds);

}  // namespace nebulafs::core
