#pragma once

#include <string>

namespace nebulafs::core {

/// @brief Generate a unique request ID for correlation.
std::string GenerateRequestId();

}  // namespace nebulafs::core
