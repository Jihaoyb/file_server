#pragma once

#include <string>

namespace nebulafs::distributed {

std::string CreatePlacementToken(const std::string& blob_id, const std::string& action,
                                 int ttl_seconds, const std::string& secret);
bool ValidatePlacementToken(const std::string& token, const std::string& blob_id,
                            const std::string& action, const std::string& secret);

}  // namespace nebulafs::distributed
