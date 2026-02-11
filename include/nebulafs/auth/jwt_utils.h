#pragma once

#include <string>
#include <vector>

namespace nebulafs::auth {

std::vector<unsigned char> Base64UrlDecode(const std::string& input);
std::string Base64UrlDecodeToString(const std::string& input);
std::vector<std::string> Split(const std::string& input, char delimiter);
std::string Trim(const std::string& input);

}  // namespace nebulafs::auth
