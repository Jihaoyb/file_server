#include "nebulafs/auth/jwt_utils.h"

#include <algorithm>
#include <cctype>

#include <openssl/evp.h>

namespace nebulafs::auth {

std::vector<unsigned char> Base64UrlDecode(const std::string& input) {
    // Normalize base64url to base64 and pad for EVP_DecodeBlock.
    std::string padded = input;
    std::replace(padded.begin(), padded.end(), '-', '+');
    std::replace(padded.begin(), padded.end(), '_', '/');
    while (padded.size() % 4 != 0) {
        padded.push_back('=');
    }

    std::vector<unsigned char> output((padded.size() / 4) * 3);
    int out_len = EVP_DecodeBlock(output.data(),
                                  reinterpret_cast<const unsigned char*>(padded.data()),
                                  static_cast<int>(padded.size()));
    if (out_len < 0) {
        return {};
    }
    int padding = 0;
    if (!padded.empty() && padded.back() == '=') {
        padding++;
        if (padded.size() > 1 && padded[padded.size() - 2] == '=') {
            padding++;
        }
    }
    output.resize(static_cast<size_t>(out_len - padding));
    return output;
}

std::string Base64UrlDecodeToString(const std::string& input) {
    auto decoded = Base64UrlDecode(input);
    return std::string(reinterpret_cast<const char*>(decoded.data()), decoded.size());
}

std::vector<std::string> Split(const std::string& input, char delimiter) {
    // Simple splitter without trimming; callers can Trim() if needed.
    std::vector<std::string> parts;
    std::string current;
    for (char c : input) {
        if (c == delimiter) {
            parts.push_back(current);
            current.clear();
        } else {
            current.push_back(c);
        }
    }
    parts.push_back(current);
    return parts;
}

std::string Trim(const std::string& input) {
    // Whitespace trim for header parsing.
    auto start = input.begin();
    while (start != input.end() && std::isspace(static_cast<unsigned char>(*start))) {
        ++start;
    }
    auto end = input.end();
    while (end != start && std::isspace(static_cast<unsigned char>(*(end - 1)))) {
        --end;
    }
    return std::string(start, end);
}

}  // namespace nebulafs::auth
