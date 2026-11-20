#pragma once

#include <array>
#include <charconv>
#include <cstdint>
#include <optional>
#include <string>
#include <string_view>

#include <openssl/crypto.h>
#include <openssl/hmac.h>

namespace nebulafs::http::pilot_download {

inline std::string Sign(std::string_view payload, const std::string& secret) {
    const auto message = "nebulafs:pilot-download:v1:" + std::string(payload);
    std::array<unsigned char, EVP_MAX_MD_SIZE> digest{};
    unsigned int size = 0;
    if (!HMAC(EVP_sha256(), secret.data(), static_cast<int>(secret.size()),
              reinterpret_cast<const unsigned char*>(message.data()), message.size(),
              digest.data(), &size)) {
        return {};
    }
    constexpr std::string_view digits = "0123456789abcdef";
    std::string result;
    result.reserve(size * 2);
    for (unsigned int i = 0; i < size; ++i) {
        result.push_back(digits[digest[i] >> 4]);
        result.push_back(digits[digest[i] & 0x0f]);
    }
    return result;
}

inline std::string Create(std::string_view upload_id, std::int64_t expiry,
                          const std::string& secret) {
    const auto payload = std::string(upload_id) + "." + std::to_string(expiry);
    const auto signature = Sign(payload, secret);
    return signature.empty() ? std::string{} : payload + "." + signature;
}

inline std::optional<std::string> Verify(std::string_view token, std::int64_t now,
                                          const std::string& secret) {
    constexpr std::size_t kSignatureLength = 64;
    if (token.size() <= kSignatureLength + 2 || token.size() > 160) {
        return std::nullopt;
    }
    const auto signature_start = token.size() - kSignatureLength;
    if (token[signature_start - 1] != '.') {
        return std::nullopt;
    }
    const auto payload = token.substr(0, signature_start - 1);
    const auto separator = payload.find('.');
    if (separator == std::string_view::npos || separator == 0) {
        return std::nullopt;
    }
    const auto expiry = payload.substr(separator + 1);
    std::int64_t expires_at = 0;
    const auto end = expiry.data() + expiry.size();
    const auto parsed = std::from_chars(expiry.data(), end, expires_at);
    if (parsed.ec != std::errc{} || parsed.ptr != end || expires_at <= now) {
        return std::nullopt;
    }
    const auto expected = Sign(payload, secret);
    if (expected.size() != kSignatureLength ||
        CRYPTO_memcmp(expected.data(), token.data() + signature_start, kSignatureLength) != 0) {
        return std::nullopt;
    }
    return std::string(payload.substr(0, separator));
}

}  // namespace nebulafs::http::pilot_download
