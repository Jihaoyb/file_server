#include "nebulafs/distributed/placement_token.h"

#include <algorithm>
#include <chrono>
#include <iomanip>
#include <sstream>
#include <string>
#include <vector>

#include <openssl/hmac.h>

namespace nebulafs::distributed {
namespace {

std::string HmacHex(const std::string& secret, const std::string& message) {
    unsigned char digest[EVP_MAX_MD_SIZE];
    unsigned int digest_len = 0;
    HMAC(EVP_sha256(), secret.data(), static_cast<int>(secret.size()),
         reinterpret_cast<const unsigned char*>(message.data()), message.size(), digest,
         &digest_len);
    std::ostringstream ss;
    ss << std::hex << std::setfill('0');
    for (unsigned int i = 0; i < digest_len; ++i) {
        ss << std::setw(2) << static_cast<int>(digest[i]);
    }
    return ss.str();
}

long long NowEpochSeconds() {
    return std::chrono::duration_cast<std::chrono::seconds>(
               std::chrono::system_clock::now().time_since_epoch())
        .count();
}

}  // namespace

std::string CreatePlacementToken(const std::string& blob_id, const std::string& action,
                                 int ttl_seconds, const std::string& secret) {
    const auto expires = NowEpochSeconds() + std::max(1, ttl_seconds);
    const auto message = blob_id + "|" + action + "|" + std::to_string(expires);
    const auto sig = HmacHex(secret, message);
    return std::to_string(expires) + ":" + sig;
}

bool ValidatePlacementToken(const std::string& token, const std::string& blob_id,
                            const std::string& action, const std::string& secret) {
    const auto pos = token.find(':');
    if (pos == std::string::npos) {
        return false;
    }
    const auto expires_text = token.substr(0, pos);
    const auto sig = token.substr(pos + 1);
    long long expires = 0;
    try {
        expires = std::stoll(expires_text);
    } catch (const std::exception&) {
        return false;
    }
    if (expires < NowEpochSeconds()) {
        return false;
    }
    const auto expected = HmacHex(secret, blob_id + "|" + action + "|" + expires_text);
    return expected == sig;
}

}  // namespace nebulafs::distributed
