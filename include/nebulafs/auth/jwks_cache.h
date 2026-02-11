#pragma once

#include <chrono>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>

#include <openssl/evp.h>

#include "nebulafs/core/result.h"

namespace nebulafs::auth {

class JwksCache {
public:
    using KeyPtr = std::shared_ptr<EVP_PKEY>;

    JwksCache(std::string url, std::chrono::seconds ttl);
    core::Result<KeyPtr> GetKey(const std::string& kid);

private:
    core::Result<void> Refresh();
    core::Result<void> LoadFromBody(const std::string& body);
    core::Result<std::string> FetchJwksBody();

    std::string url_;
    std::chrono::seconds ttl_;
    std::chrono::system_clock::time_point expires_at_{};
    std::mutex mutex_;
    std::unordered_map<std::string, KeyPtr> keys_;
};

}  // namespace nebulafs::auth
