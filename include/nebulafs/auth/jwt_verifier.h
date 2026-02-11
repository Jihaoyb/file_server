#pragma once

#include <memory>
#include <string>
#include <vector>

#include "nebulafs/auth/jwks_cache.h"
#include "nebulafs/core/config.h"
#include "nebulafs/core/result.h"

namespace nebulafs::auth {

struct JwtClaims {
    std::string subject;
    std::string issuer;
    std::vector<std::string> audience;
    std::vector<std::string> scopes;
};

class JwtVerifier {
public:
    explicit JwtVerifier(core::AuthConfig config);
    core::Result<JwtClaims> Verify(const std::string& token);

private:
    core::AuthConfig config_;
    std::shared_ptr<JwksCache> jwks_;
};

}  // namespace nebulafs::auth
