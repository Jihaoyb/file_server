#pragma once

#include <optional>
#include <string>
#include <vector>

namespace nebulafs::http {

struct AuthContext {
    std::string subject;
    std::string issuer;
    std::vector<std::string> audience;
    std::vector<std::string> scopes;
};

/// @brief Per-request metadata used for logging and error responses.
struct RequestContext {
    std::string request_id;
    std::string method;
    std::string target;
    std::string remote;
    std::optional<AuthContext> auth;
};

}  // namespace nebulafs::http
