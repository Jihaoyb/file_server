#pragma once

#include <string>

namespace nebulafs::http {

/// @brief Per-request metadata used for logging and error responses.
struct RequestContext {
    std::string request_id;
    std::string method;
    std::string target;
    std::string remote;
};

}  // namespace nebulafs::http
