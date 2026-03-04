#pragma once

#include <map>
#include <string>

#include "nebulafs/core/result.h"

namespace nebulafs::distributed {

struct HttpCallResult {
    int status{0};
    std::string body;
    std::map<std::string, std::string> headers;
};

core::Result<HttpCallResult> SendHttpRequest(const std::string& method,
                                             const std::string& url,
                                             const std::string& body,
                                             const std::string& content_type,
                                             const std::string& bearer_token,
                                             const std::map<std::string, std::string>& headers);

}  // namespace nebulafs::distributed
