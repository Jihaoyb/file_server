#pragma once

#include <functional>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

#include <boost/beast/http.hpp>

#include "nebulafs/core/result.h"
#include "nebulafs/http/request_context.h"

namespace nebulafs::http {

using HttpRequest = boost::beast::http::request<boost::beast::http::string_body>;
using HttpResponse = boost::beast::http::response<boost::beast::http::string_body>;
using RouteParams = std::unordered_map<std::string, std::string>;
using Handler = std::function<core::Result<HttpResponse>(const RequestContext&, const HttpRequest&, const RouteParams&)>;
using Middleware = std::function<std::optional<HttpResponse>(RequestContext&, HttpRequest&, RouteParams&)>;

/// @brief Simple route table with path-template matching.
class Router {
public:
    void Add(const std::string& method, const std::string& pattern, Handler handler);
    void Use(Middleware middleware);
    core::Result<HttpResponse> Route(const RequestContext& ctx, const HttpRequest& request) const;

    static bool Match(const std::string& pattern, const std::string& path, RouteParams* out_params);

private:
    struct RouteEntry {
        std::string method;
        std::string pattern;
        Handler handler;
    };

    static std::vector<std::string> SplitPath(const std::string& path);

    std::vector<RouteEntry> routes_;
    std::vector<Middleware> middleware_;
};

}  // namespace nebulafs::http
