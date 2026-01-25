#include "nebulafs/http/router.h"

#include <sstream>

namespace nebulafs::http {

void Router::Add(const std::string& method, const std::string& pattern, Handler handler) {
    routes_.push_back(RouteEntry{method, pattern, std::move(handler)});
}

void Router::Use(Middleware middleware) { middleware_.push_back(std::move(middleware)); }

core::Result<HttpResponse> Router::Route(const RequestContext& ctx,
                                        const HttpRequest& request) const {
    const auto target = std::string(request.target());
    const auto path = target.substr(0, target.find('?'));

    for (const auto& route : routes_) {
        if (route.method != request.method_string()) {
            continue;
        }
        RouteParams params;
        if (Match(route.pattern, path, &params)) {
            RequestContext mutable_ctx = ctx;
            HttpRequest mutable_request = request;
            for (const auto& middleware : middleware_) {
                auto maybe_response = middleware(mutable_ctx, mutable_request, params);
                if (maybe_response) {
                    return *maybe_response;
                }
            }
            return route.handler(mutable_ctx, mutable_request, params);
        }
    }

    HttpResponse response{boost::beast::http::status::not_found, request.version()};
    response.set(boost::beast::http::field::content_type, "application/json");
    response.body() =
        "{\"error\":{\"code\":\"NOT_FOUND\",\"message\":\"route not found\","
        "\"request_id\":\"" + ctx.request_id + "\"}}";
    response.prepare_payload();
    return response;
}

std::vector<std::string> Router::SplitPath(const std::string& path) {
    std::vector<std::string> parts;
    std::stringstream ss(path);
    std::string item;
    while (std::getline(ss, item, '/')) {
        if (!item.empty()) {
            parts.push_back(item);
        }
    }
    return parts;
}

bool Router::Match(const std::string& pattern, const std::string& path, RouteParams* out_params) {
    auto pattern_parts = SplitPath(pattern);
    auto path_parts = SplitPath(path);
    if (pattern_parts.size() != path_parts.size()) {
        return false;
    }

    for (size_t i = 0; i < pattern_parts.size(); ++i) {
        const auto& p = pattern_parts[i];
        const auto& v = path_parts[i];
        if (p.size() >= 2 && p.front() == '{' && p.back() == '}') {
            if (out_params) {
                (*out_params)[p.substr(1, p.size() - 2)] = v;
            }
        } else if (p != v) {
            return false;
        }
    }
    return true;
}

}  // namespace nebulafs::http
