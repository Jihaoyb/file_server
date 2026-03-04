#include "nebulafs/distributed/http_client.h"

#include <istream>
#include <sstream>
#include <string>

#include <Poco/Exception.h>
#include <Poco/Net/HTTPClientSession.h>
#include <Poco/Net/HTTPRequest.h>
#include <Poco/Net/HTTPResponse.h>
#include <Poco/URI.h>

#include "nebulafs/core/error.h"

namespace nebulafs::distributed {

core::Result<HttpCallResult> SendHttpRequest(const std::string& method, const std::string& url,
                                             const std::string& body,
                                             const std::string& content_type,
                                             const std::string& bearer_token,
                                             const std::map<std::string, std::string>& headers) {
    try {
        Poco::URI uri(url);
        const std::string path_and_query = uri.getPathAndQuery().empty() ? "/" : uri.getPathAndQuery();

        Poco::Net::HTTPClientSession session(uri.getHost(), uri.getPort());
        Poco::Net::HTTPRequest request(method, path_and_query, Poco::Net::HTTPMessage::HTTP_1_1);
        request.set(Poco::Net::HTTPRequest::HOST, uri.getHost());
        request.set("User-Agent", "NebulaFS-Distributed");
        if (!content_type.empty()) {
            request.setContentType(content_type);
        }
        if (!bearer_token.empty()) {
            request.set("Authorization", "Bearer " + bearer_token);
        }
        for (const auto& header : headers) {
            request.set(header.first, header.second);
        }
        request.setContentLength(static_cast<int>(body.size()));

        std::ostream& out = session.sendRequest(request);
        out << body;

        Poco::Net::HTTPResponse response;
        std::istream& in = session.receiveResponse(response);
        std::ostringstream ss;
        ss << in.rdbuf();

        HttpCallResult result;
        result.status = static_cast<int>(response.getStatus());
        result.body = ss.str();
        for (const auto& name : response) {
            result.headers[name.first] = name.second;
        }
        return result;
    } catch (const Poco::Exception& ex) {
        return core::Error{core::ErrorCode::kIoError, ex.displayText()};
    }
}

}  // namespace nebulafs::distributed
