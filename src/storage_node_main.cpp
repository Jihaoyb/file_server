#include <filesystem>
#include <fstream>
#include <sstream>
#include <string>

#include <Poco/JSON/Object.h>
#include <Poco/Net/HTTPServer.h>
#include <Poco/Net/HTTPServerParams.h>
#include <Poco/Net/HTTPRequest.h>
#include <Poco/Net/HTTPRequestHandler.h>
#include <Poco/Net/HTTPRequestHandlerFactory.h>
#include <Poco/Net/HTTPServerRequest.h>
#include <Poco/Net/HTTPServerResponse.h>
#include <Poco/Net/HTTPResponse.h>
#include <Poco/Net/ServerSocket.h>
#include <Poco/Thread.h>
#include <Poco/URI.h>

#include "nebulafs/core/config.h"
#include "nebulafs/core/logger.h"
#include "nebulafs/distributed/placement_token.h"

namespace {

std::string GetArgValue(int argc, char** argv, const std::string& key,
                        const std::string& default_value) {
    for (int i = 1; i < argc - 1; ++i) {
        if (argv[i] == key) {
            return argv[i + 1];
        }
    }
    return default_value;
}

bool IsAuthorized(const Poco::Net::HTTPServerRequest& req, const std::string& token) {
    const auto auth = req.get("Authorization", "");
    return auth == "Bearer " + token;
}

std::string BlobPath(const std::string& root, const std::string& blob_id) {
    return (std::filesystem::path(root) / "blobs" / blob_id).string();
}

bool HasValidPlacementToken(const Poco::Net::HTTPServerRequest& req,
                            const std::string& service_token,
                            const std::string& blob_id) {
    return nebulafs::distributed::ValidatePlacementToken(req.get("X-Placement-Token", ""), blob_id,
                                                         "write", service_token);
}

void WriteJson(Poco::Net::HTTPServerResponse& res, Poco::JSON::Object::Ptr obj,
               Poco::Net::HTTPResponse::HTTPStatus status = Poco::Net::HTTPResponse::HTTP_OK) {
    res.setStatus(status);
    res.setContentType("application/json");
    std::ostream& out = res.send();
    obj->stringify(out);
}

class StorageNodeHandler : public Poco::Net::HTTPRequestHandler {
public:
    StorageNodeHandler(std::string root_path, std::string service_token)
        : root_path_(std::move(root_path)),
          service_token_(std::move(service_token)) {
        std::filesystem::create_directories(std::filesystem::path(root_path_) / "blobs");
    }

    void handleRequest(Poco::Net::HTTPServerRequest& req,
                       Poco::Net::HTTPServerResponse& res) override {
        Poco::URI uri(req.getURI());
        const auto path = uri.getPath();
        if (path == "/healthz") {
            Poco::JSON::Object::Ptr root = new Poco::JSON::Object();
            root->set("status", "ok");
            return WriteJson(res, root);
        }
        if (!IsAuthorized(req, service_token_)) {
            res.setStatus(Poco::Net::HTTPResponse::HTTP_UNAUTHORIZED);
            res.send();
            return;
        }

        const std::string prefix = "/internal/v1/blobs/";
        if (path.rfind(prefix, 0) != 0 || path.size() <= prefix.size()) {
            res.setStatus(Poco::Net::HTTPResponse::HTTP_NOT_FOUND);
            res.send();
            return;
        }
        const std::string blob_id = path.substr(prefix.size());
        const auto file_path = BlobPath(root_path_, blob_id);

        if (req.getMethod() == Poco::Net::HTTPRequest::HTTP_PUT) {
            if (!HasValidPlacementToken(req, service_token_, blob_id)) {
                res.setStatus(Poco::Net::HTTPResponse::HTTP_UNAUTHORIZED);
                res.send();
                return;
            }
            std::ofstream out(file_path, std::ios::binary | std::ios::trunc);
            out << req.stream().rdbuf();
            out.close();
            Poco::JSON::Object::Ptr root = new Poco::JSON::Object();
            root->set("blob_id", blob_id);
            root->set("stored", true);
            return WriteJson(res, root);
        }

        if (req.getMethod() == Poco::Net::HTTPRequest::HTTP_GET) {
            if (!std::filesystem::exists(file_path)) {
                res.setStatus(Poco::Net::HTTPResponse::HTTP_NOT_FOUND);
                res.send();
                return;
            }
            res.setStatus(Poco::Net::HTTPResponse::HTTP_OK);
            res.setContentType("application/octet-stream");
            std::ifstream in(file_path, std::ios::binary);
            std::ostream& out = res.send();
            out << in.rdbuf();
            return;
        }

        if (req.getMethod() == Poco::Net::HTTPRequest::HTTP_DELETE) {
            std::error_code ec;
            std::filesystem::remove(file_path, ec);
            Poco::JSON::Object::Ptr root = new Poco::JSON::Object();
            root->set("blob_id", blob_id);
            root->set("deleted", true);
            return WriteJson(res, root);
        }

        res.setStatus(Poco::Net::HTTPResponse::HTTP_METHOD_NOT_ALLOWED);
        res.send();
    }

private:
    std::string root_path_;
    std::string service_token_;
};

class StorageNodeHandlerFactory : public Poco::Net::HTTPRequestHandlerFactory {
public:
    StorageNodeHandlerFactory(std::string root_path, std::string service_token)
        : root_path_(std::move(root_path)),
          service_token_(std::move(service_token)) {
    }

    Poco::Net::HTTPRequestHandler* createRequestHandler(
        const Poco::Net::HTTPServerRequest&) override {
        return new StorageNodeHandler(root_path_, service_token_);
    }

private:
    std::string root_path_;
    std::string service_token_;
};

}  // namespace

int main(int argc, char** argv) {
    const std::string config_path = GetArgValue(argc, argv, "--config", "config/server.json");
    auto config = nebulafs::core::LoadConfig(config_path);
    nebulafs::core::InitLogging(config.observability.log_level);

    Poco::Net::ServerSocket socket(config.server.port);
    Poco::Net::HTTPServer server(
        new StorageNodeHandlerFactory(config.storage.base_path, config.distributed.service_auth_token),
        socket, new Poco::Net::HTTPServerParams());
    server.start();
    nebulafs::core::LogInfo("Storage node listening on port " + std::to_string(config.server.port));
    while (true) {
        Poco::Thread::sleep(1000);
    }
}
