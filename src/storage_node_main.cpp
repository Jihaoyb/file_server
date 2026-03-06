#include <chrono>
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
#include "nebulafs/core/ids.h"
#include "nebulafs/core/logger.h"
#include "nebulafs/distributed/placement_token.h"
#include "nebulafs/observability/metrics.h"

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
               Poco::Net::HTTPResponse::HTTPStatus status = Poco::Net::HTTPResponse::HTTP_OK,
               const std::string& request_id = "") {
    res.setStatus(status);
    res.setContentType("application/json");
    if (!request_id.empty()) {
        res.set("X-Request-Id", request_id);
    }
    std::ostream& out = res.send();
    obj->stringify(out);
}

void WriteError(Poco::Net::HTTPServerResponse& res, const std::string& request_id,
                const std::string& code, const std::string& message,
                Poco::Net::HTTPResponse::HTTPStatus status) {
    Poco::JSON::Object::Ptr err = new Poco::JSON::Object();
    err->set("code", code);
    err->set("message", message);
    err->set("request_id", request_id);

    Poco::JSON::Object::Ptr root = new Poco::JSON::Object();
    root->set("error", err);
    WriteJson(res, root, status, request_id);
}

long long ElapsedMs(const std::chrono::steady_clock::time_point& started_at) {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
               std::chrono::steady_clock::now() - started_at)
        .count();
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
        const std::string request_id = nebulafs::core::GenerateRequestId();
        Poco::URI uri(req.getURI());
        const auto path = uri.getPath();
        if (path == "/healthz") {
            Poco::JSON::Object::Ptr root = new Poco::JSON::Object();
            root->set("status", "ok");
            return WriteJson(res, root, Poco::Net::HTTPResponse::HTTP_OK, request_id);
        }
        if (path == "/metrics") {
            res.setStatus(Poco::Net::HTTPResponse::HTTP_OK);
            res.setContentType("text/plain; version=0.0.4");
            res.set("X-Request-Id", request_id);
            res.send() << nebulafs::observability::RenderMetrics();
            return;
        }
        if (!IsAuthorized(req, service_token_)) {
            return WriteError(res, request_id, "UNAUTHORIZED", "invalid service token",
                              Poco::Net::HTTPResponse::HTTP_UNAUTHORIZED);
        }

        const std::string prefix = "/internal/v1/blobs/";
        if (path.rfind(prefix, 0) != 0 || path.size() <= prefix.size()) {
            return WriteError(res, request_id, "NOT_FOUND", "route not found",
                              Poco::Net::HTTPResponse::HTTP_NOT_FOUND);
        }
        const std::string blob_id = path.substr(prefix.size());
        const auto file_path = BlobPath(root_path_, blob_id);

        if (req.getMethod() == Poco::Net::HTTPRequest::HTTP_PUT) {
            const auto started_at = std::chrono::steady_clock::now();
            if (!HasValidPlacementToken(req, service_token_, blob_id)) {
                nebulafs::observability::RecordStorageNodeWrite(false, ElapsedMs(started_at));
                return WriteError(res, request_id, "UNAUTHORIZED", "invalid placement token",
                                  Poco::Net::HTTPResponse::HTTP_UNAUTHORIZED);
            }
            std::ofstream out(file_path, std::ios::binary | std::ios::trunc);
            if (!out.is_open()) {
                nebulafs::observability::RecordStorageNodeWrite(false, ElapsedMs(started_at));
                return WriteError(res, request_id, "INTERNAL_ERROR",
                                  "failed to open blob file for write",
                                  Poco::Net::HTTPResponse::HTTP_INTERNAL_SERVER_ERROR);
            }
            out << req.stream().rdbuf();
            out.close();
            nebulafs::observability::RecordStorageNodeWrite(true, ElapsedMs(started_at));
            Poco::JSON::Object::Ptr root = new Poco::JSON::Object();
            root->set("blob_id", blob_id);
            root->set("stored", true);
            return WriteJson(res, root, Poco::Net::HTTPResponse::HTTP_OK, request_id);
        }

        if (req.getMethod() == Poco::Net::HTTPRequest::HTTP_GET) {
            const auto started_at = std::chrono::steady_clock::now();
            if (!std::filesystem::exists(file_path)) {
                nebulafs::observability::RecordStorageNodeRead(false, ElapsedMs(started_at));
                return WriteError(res, request_id, "NOT_FOUND", "blob not found",
                                  Poco::Net::HTTPResponse::HTTP_NOT_FOUND);
            }
            res.setStatus(Poco::Net::HTTPResponse::HTTP_OK);
            res.setContentType("application/octet-stream");
            res.set("X-Request-Id", request_id);
            std::ifstream in(file_path, std::ios::binary);
            std::ostream& out = res.send();
            out << in.rdbuf();
            nebulafs::observability::RecordStorageNodeRead(true, ElapsedMs(started_at));
            return;
        }

        if (req.getMethod() == Poco::Net::HTTPRequest::HTTP_DELETE) {
            const auto started_at = std::chrono::steady_clock::now();
            std::error_code ec;
            const bool removed = std::filesystem::remove(file_path, ec);
            if (ec) {
                nebulafs::observability::RecordStorageNodeDelete(false, ElapsedMs(started_at));
                return WriteError(res, request_id, "INTERNAL_ERROR", ec.message(),
                                  Poco::Net::HTTPResponse::HTTP_INTERNAL_SERVER_ERROR);
            }
            nebulafs::observability::RecordStorageNodeDelete(true, ElapsedMs(started_at));
            Poco::JSON::Object::Ptr root = new Poco::JSON::Object();
            root->set("blob_id", blob_id);
            root->set("deleted", removed);
            return WriteJson(res, root, Poco::Net::HTTPResponse::HTTP_OK, request_id);
        }

        return WriteError(res, request_id, "METHOD_NOT_ALLOWED", "method not allowed",
                          Poco::Net::HTTPResponse::HTTP_METHOD_NOT_ALLOWED);
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
