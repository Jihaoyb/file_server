#include <array>
#include <chrono>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <optional>
#include <sstream>
#include <string>
#include <vector>

#include <Poco/DigestEngine.h>
#include <Poco/JSON/Array.h>
#include <Poco/JSON/Object.h>
#include <Poco/JSON/Parser.h>
#include <Poco/Net/HTTPServer.h>
#include <Poco/Net/HTTPServerParams.h>
#include <Poco/Net/HTTPRequest.h>
#include <Poco/Net/HTTPRequestHandler.h>
#include <Poco/Net/HTTPRequestHandlerFactory.h>
#include <Poco/Net/HTTPServerRequest.h>
#include <Poco/Net/HTTPServerResponse.h>
#include <Poco/Net/HTTPResponse.h>
#include <Poco/Net/ServerSocket.h>
#include <Poco/SHA2Engine.h>
#include <Poco/Thread.h>
#include <Poco/URI.h>
#include <Poco/UUIDGenerator.h>

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

std::optional<std::vector<std::string>> ParseComposeSources(std::istream& stream) {
    try {
        Poco::JSON::Parser parser;
        auto root = parser.parse(stream).extract<Poco::JSON::Object::Ptr>();
        auto source_blob_ids = root->getArray("source_blob_ids");
        if (!source_blob_ids || source_blob_ids->empty()) {
            return std::nullopt;
        }
        std::vector<std::string> blob_ids;
        blob_ids.reserve(source_blob_ids->size());
        for (size_t i = 0; i < source_blob_ids->size(); ++i) {
            const auto blob_id = source_blob_ids->getElement<std::string>(i);
            if (blob_id.empty()) {
                return std::nullopt;
            }
            blob_ids.push_back(blob_id);
        }
        return blob_ids;
    } catch (const std::exception&) {
        return std::nullopt;
    }
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
        std::string blob_id = path.substr(prefix.size());
        bool is_compose_route = false;
        constexpr const char* kComposeSuffix = "/compose";
        if (blob_id.size() > std::strlen(kComposeSuffix) &&
            blob_id.compare(blob_id.size() - std::strlen(kComposeSuffix),
                            std::strlen(kComposeSuffix), kComposeSuffix) == 0) {
            is_compose_route = true;
            blob_id = blob_id.substr(0, blob_id.size() - std::strlen(kComposeSuffix));
        }
        if (blob_id.empty()) {
            return WriteError(res, request_id, "NOT_FOUND", "route not found",
                              Poco::Net::HTTPResponse::HTTP_NOT_FOUND);
        }
        const auto file_path = BlobPath(root_path_, blob_id);

        if (is_compose_route) {
            if (req.getMethod() != Poco::Net::HTTPRequest::HTTP_POST) {
                return WriteError(res, request_id, "METHOD_NOT_ALLOWED", "method not allowed",
                                  Poco::Net::HTTPResponse::HTTP_METHOD_NOT_ALLOWED);
            }

            const auto started_at = std::chrono::steady_clock::now();
            if (!HasValidPlacementToken(req, service_token_, blob_id)) {
                nebulafs::observability::RecordStorageNodeCompose(false, ElapsedMs(started_at));
                return WriteError(res, request_id, "UNAUTHORIZED", "invalid placement token",
                                  Poco::Net::HTTPResponse::HTTP_UNAUTHORIZED);
            }

            auto source_blob_ids = ParseComposeSources(req.stream());
            if (!source_blob_ids.has_value()) {
                nebulafs::observability::RecordStorageNodeCompose(false, ElapsedMs(started_at));
                return WriteError(res, request_id, "INVALID_REQUEST",
                                  "source_blob_ids must be a non-empty array",
                                  Poco::Net::HTTPResponse::HTTP_BAD_REQUEST);
            }

            const auto temp_path =
                BlobPath(root_path_, Poco::UUIDGenerator().createOne().toString() + ".compose.tmp");
            std::ofstream out(temp_path, std::ios::binary | std::ios::trunc);
            if (!out.is_open()) {
                nebulafs::observability::RecordStorageNodeCompose(false, ElapsedMs(started_at));
                return WriteError(res, request_id, "INTERNAL_ERROR",
                                  "failed to open compose temp file",
                                  Poco::Net::HTTPResponse::HTTP_INTERNAL_SERVER_ERROR);
            }

            Poco::SHA2Engine256 sha256;
            std::array<char, 8192> buffer{};
            std::uint64_t total_bytes = 0;
            for (const auto& source_blob_id : source_blob_ids.value()) {
                const auto source_path = BlobPath(root_path_, source_blob_id);
                std::ifstream in(source_path, std::ios::binary);
                if (!in.is_open()) {
                    out.close();
                    std::error_code remove_ec;
                    std::filesystem::remove(temp_path, remove_ec);
                    nebulafs::observability::RecordStorageNodeCompose(false, ElapsedMs(started_at));
                    return WriteError(res, request_id, "NOT_FOUND",
                                      "source blob not found: " + source_blob_id,
                                      Poco::Net::HTTPResponse::HTTP_NOT_FOUND);
                }
                while (in) {
                    in.read(buffer.data(), static_cast<std::streamsize>(buffer.size()));
                    const auto bytes = in.gcount();
                    if (bytes <= 0) {
                        break;
                    }
                    out.write(buffer.data(), bytes);
                    if (!out) {
                        out.close();
                        std::error_code remove_ec;
                        std::filesystem::remove(temp_path, remove_ec);
                        nebulafs::observability::RecordStorageNodeCompose(false,
                                                                          ElapsedMs(started_at));
                        return WriteError(res, request_id, "INTERNAL_ERROR",
                                          "failed to write compose output",
                                          Poco::Net::HTTPResponse::HTTP_INTERNAL_SERVER_ERROR);
                    }
                    sha256.update(buffer.data(), static_cast<unsigned int>(bytes));
                    total_bytes += static_cast<std::uint64_t>(bytes);
                }
            }

            out.flush();
            out.close();

            std::error_code rename_ec;
            std::filesystem::rename(temp_path, file_path, rename_ec);
            if (rename_ec) {
                std::error_code remove_ec;
                std::filesystem::remove(temp_path, remove_ec);
                nebulafs::observability::RecordStorageNodeCompose(false, ElapsedMs(started_at));
                return WriteError(res, request_id, "INTERNAL_ERROR", rename_ec.message(),
                                  Poco::Net::HTTPResponse::HTTP_INTERNAL_SERVER_ERROR);
            }

            nebulafs::observability::RecordStorageNodeCompose(true, ElapsedMs(started_at));
            Poco::JSON::Object::Ptr root = new Poco::JSON::Object();
            root->set("blob_id", blob_id);
            root->set("size_bytes", static_cast<Poco::UInt64>(total_bytes));
            root->set("etag", Poco::DigestEngine::digestToHex(sha256.digest()));
            return WriteJson(res, root, Poco::Net::HTTPResponse::HTTP_OK, request_id);
        }

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
