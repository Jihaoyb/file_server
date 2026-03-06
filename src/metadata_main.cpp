#include <chrono>
#include <filesystem>
#include <memory>
#include <sstream>
#include <string>

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
#include <Poco/Thread.h>
#include <Poco/URI.h>

#include "nebulafs/core/config.h"
#include "nebulafs/core/ids.h"
#include "nebulafs/core/logger.h"
#include "nebulafs/metadata/sqlite_metadata_store.h"
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

Poco::JSON::Object::Ptr ParseBody(Poco::Net::HTTPServerRequest& req) {
    std::stringstream body;
    body << req.stream().rdbuf();
    Poco::JSON::Parser parser;
    return parser.parse(body.str()).extract<Poco::JSON::Object::Ptr>();
}

long long ElapsedMs(const std::chrono::steady_clock::time_point& started_at) {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
               std::chrono::steady_clock::now() - started_at)
        .count();
}

class MetadataHandler : public Poco::Net::HTTPRequestHandler {
public:
    MetadataHandler(std::shared_ptr<nebulafs::metadata::SqliteMetadataStore> store,
                    std::string token)
        : store_(std::move(store)),
          token_(std::move(token)) {
    }

    void handleRequest(Poco::Net::HTTPServerRequest& req,
                       Poco::Net::HTTPServerResponse& res) override {
        const std::string request_id = nebulafs::core::GenerateRequestId();
        Poco::URI uri(req.getURI());
        const auto path = uri.getPath();

        try {
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

            if (path.rfind("/internal/v1/", 0) == 0 && !IsAuthorized(req, token_)) {
                return WriteError(res, request_id, "UNAUTHORIZED", "invalid service token",
                                  Poco::Net::HTTPResponse::HTTP_UNAUTHORIZED);
            }

            if (req.getMethod() == Poco::Net::HTTPRequest::HTTP_POST &&
                path == "/internal/v1/storage-nodes/configure") {
                auto body = ParseBody(req);
                auto arr = body->getArray("endpoints");
                std::vector<std::string> endpoints;
                for (size_t i = 0; i < arr->size(); ++i) {
                    endpoints.push_back(arr->getElement<std::string>(i));
                }
                auto result = store_->ConfigureStorageNodes(endpoints);
                if (!result.ok()) {
                    return WriteError(res, request_id, "INTERNAL_ERROR", result.error().message,
                                      Poco::Net::HTTPResponse::HTTP_INTERNAL_SERVER_ERROR);
                }
                Poco::JSON::Object::Ptr root = new Poco::JSON::Object();
                root->set("ok", true);
                return WriteJson(res, root, Poco::Net::HTTPResponse::HTTP_OK, request_id);
            }

            if (req.getMethod() == Poco::Net::HTTPRequest::HTTP_POST &&
                path == "/internal/v1/buckets/create") {
                auto body = ParseBody(req);
                auto result = store_->CreateBucket(body->getValue<std::string>("name"));
                if (!result.ok()) {
                    if (result.error().code == nebulafs::core::ErrorCode::kAlreadyExists) {
                        return WriteError(res, request_id, "ALREADY_EXISTS", "bucket exists",
                                          Poco::Net::HTTPResponse::HTTP_CONFLICT);
                    }
                    return WriteError(res, request_id, "INTERNAL_ERROR", result.error().message,
                                      Poco::Net::HTTPResponse::HTTP_INTERNAL_SERVER_ERROR);
                }
                Poco::JSON::Object::Ptr root = new Poco::JSON::Object();
                root->set("id", result.value().id);
                root->set("name", result.value().name);
                root->set("created_at", result.value().created_at);
                return WriteJson(res, root, Poco::Net::HTTPResponse::HTTP_OK, request_id);
            }

            if (req.getMethod() == Poco::Net::HTTPRequest::HTTP_GET &&
                path == "/internal/v1/buckets/list") {
                auto result = store_->ListBuckets();
                if (!result.ok()) {
                    return WriteError(res, request_id, "INTERNAL_ERROR", result.error().message,
                                      Poco::Net::HTTPResponse::HTTP_INTERNAL_SERVER_ERROR);
                }
                Poco::JSON::Array::Ptr arr = new Poco::JSON::Array();
                for (const auto& bucket : result.value()) {
                    Poco::JSON::Object::Ptr item = new Poco::JSON::Object();
                    item->set("id", bucket.id);
                    item->set("name", bucket.name);
                    item->set("created_at", bucket.created_at);
                    arr->add(item);
                }
                Poco::JSON::Object::Ptr root = new Poco::JSON::Object();
                root->set("buckets", arr);
                return WriteJson(res, root, Poco::Net::HTTPResponse::HTTP_OK, request_id);
            }

            if (req.getMethod() == Poco::Net::HTTPRequest::HTTP_GET &&
                path == "/internal/v1/buckets/get") {
                auto params = uri.getQueryParameters();
                std::string name;
                for (const auto& p : params) {
                    if (p.first == "name") name = p.second;
                }
                auto result = store_->GetBucket(name);
                if (!result.ok()) {
                    return WriteError(res, request_id, "NOT_FOUND", "bucket not found",
                                      Poco::Net::HTTPResponse::HTTP_NOT_FOUND);
                }
                Poco::JSON::Object::Ptr root = new Poco::JSON::Object();
                root->set("id", result.value().id);
                root->set("name", result.value().name);
                root->set("created_at", result.value().created_at);
                return WriteJson(res, root, Poco::Net::HTTPResponse::HTTP_OK, request_id);
            }

            if (req.getMethod() == Poco::Net::HTTPRequest::HTTP_POST &&
                path == "/internal/v1/objects/upsert") {
                auto body = ParseBody(req);
                nebulafs::metadata::ObjectMetadata meta;
                meta.name = body->getValue<std::string>("name");
                meta.size_bytes = body->getValue<Poco::UInt64>("size_bytes");
                meta.etag = body->getValue<std::string>("etag");
                auto result = store_->UpsertObject(body->getValue<std::string>("bucket"), meta);
                if (!result.ok()) {
                    return WriteError(res, request_id, "INTERNAL_ERROR", result.error().message,
                                      Poco::Net::HTTPResponse::HTTP_INTERNAL_SERVER_ERROR);
                }
                Poco::JSON::Object::Ptr root = new Poco::JSON::Object();
                root->set("id", result.value().id);
                root->set("bucket_id", result.value().bucket_id);
                root->set("name", result.value().name);
                root->set("size_bytes", static_cast<Poco::UInt64>(result.value().size_bytes));
                root->set("etag", result.value().etag);
                root->set("created_at", result.value().created_at);
                root->set("updated_at", result.value().updated_at);
                return WriteJson(res, root, Poco::Net::HTTPResponse::HTTP_OK, request_id);
            }

            if (req.getMethod() == Poco::Net::HTTPRequest::HTTP_GET &&
                path == "/internal/v1/objects/get") {
                std::string bucket;
                std::string object_name;
                for (const auto& p : uri.getQueryParameters()) {
                    if (p.first == "bucket") bucket = p.second;
                    if (p.first == "object") object_name = p.second;
                }
                auto result = store_->GetObject(bucket, object_name);
                if (!result.ok()) {
                    return WriteError(res, request_id, "NOT_FOUND", "object not found",
                                      Poco::Net::HTTPResponse::HTTP_NOT_FOUND);
                }
                Poco::JSON::Object::Ptr root = new Poco::JSON::Object();
                root->set("id", result.value().id);
                root->set("bucket_id", result.value().bucket_id);
                root->set("name", result.value().name);
                root->set("size_bytes", static_cast<Poco::UInt64>(result.value().size_bytes));
                root->set("etag", result.value().etag);
                root->set("created_at", result.value().created_at);
                root->set("updated_at", result.value().updated_at);
                return WriteJson(res, root, Poco::Net::HTTPResponse::HTTP_OK, request_id);
            }

            if (req.getMethod() == Poco::Net::HTTPRequest::HTTP_GET &&
                path == "/internal/v1/objects/list") {
                std::string bucket;
                std::string prefix;
                for (const auto& p : uri.getQueryParameters()) {
                    if (p.first == "bucket") bucket = p.second;
                    if (p.first == "prefix") prefix = p.second;
                }
                auto result = store_->ListObjects(bucket, prefix);
                if (!result.ok()) {
                    return WriteError(res, request_id, "INTERNAL_ERROR", result.error().message,
                                      Poco::Net::HTTPResponse::HTTP_INTERNAL_SERVER_ERROR);
                }
                Poco::JSON::Array::Ptr arr = new Poco::JSON::Array();
                for (const auto& object : result.value()) {
                    Poco::JSON::Object::Ptr item = new Poco::JSON::Object();
                    item->set("id", object.id);
                    item->set("bucket_id", object.bucket_id);
                    item->set("name", object.name);
                    item->set("size_bytes", static_cast<Poco::UInt64>(object.size_bytes));
                    item->set("etag", object.etag);
                    item->set("created_at", object.created_at);
                    item->set("updated_at", object.updated_at);
                    arr->add(item);
                }
                Poco::JSON::Object::Ptr root = new Poco::JSON::Object();
                root->set("objects", arr);
                return WriteJson(res, root, Poco::Net::HTTPResponse::HTTP_OK, request_id);
            }

            if (req.getMethod() == Poco::Net::HTTPRequest::HTTP_DELETE &&
                path == "/internal/v1/objects/delete") {
                std::string bucket;
                std::string object_name;
                for (const auto& p : uri.getQueryParameters()) {
                    if (p.first == "bucket") bucket = p.second;
                    if (p.first == "object") object_name = p.second;
                }
                auto result = store_->DeleteObject(bucket, object_name);
                if (!result.ok()) {
                    return WriteError(res, request_id, "INTERNAL_ERROR", result.error().message,
                                      Poco::Net::HTTPResponse::HTTP_INTERNAL_SERVER_ERROR);
                }
                Poco::JSON::Object::Ptr root = new Poco::JSON::Object();
                root->set("ok", true);
                return WriteJson(res, root, Poco::Net::HTTPResponse::HTTP_OK, request_id);
            }

            if (req.getMethod() == Poco::Net::HTTPRequest::HTTP_POST &&
                path == "/internal/v1/objects/allocate-write") {
                const auto started_at = std::chrono::steady_clock::now();
                auto body = ParseBody(req);
                auto result = store_->AllocateWrite(body->getValue<std::string>("bucket"),
                                                    body->getValue<std::string>("object"),
                                                    body->getValue<int>("replication_factor"),
                                                    body->getValue<std::string>("service_token"));
                if (!result.ok()) {
                    nebulafs::observability::RecordMetadataAllocate(false, ElapsedMs(started_at));
                    if (result.error().code == nebulafs::core::ErrorCode::kNotFound) {
                        return WriteError(res, request_id, "NOT_FOUND", result.error().message,
                                          Poco::Net::HTTPResponse::HTTP_NOT_FOUND);
                    }
                    if (result.error().code == nebulafs::core::ErrorCode::kInvalidArgument) {
                        return WriteError(res, request_id, "INVALID_ARGUMENT",
                                          result.error().message,
                                          Poco::Net::HTTPResponse::HTTP_BAD_REQUEST);
                    }
                    return WriteError(res, request_id, "INTERNAL_ERROR", result.error().message,
                                      Poco::Net::HTTPResponse::HTTP_INTERNAL_SERVER_ERROR);
                }
                nebulafs::observability::RecordMetadataAllocate(true, ElapsedMs(started_at));
                Poco::JSON::Array::Ptr replicas = new Poco::JSON::Array();
                for (const auto& replica : result.value().replicas) {
                    Poco::JSON::Object::Ptr item = new Poco::JSON::Object();
                    item->set("node_id", replica.node_id);
                    item->set("replica_index", replica.replica_index);
                    item->set("endpoint", replica.endpoint);
                    replicas->add(item);
                }
                Poco::JSON::Object::Ptr root = new Poco::JSON::Object();
                root->set("blob_id", result.value().blob_id);
                root->set("write_token", result.value().write_token);
                root->set("replicas", replicas);
                return WriteJson(res, root, Poco::Net::HTTPResponse::HTTP_OK, request_id);
            }

            if (req.getMethod() == Poco::Net::HTTPRequest::HTTP_POST &&
                path == "/internal/v1/objects/commit") {
                const auto started_at = std::chrono::steady_clock::now();
                auto body = ParseBody(req);
                std::vector<nebulafs::metadata::ReplicaTarget> replicas;
                auto arr = body->getArray("replicas");
                for (size_t i = 0; i < arr->size(); ++i) {
                    auto item = arr->getObject(i);
                    replicas.push_back(nebulafs::metadata::ReplicaTarget{
                        item->getValue<int>("node_id"), item->getValue<int>("replica_index"),
                        item->getValue<std::string>("endpoint")});
                }
                auto result = store_->CommitWrite(body->getValue<std::string>("bucket"),
                                                  body->getValue<std::string>("object"),
                                                  body->getValue<std::string>("blob_id"),
                                                  body->getValue<Poco::UInt64>("size_bytes"),
                                                  body->getValue<std::string>("etag"), replicas);
                if (!result.ok()) {
                    nebulafs::observability::RecordMetadataCommit(false, ElapsedMs(started_at));
                    return WriteError(res, request_id, "INTERNAL_ERROR", result.error().message,
                                      Poco::Net::HTTPResponse::HTTP_INTERNAL_SERVER_ERROR);
                }
                nebulafs::observability::RecordMetadataCommit(true, ElapsedMs(started_at));
                Poco::JSON::Object::Ptr root = new Poco::JSON::Object();
                root->set("ok", true);
                return WriteJson(res, root, Poco::Net::HTTPResponse::HTTP_OK, request_id);
            }

            if (req.getMethod() == Poco::Net::HTTPRequest::HTTP_GET &&
                path == "/internal/v1/objects/resolve-read") {
                std::string bucket;
                std::string object_name;
                for (const auto& p : uri.getQueryParameters()) {
                    if (p.first == "bucket") bucket = p.second;
                    if (p.first == "object") object_name = p.second;
                }
                auto result = store_->ResolveRead(bucket, object_name);
                if (!result.ok()) {
                    return WriteError(res, request_id, "NOT_FOUND", "object not found",
                                      Poco::Net::HTTPResponse::HTTP_NOT_FOUND);
                }
                Poco::JSON::Array::Ptr replicas = new Poco::JSON::Array();
                for (const auto& replica : result.value().replicas) {
                    Poco::JSON::Object::Ptr item = new Poco::JSON::Object();
                    item->set("node_id", replica.node_id);
                    item->set("replica_index", replica.replica_index);
                    item->set("endpoint", replica.endpoint);
                    replicas->add(item);
                }
                Poco::JSON::Object::Ptr root = new Poco::JSON::Object();
                root->set("blob_id", result.value().blob_id);
                root->set("etag", result.value().etag);
                root->set("size_bytes", static_cast<Poco::UInt64>(result.value().size_bytes));
                root->set("replicas", replicas);
                return WriteJson(res, root, Poco::Net::HTTPResponse::HTTP_OK, request_id);
            }

            return WriteError(res, request_id, "NOT_FOUND", "route not found",
                              Poco::Net::HTTPResponse::HTTP_NOT_FOUND);
        } catch (const Poco::Exception& ex) {
            return WriteError(res, request_id, "INVALID_ARGUMENT", ex.displayText(),
                              Poco::Net::HTTPResponse::HTTP_BAD_REQUEST);
        } catch (const std::exception& ex) {
            return WriteError(res, request_id, "INTERNAL_ERROR", ex.what(),
                              Poco::Net::HTTPResponse::HTTP_INTERNAL_SERVER_ERROR);
        }
    }

private:
    std::shared_ptr<nebulafs::metadata::SqliteMetadataStore> store_;
    std::string token_;
};

class MetadataHandlerFactory : public Poco::Net::HTTPRequestHandlerFactory {
public:
    MetadataHandlerFactory(std::shared_ptr<nebulafs::metadata::SqliteMetadataStore> store,
                           std::string token)
        : store_(std::move(store)),
          token_(std::move(token)) {
    }

    Poco::Net::HTTPRequestHandler* createRequestHandler(
        const Poco::Net::HTTPServerRequest&) override {
        return new MetadataHandler(store_, token_);
    }

private:
    std::shared_ptr<nebulafs::metadata::SqliteMetadataStore> store_;
    std::string token_;
};

}  // namespace

int main(int argc, char** argv) {
    const std::string config_path = GetArgValue(argc, argv, "--config", "config/server.json");
    const std::string db_path = GetArgValue(argc, argv, "--database", "config/database.json");

    auto config = nebulafs::core::LoadConfig(config_path);
    nebulafs::core::InitLogging(config.observability.log_level);
    auto sqlite_path = nebulafs::core::LoadDatabasePath(db_path);
    std::filesystem::create_directories(std::filesystem::path(sqlite_path).parent_path());

    auto store = std::make_shared<nebulafs::metadata::SqliteMetadataStore>(sqlite_path);
    if (!config.distributed.storage_nodes.empty()) {
        auto configured = store->ConfigureStorageNodes(config.distributed.storage_nodes);
        if (!configured.ok()) {
            throw std::runtime_error(configured.error().message);
        }
    }

    Poco::Net::ServerSocket socket(config.server.port);
    Poco::Net::HTTPServer server(
        new MetadataHandlerFactory(store, config.distributed.service_auth_token), socket,
        new Poco::Net::HTTPServerParams());
    server.start();
    nebulafs::core::LogInfo("Metadata service listening on port " + std::to_string(config.server.port));
    while (true) {
        Poco::Thread::sleep(1000);
    }
}
