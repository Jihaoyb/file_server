#include <cstdlib>
#include <filesystem>
#include <iostream>
#include <memory>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

#include <Poco/JSON/Array.h>
#include <Poco/JSON/Object.h>
#include <Poco/JSON/Parser.h>

#include <boost/asio/io_context.hpp>

#include "nebulafs/core/config.h"
#include "nebulafs/core/logger.h"
#include "nebulafs/http/http_server.h"
#include "nebulafs/http/router.h"
#include "nebulafs/metadata/sqlite_metadata_store.h"
#include "nebulafs/observability/metrics.h"
#include "nebulafs/storage/local_storage.h"

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

std::string GetQueryParam(const std::string& target, const std::string& key) {
    auto pos = target.find('?');
    if (pos == std::string::npos) {
        return "";
    }
    auto query = target.substr(pos + 1);
    std::stringstream ss(query);
    std::string item;
    while (std::getline(ss, item, '&')) {
        auto eq = item.find('=');
        if (eq == std::string::npos) {
            continue;
        }
        if (item.substr(0, eq) == key) {
            return item.substr(eq + 1);
        }
    }
    return "";
}

nebulafs::http::HttpResponse JsonOk(int version, const std::string& body) {
    nebulafs::http::HttpResponse response{boost::beast::http::status::ok, version};
    response.set(boost::beast::http::field::content_type, "application/json");
    response.body() = body;
    response.prepare_payload();
    return response;
}

nebulafs::http::HttpResponse JsonError(int version, const std::string& code,
                                       const std::string& message,
                                       const std::string& request_id,
                                       boost::beast::http::status status) {
    nebulafs::http::HttpResponse response{status, version};
    response.set(boost::beast::http::field::content_type, "application/json");
    response.body() = "{\"error\":{\"code\":\"" + code + "\",\"message\":\"" + message +
                      "\",\"request_id\":\"" + request_id + "\"}}";
    response.prepare_payload();
    return response;
}

}  // namespace

int main(int argc, char** argv) {
    const std::string config_path = GetArgValue(argc, argv, "--config", "config/server.json");
    const std::string db_path = GetArgValue(argc, argv, "--database", "config/database.json");

    auto config = nebulafs::core::LoadConfig(config_path);
    nebulafs::core::InitLogging(config.observability.log_level);

    auto sqlite_path = nebulafs::core::LoadDatabasePath(db_path);
    std::filesystem::create_directories(std::filesystem::path(sqlite_path).parent_path());
    auto metadata = std::make_shared<nebulafs::metadata::SqliteMetadataStore>(sqlite_path);
    auto storage = std::make_shared<nebulafs::storage::LocalStorage>(config.storage.base_path,
                                                                     config.storage.temp_path);

    nebulafs::http::Router router;

    router.Add("GET", "/healthz",
               [](const nebulafs::http::RequestContext& ctx,
                  const nebulafs::http::HttpRequest& req,
                  const nebulafs::http::RouteParams&) {
                   return JsonOk(req.version(),
                                 "{\"status\":\"ok\",\"request_id\":\"" +
                                     ctx.request_id + "\"}");
               });

    router.Add("GET", "/readyz",
               [](const nebulafs::http::RequestContext& ctx,
                  const nebulafs::http::HttpRequest& req,
                  const nebulafs::http::RouteParams&) {
                   return JsonOk(req.version(),
                                 "{\"status\":\"ready\",\"request_id\":\"" +
                                     ctx.request_id + "\"}");
               });

    router.Add("GET", "/metrics",
               [](const nebulafs::http::RequestContext&, const nebulafs::http::HttpRequest& req,
                  const nebulafs::http::RouteParams&) {
                   nebulafs::http::HttpResponse response{boost::beast::http::status::ok,
                                                         req.version()};
                   response.set(boost::beast::http::field::content_type, "text/plain");
                   response.body() = nebulafs::observability::RenderMetrics();
                   response.prepare_payload();
                   return response;
               });

    router.Add("POST", "/v1/buckets",
               [metadata](const nebulafs::http::RequestContext& ctx,
                          const nebulafs::http::HttpRequest& req,
                          const nebulafs::http::RouteParams&) {
                   try {
                       Poco::JSON::Parser parser;
                       auto result = parser.parse(req.body());
                       auto obj = result.extract<Poco::JSON::Object::Ptr>();
                       auto name = obj->getValue<std::string>("name");
                       if (!nebulafs::storage::LocalStorage::IsSafeName(name)) {
                           return JsonError(req.version(), "INVALID_NAME", "invalid bucket name",
                                            ctx.request_id,
                                            boost::beast::http::status::bad_request);
                       }
                       auto created = metadata->CreateBucket(name);
                       if (!created.ok()) {
                           return JsonError(req.version(), "ALREADY_EXISTS", "bucket exists",
                                            ctx.request_id,
                                            boost::beast::http::status::conflict);
                       }
                       return JsonOk(req.version(),
                                     "{\"name\":\"" + created.value().name + "\"}");
                   } catch (const std::exception& ex) {
                       return JsonError(req.version(), "INVALID_JSON", ex.what(), ctx.request_id,
                                        boost::beast::http::status::bad_request);
                   }
               });

    router.Add("GET", "/v1/buckets",
               [metadata](const nebulafs::http::RequestContext& ctx,
                          const nebulafs::http::HttpRequest& req,
                          const nebulafs::http::RouteParams&) {
                   auto buckets = metadata->ListBuckets();
                   if (!buckets.ok()) {
                       return JsonError(req.version(), "DB_ERROR", buckets.error().message,
                                        ctx.request_id,
                                        boost::beast::http::status::internal_server_error);
                   }
                   Poco::JSON::Array::Ptr arr = new Poco::JSON::Array();
                   for (const auto& bucket : buckets.value()) {
                       Poco::JSON::Object::Ptr item = new Poco::JSON::Object();
                       item->set("name", bucket.name);
                       item->set("created_at", bucket.created_at);
                       arr->add(item);
                   }
                   Poco::JSON::Object::Ptr root = new Poco::JSON::Object();
                   root->set("buckets", arr);
                   std::stringstream ss;
                   root->stringify(ss);
                   return JsonOk(req.version(), ss.str());
               });

    router.Add("GET", "/v1/buckets/{bucket}/objects",
               [metadata](const nebulafs::http::RequestContext& ctx,
                          const nebulafs::http::HttpRequest& req,
                          const nebulafs::http::RouteParams& params) {
                   const auto bucket = params.at("bucket");
                   const auto prefix = GetQueryParam(std::string(req.target()), "prefix");
                   auto objects = metadata->ListObjects(bucket, prefix);
                   if (!objects.ok()) {
                       return JsonError(req.version(), "DB_ERROR", objects.error().message,
                                        ctx.request_id,
                                        boost::beast::http::status::internal_server_error);
                   }
                   Poco::JSON::Array::Ptr arr = new Poco::JSON::Array();
                   for (const auto& object : objects.value()) {
                       Poco::JSON::Object::Ptr item = new Poco::JSON::Object();
                       item->set("name", object.name);
                       item->set("size", static_cast<Poco::UInt64>(object.size_bytes));
                       item->set("etag", object.etag);
                       item->set("updated_at", object.updated_at);
                       arr->add(item);
                   }
                   Poco::JSON::Object::Ptr root = new Poco::JSON::Object();
                   root->set("objects", arr);
                   std::stringstream ss;
                   root->stringify(ss);
                   return JsonOk(req.version(), ss.str());
               });

    router.Add("DELETE", "/v1/buckets/{bucket}/objects/{object}",
               [metadata, storage](const nebulafs::http::RequestContext& ctx,
                                   const nebulafs::http::HttpRequest& req,
                                   const nebulafs::http::RouteParams& params) {
                   const auto bucket = params.at("bucket");
                   const auto object = params.at("object");
                   auto storage_result = storage->DeleteObject(bucket, object);
                   if (!storage_result.ok()) {
                       return JsonError(req.version(), "OBJECT_NOT_FOUND", "object not found",
                                        ctx.request_id,
                                        boost::beast::http::status::not_found);
                   }
                   metadata->DeleteObject(bucket, object);
                   return JsonOk(req.version(), "{\"deleted\":true}");
               });

    boost::asio::io_context ioc(config.server.threads);
    nebulafs::http::HttpServer server(ioc, config, std::move(router), storage, metadata);
    server.Run();

    std::vector<std::thread> threads;
    threads.reserve(static_cast<size_t>(config.server.threads));
    for (int i = 0; i < config.server.threads; ++i) {
        threads.emplace_back([&ioc]() { ioc.run(); });
    }
    for (auto& t : threads) {
        t.join();
    }

    return 0;
}
