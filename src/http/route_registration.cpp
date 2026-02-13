#include "nebulafs/http/route_registration.h"

#include <filesystem>
#include <fstream>
#include <optional>
#include <sstream>
#include <string>
#include <cstdint>

#include <Poco/DigestEngine.h>
#include <Poco/JSON/Array.h>
#include <Poco/JSON/Object.h>
#include <Poco/JSON/Parser.h>
#include <Poco/SHA2Engine.h>
#include <Poco/UUIDGenerator.h>

#include "nebulafs/core/time.h"
#include "nebulafs/metadata/metadata_store.h"
#include "nebulafs/observability/metrics.h"
#include "nebulafs/storage/local_storage.h"

namespace nebulafs::http {
namespace {

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

std::optional<int> ParsePositiveInt(const std::string& value) {
    if (value.empty()) {
        return std::nullopt;
    }
    try {
        size_t consumed = 0;
        int parsed = std::stoi(value, &consumed);
        if (consumed != value.size() || parsed <= 0) {
            return std::nullopt;
        }
        return parsed;
    } catch (const std::exception&) {
        return std::nullopt;
    }
}

std::string MultipartPartPath(const std::string& temp_root, const std::string& upload_id,
                              int part_number) {
    return (std::filesystem::path(temp_root) / "multipart" / upload_id /
            ("part-" + std::to_string(part_number)))
        .string();
}

HttpResponse JsonOk(int version, const std::string& body) {
    HttpResponse response{boost::beast::http::status::ok, version};
    response.set(boost::beast::http::field::content_type, "application/json");
    response.body() = body;
    response.prepare_payload();
    return response;
}

HttpResponse JsonError(int version, const std::string& code, const std::string& message,
                       const std::string& request_id, boost::beast::http::status status) {
    HttpResponse response{status, version};
    response.set(boost::beast::http::field::content_type, "application/json");
    response.body() = "{\"error\":{\"code\":\"" + code + "\",\"message\":\"" + message +
                      "\",\"request_id\":\"" + request_id + "\"}}";
    response.prepare_payload();
    return response;
}

}  // namespace

void RegisterDefaultRoutes(Router& router, std::shared_ptr<metadata::MetadataStore> metadata,
                           std::shared_ptr<storage::LocalStorage> storage) {
    router.Add("GET", "/healthz",
               [](const RequestContext& ctx, const HttpRequest& req, const RouteParams&) {
                   return JsonOk(req.version(),
                                 "{\"status\":\"ok\",\"request_id\":\"" + ctx.request_id + "\"}");
               });

    router.Add("GET", "/readyz",
               [](const RequestContext& ctx, const HttpRequest& req, const RouteParams&) {
                   return JsonOk(req.version(),
                                 "{\"status\":\"ready\",\"request_id\":\"" + ctx.request_id + "\"}");
               });

    router.Add("GET", "/metrics",
               [](const RequestContext&, const HttpRequest& req, const RouteParams&) {
                   HttpResponse response{boost::beast::http::status::ok, req.version()};
                   response.set(boost::beast::http::field::content_type, "text/plain");
                   response.body() = observability::RenderMetrics();
                   response.prepare_payload();
                   return response;
               });

    router.Add("POST", "/v1/buckets",
               [metadata](const RequestContext& ctx, const HttpRequest& req, const RouteParams&) {
                   try {
                       Poco::JSON::Parser parser;
                       auto result = parser.parse(req.body());
                       auto obj = result.extract<Poco::JSON::Object::Ptr>();
                       auto name = obj->getValue<std::string>("name");
                       if (!storage::LocalStorage::IsSafeName(name)) {
                           return JsonError(req.version(), "INVALID_NAME", "invalid bucket name",
                                            ctx.request_id,
                                            boost::beast::http::status::bad_request);
                       }
                       auto created = metadata->CreateBucket(name);
                       if (!created.ok()) {
                           return JsonError(req.version(), "ALREADY_EXISTS", "bucket exists",
                                            ctx.request_id, boost::beast::http::status::conflict);
                       }
                       return JsonOk(req.version(), "{\"name\":\"" + created.value().name + "\"}");
                   } catch (const std::exception& ex) {
                       return JsonError(req.version(), "INVALID_JSON", ex.what(), ctx.request_id,
                                        boost::beast::http::status::bad_request);
                   }
               });

    router.Add("GET", "/v1/buckets",
               [metadata](const RequestContext& ctx, const HttpRequest& req, const RouteParams&) {
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
               [metadata](const RequestContext& ctx, const HttpRequest& req,
                          const RouteParams& params) {
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

    router.Add("POST", "/v1/buckets/{bucket}/multipart-uploads",
               [metadata](const RequestContext& ctx, const HttpRequest& req,
                          const RouteParams& params) {
                   const auto bucket = params.at("bucket");
                   if (!storage::LocalStorage::IsSafeName(bucket)) {
                       return JsonError(req.version(), "INVALID_NAME", "invalid bucket name",
                                        ctx.request_id,
                                        boost::beast::http::status::bad_request);
                   }
                   auto bucket_exists = metadata->GetBucket(bucket);
                   if (!bucket_exists.ok()) {
                       return JsonError(req.version(), "BUCKET_NOT_FOUND", "bucket not found",
                                        ctx.request_id, boost::beast::http::status::not_found);
                   }

                   try {
                       Poco::JSON::Parser parser;
                       auto result = parser.parse(req.body());
                       auto obj = result.extract<Poco::JSON::Object::Ptr>();
                       auto object_name = obj->getValue<std::string>("object");
                       if (!storage::LocalStorage::IsSafeName(object_name)) {
                           return JsonError(req.version(), "INVALID_NAME", "invalid object name",
                                            ctx.request_id,
                                            boost::beast::http::status::bad_request);
                       }

                       const auto upload_id = Poco::UUIDGenerator().createOne().toString();
                       const auto expires_at = core::NowIso8601();
                       auto created =
                           metadata->CreateMultipartUpload(bucket, upload_id, object_name, expires_at);
                       if (!created.ok()) {
                           return JsonError(req.version(), "DB_ERROR", created.error().message,
                                            ctx.request_id,
                                            boost::beast::http::status::internal_server_error);
                       }

                       return JsonOk(req.version(),
                                     "{\"upload_id\":\"" + upload_id + "\",\"object\":\"" +
                                         object_name + "\",\"expires_at\":\"" + expires_at + "\"}");
                   } catch (const std::exception& ex) {
                       return JsonError(req.version(), "INVALID_JSON", ex.what(), ctx.request_id,
                                        boost::beast::http::status::bad_request);
                   }
               });

    router.Add("PUT", "/v1/buckets/{bucket}/multipart-uploads/{upload_id}/parts/{part_number}",
               [metadata, storage](const RequestContext& ctx, const HttpRequest& req,
                                   const RouteParams& params) {
                   const auto bucket = params.at("bucket");
                   const auto upload_id = params.at("upload_id");
                   const auto part_number_text = params.at("part_number");
                   auto part_number = ParsePositiveInt(part_number_text);
                   if (!part_number) {
                       return JsonError(req.version(), "INVALID_PART_NUMBER",
                                        "part_number must be positive integer", ctx.request_id,
                                        boost::beast::http::status::bad_request);
                   }

                   auto bucket_result = metadata->GetBucket(bucket);
                   if (!bucket_result.ok()) {
                       return JsonError(req.version(), "BUCKET_NOT_FOUND", "bucket not found",
                                        ctx.request_id, boost::beast::http::status::not_found);
                   }

                   auto upload = metadata->GetMultipartUpload(upload_id);
                   if (!upload.ok()) {
                       return JsonError(req.version(), "UPLOAD_NOT_FOUND",
                                        "multipart upload not found", ctx.request_id,
                                        boost::beast::http::status::not_found);
                   }
                   if (upload.value().bucket_id != bucket_result.value().id) {
                       return JsonError(req.version(), "UPLOAD_NOT_FOUND",
                                        "multipart upload not found for bucket", ctx.request_id,
                                        boost::beast::http::status::not_found);
                   }
                   if (upload.value().state == "completed" || upload.value().state == "aborted" ||
                       upload.value().state == "expired") {
                       return JsonError(req.version(), "INVALID_STATE", "upload is not writable",
                                        ctx.request_id, boost::beast::http::status::conflict);
                   }

                   const auto part_path = MultipartPartPath(storage->temp_path(), upload_id, *part_number);
                   std::filesystem::create_directories(std::filesystem::path(part_path).parent_path());

                   try {
                       std::ofstream out(part_path, std::ios::binary | std::ios::trunc);
                       if (!out.is_open()) {
                           return JsonError(req.version(), "IO_ERROR", "failed to open part file",
                                            ctx.request_id,
                                            boost::beast::http::status::internal_server_error);
                       }
                       out.write(req.body().data(), static_cast<std::streamsize>(req.body().size()));
                       out.flush();

                       Poco::SHA2Engine256 sha256;
                       sha256.update(req.body().data(),
                                     static_cast<unsigned int>(req.body().size()));
                       const auto etag = Poco::DigestEngine::digestToHex(sha256.digest());

                       auto part = metadata->UpsertMultipartPart(
                           upload_id, *part_number, static_cast<std::uint64_t>(req.body().size()),
                           etag, part_path);
                       if (!part.ok()) {
                           return JsonError(req.version(), "DB_ERROR", part.error().message,
                                            ctx.request_id,
                                            boost::beast::http::status::internal_server_error);
                       }
                       auto state_update = metadata->UpdateMultipartUploadState(upload_id, "uploading");
                       if (!state_update.ok()) {
                           return JsonError(req.version(), "DB_ERROR", state_update.error().message,
                                            ctx.request_id,
                                            boost::beast::http::status::internal_server_error);
                       }

                       return JsonOk(req.version(),
                                     "{\"upload_id\":\"" + upload_id + "\",\"part_number\":" +
                                         std::to_string(*part_number) + ",\"etag\":\"" + etag +
                                         "\",\"size\":" + std::to_string(req.body().size()) + "}");
                   } catch (const std::exception& ex) {
                       return JsonError(req.version(), "IO_ERROR", ex.what(), ctx.request_id,
                                        boost::beast::http::status::internal_server_error);
                   }
               });

    router.Add("GET", "/v1/buckets/{bucket}/multipart-uploads/{upload_id}/parts",
               [metadata](const RequestContext& ctx, const HttpRequest& req,
                          const RouteParams& params) {
                   const auto bucket = params.at("bucket");
                   const auto upload_id = params.at("upload_id");

                   auto bucket_result = metadata->GetBucket(bucket);
                   if (!bucket_result.ok()) {
                       return JsonError(req.version(), "BUCKET_NOT_FOUND", "bucket not found",
                                        ctx.request_id, boost::beast::http::status::not_found);
                   }
                   auto upload = metadata->GetMultipartUpload(upload_id);
                   if (!upload.ok()) {
                       return JsonError(req.version(), "UPLOAD_NOT_FOUND",
                                        "multipart upload not found", ctx.request_id,
                                        boost::beast::http::status::not_found);
                   }
                   if (upload.value().bucket_id != bucket_result.value().id) {
                       return JsonError(req.version(), "UPLOAD_NOT_FOUND",
                                        "multipart upload not found for bucket", ctx.request_id,
                                        boost::beast::http::status::not_found);
                   }

                   auto parts = metadata->ListMultipartParts(upload_id);
                   if (!parts.ok()) {
                       return JsonError(req.version(), "DB_ERROR", parts.error().message,
                                        ctx.request_id,
                                        boost::beast::http::status::internal_server_error);
                   }

                   Poco::JSON::Array::Ptr arr = new Poco::JSON::Array();
                   for (const auto& part : parts.value()) {
                       Poco::JSON::Object::Ptr item = new Poco::JSON::Object();
                       item->set("part_number", part.part_number);
                       item->set("size", static_cast<Poco::UInt64>(part.size_bytes));
                       item->set("etag", part.etag);
                       arr->add(item);
                   }
                   Poco::JSON::Object::Ptr root = new Poco::JSON::Object();
                   root->set("upload_id", upload_id);
                   root->set("object", upload.value().object_name);
                   root->set("state", upload.value().state);
                   root->set("parts", arr);
                   std::stringstream ss;
                   root->stringify(ss);
                   return JsonOk(req.version(), ss.str());
               });

    router.Add("DELETE", "/v1/buckets/{bucket}/objects/{object}",
               [metadata, storage](const RequestContext& ctx, const HttpRequest& req,
                                   const RouteParams& params) {
                   const auto bucket = params.at("bucket");
                   const auto object = params.at("object");
                   auto storage_result = storage->DeleteObject(bucket, object);
                   if (!storage_result.ok()) {
                       return JsonError(req.version(), "OBJECT_NOT_FOUND", "object not found",
                                        ctx.request_id, boost::beast::http::status::not_found);
                   }
                   metadata->DeleteObject(bucket, object);
                   return JsonOk(req.version(), "{\"deleted\":true}");
               });
}

}  // namespace nebulafs::http
