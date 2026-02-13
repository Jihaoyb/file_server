#include "nebulafs/http/route_registration.h"

#include <array>
#include <filesystem>
#include <fstream>
#include <optional>
#include <sstream>
#include <string>
#include <cstdint>
#include <unordered_map>

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

std::filesystem::path MultipartUploadDir(const std::string& temp_root, const std::string& upload_id) {
    return std::filesystem::path(temp_root) / "multipart" / upload_id;
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

core::Result<void> ValidateUploadForBucket(metadata::MetadataStore* metadata,
                                           const std::string& bucket, const std::string& upload_id,
                                           int* bucket_id_out,
                                           metadata::MultipartUpload* upload_out) {
    auto bucket_result = metadata->GetBucket(bucket);
    if (!bucket_result.ok()) {
        return bucket_result.error();
    }
    auto upload = metadata->GetMultipartUpload(upload_id);
    if (!upload.ok()) {
        return upload.error();
    }
    if (upload.value().bucket_id != bucket_result.value().id) {
        return core::Error{core::ErrorCode::kNotFound, "multipart upload not found for bucket"};
    }

    *bucket_id_out = bucket_result.value().id;
    *upload_out = upload.value();
    return core::Ok();
}

struct CompletePart {
    int part_number{0};
    std::string etag;
};

core::Result<std::vector<CompletePart>> ParseCompleteParts(const std::string& body) {
    try {
        Poco::JSON::Parser parser;
        auto result = parser.parse(body);
        auto obj = result.extract<Poco::JSON::Object::Ptr>();
        auto parts = obj->getArray("parts");
        if (!parts || parts->empty()) {
            return core::Error{core::ErrorCode::kInvalidArgument, "parts list is required"};
        }

        std::vector<CompletePart> expected_parts;
        expected_parts.reserve(parts->size());
        int previous = 0;
        for (size_t i = 0; i < parts->size(); ++i) {
            auto part_obj = parts->getObject(i);
            if (!part_obj) {
                return core::Error{core::ErrorCode::kInvalidArgument, "invalid part entry"};
            }
            const int part_number = part_obj->getValue<int>("part_number");
            const std::string etag = part_obj->getValue<std::string>("etag");
            if (part_number <= 0 || etag.empty()) {
                return core::Error{core::ErrorCode::kInvalidArgument,
                                   "invalid part_number or etag"};
            }
            if (part_number <= previous) {
                return core::Error{core::ErrorCode::kInvalidArgument,
                                   "parts must be strictly increasing"};
            }
            previous = part_number;
            expected_parts.push_back(CompletePart{part_number, etag});
        }

        return expected_parts;
    } catch (const std::exception& ex) {
        return core::Error{core::ErrorCode::kInvalidArgument, ex.what()};
    }
}

}  // namespace

void RegisterDefaultRoutes(Router& router, std::shared_ptr<metadata::MetadataStore> metadata,
                           std::shared_ptr<storage::LocalStorage> storage,
                           const core::Config& config) {
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
               [metadata,
                ttl_seconds = config.storage.multipart.max_upload_ttl_seconds](
                   const RequestContext& ctx, const HttpRequest& req,
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
                       const auto expires_at =
                           core::NowIso8601WithOffsetSeconds(ttl_seconds);
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

                   int bucket_id = 0;
                   metadata::MultipartUpload upload;
                   auto validated =
                       ValidateUploadForBucket(metadata.get(), bucket, upload_id, &bucket_id, &upload);
                   if (!validated.ok()) {
                       const auto status =
                           validated.error().code == core::ErrorCode::kNotFound
                               ? boost::beast::http::status::not_found
                               : boost::beast::http::status::internal_server_error;
                       return JsonError(req.version(), "UPLOAD_NOT_FOUND",
                                        validated.error().message, ctx.request_id, status);
                   }
                   if (upload.state == "completed" || upload.state == "aborted" ||
                       upload.state == "expired") {
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

                   int bucket_id = 0;
                   metadata::MultipartUpload upload;
                   auto validated =
                       ValidateUploadForBucket(metadata.get(), bucket, upload_id, &bucket_id, &upload);
                   if (!validated.ok()) {
                       return JsonError(req.version(), "UPLOAD_NOT_FOUND",
                                        validated.error().message, ctx.request_id,
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
                   root->set("object", upload.object_name);
                   root->set("state", upload.state);
                   root->set("parts", arr);
                   std::stringstream ss;
                   root->stringify(ss);
                   return JsonOk(req.version(), ss.str());
               });

    router.Add("POST", "/v1/buckets/{bucket}/multipart-uploads/{upload_id}/complete",
               [metadata, storage](const RequestContext& ctx, const HttpRequest& req,
                                   const RouteParams& params) {
                   const auto bucket = params.at("bucket");
                   const auto upload_id = params.at("upload_id");

                   int bucket_id = 0;
                   metadata::MultipartUpload upload;
                   auto validated =
                       ValidateUploadForBucket(metadata.get(), bucket, upload_id, &bucket_id, &upload);
                   if (!validated.ok()) {
                       return JsonError(req.version(), "UPLOAD_NOT_FOUND",
                                        validated.error().message, ctx.request_id,
                                        boost::beast::http::status::not_found);
                   }
                   if (upload.state == "completed" || upload.state == "aborted" ||
                       upload.state == "expired") {
                       return JsonError(req.version(), "INVALID_STATE", "upload is not completable",
                                        ctx.request_id, boost::beast::http::status::conflict);
                   }

                   auto expected_parts = ParseCompleteParts(req.body());
                   if (!expected_parts.ok()) {
                       return JsonError(req.version(), "INVALID_JSON",
                                        expected_parts.error().message, ctx.request_id,
                                        boost::beast::http::status::bad_request);
                   }

                   auto listed_parts = metadata->ListMultipartParts(upload_id);
                   if (!listed_parts.ok()) {
                       return JsonError(req.version(), "DB_ERROR", listed_parts.error().message,
                                        ctx.request_id,
                                        boost::beast::http::status::internal_server_error);
                   }
                   if (listed_parts.value().empty()) {
                       return JsonError(req.version(), "INVALID_STATE", "no parts uploaded",
                                        ctx.request_id, boost::beast::http::status::conflict);
                   }

                   std::unordered_map<int, metadata::MultipartPart> part_map;
                   for (const auto& part : listed_parts.value()) {
                       part_map[part.part_number] = part;
                   }

                   const auto upload_dir = MultipartUploadDir(storage->temp_path(), upload_id);
                   const auto final_temp_path =
                       (upload_dir / ("complete-" + Poco::UUIDGenerator().createOne().toString()))
                           .string();
                   std::ofstream out(final_temp_path, std::ios::binary | std::ios::trunc);
                   if (!out.is_open()) {
                       return JsonError(req.version(), "IO_ERROR", "failed to open final temp file",
                                        ctx.request_id,
                                        boost::beast::http::status::internal_server_error);
                   }

                   Poco::SHA2Engine256 sha256;
                   std::uint64_t total_size = 0;
                   std::array<char, 8192> buffer{};
                   for (const auto& expected : expected_parts.value()) {
                       auto it = part_map.find(expected.part_number);
                       if (it == part_map.end()) {
                           std::filesystem::remove(final_temp_path);
                           return JsonError(req.version(), "MISSING_PART",
                                            "missing uploaded part " +
                                                std::to_string(expected.part_number),
                                            ctx.request_id,
                                            boost::beast::http::status::conflict);
                       }
                       if (it->second.etag != expected.etag) {
                           std::filesystem::remove(final_temp_path);
                           return JsonError(req.version(), "ETAG_MISMATCH",
                                            "part etag mismatch for part " +
                                                std::to_string(expected.part_number),
                                            ctx.request_id,
                                            boost::beast::http::status::conflict);
                       }

                       std::ifstream in(it->second.temp_path, std::ios::binary);
                       if (!in.is_open()) {
                           std::filesystem::remove(final_temp_path);
                           return JsonError(req.version(), "IO_ERROR",
                                            "failed to read uploaded part", ctx.request_id,
                                            boost::beast::http::status::internal_server_error);
                       }
                       while (in) {
                           in.read(buffer.data(), static_cast<std::streamsize>(buffer.size()));
                           const auto bytes = in.gcount();
                           if (bytes <= 0) {
                               break;
                           }
                           out.write(buffer.data(), bytes);
                           sha256.update(buffer.data(), static_cast<unsigned int>(bytes));
                           total_size += static_cast<std::uint64_t>(bytes);
                       }
                   }
                   out.flush();
                   out.close();

                   const auto final_path = storage::LocalStorage::BuildObjectPath(
                       storage->base_path(), bucket, upload.object_name);
                   std::filesystem::create_directories(
                       std::filesystem::path(final_path).parent_path());
                   std::filesystem::rename(final_temp_path, final_path);

                   metadata::ObjectMetadata object_meta;
                   object_meta.name = upload.object_name;
                   object_meta.size_bytes = total_size;
                   object_meta.etag = Poco::DigestEngine::digestToHex(sha256.digest());
                   auto upsert = metadata->UpsertObject(bucket, object_meta);
                   if (!upsert.ok()) {
                       return JsonError(req.version(), "DB_ERROR", upsert.error().message,
                                        ctx.request_id,
                                        boost::beast::http::status::internal_server_error);
                   }

                   (void)metadata->UpdateMultipartUploadState(upload_id, "completed");
                   (void)metadata->DeleteMultipartParts(upload_id);
                   (void)metadata->DeleteMultipartUpload(upload_id);
                   std::error_code ec;
                   std::filesystem::remove_all(upload_dir, ec);

                   return JsonOk(req.version(),
                                 "{\"name\":\"" + object_meta.name + "\",\"etag\":\"" +
                                     object_meta.etag + "\",\"size\":" +
                                     std::to_string(object_meta.size_bytes) + "}");
               });

    router.Add("DELETE", "/v1/buckets/{bucket}/multipart-uploads/{upload_id}",
               [metadata, storage](const RequestContext& ctx, const HttpRequest& req,
                                   const RouteParams& params) {
                   const auto bucket = params.at("bucket");
                   const auto upload_id = params.at("upload_id");

                   int bucket_id = 0;
                   metadata::MultipartUpload upload;
                   auto validated =
                       ValidateUploadForBucket(metadata.get(), bucket, upload_id, &bucket_id, &upload);
                   if (!validated.ok()) {
                       return JsonError(req.version(), "UPLOAD_NOT_FOUND",
                                        validated.error().message, ctx.request_id,
                                        boost::beast::http::status::not_found);
                   }
                   if (upload.state == "completed") {
                       return JsonError(req.version(), "INVALID_STATE", "completed upload cannot abort",
                                        ctx.request_id, boost::beast::http::status::conflict);
                   }

                   (void)metadata->UpdateMultipartUploadState(upload_id, "aborted");
                   (void)metadata->DeleteMultipartParts(upload_id);
                   (void)metadata->DeleteMultipartUpload(upload_id);
                   std::error_code ec;
                   std::filesystem::remove_all(MultipartUploadDir(storage->temp_path(), upload_id), ec);

                   HttpResponse response{boost::beast::http::status::no_content, req.version()};
                   return response;
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
