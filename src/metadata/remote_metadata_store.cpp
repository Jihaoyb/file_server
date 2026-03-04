#include "nebulafs/metadata/remote_metadata_store.h"

#include <cstdint>
#include <sstream>
#include <string>

#include <Poco/JSON/Array.h>
#include <Poco/JSON/Object.h>
#include <Poco/JSON/Parser.h>
#include <Poco/URI.h>

#include "nebulafs/distributed/http_client.h"

// Poco pulls in Windows headers on win32, which define GetObject as a macro.
// Remove the macro before member definitions to avoid GetObject->GetObjectW rewrite.
#ifdef GetObject
#undef GetObject
#endif

namespace nebulafs::metadata {
namespace {

std::string JoinUrl(const std::string& base_url, const std::string& path_and_query) {
    if (!base_url.empty() && base_url.back() == '/') {
        if (!path_and_query.empty() && path_and_query.front() == '/') {
            return base_url.substr(0, base_url.size() - 1) + path_and_query;
        }
        return base_url + path_and_query;
    }
    if (!path_and_query.empty() && path_and_query.front() == '/') {
        return base_url + path_and_query;
    }
    return base_url + "/" + path_and_query;
}

core::Error HttpError(const std::string& message) {
    return core::Error{core::ErrorCode::kInternal, message};
}

template <typename Builder>
core::Result<distributed::HttpCallResult> CallJson(const std::string& method, const std::string& url,
                                                   const std::string& token, Builder&& build_body) {
    Poco::JSON::Object::Ptr root = new Poco::JSON::Object();
    build_body(root);
    std::ostringstream ss;
    root->stringify(ss);
    return distributed::SendHttpRequest(method, url, ss.str(), "application/json", token, {});
}

core::Result<Poco::JSON::Object::Ptr> ParseObject(const distributed::HttpCallResult& response) {
    try {
        Poco::JSON::Parser parser;
        auto v = parser.parse(response.body);
        return v.extract<Poco::JSON::Object::Ptr>();
    } catch (const std::exception& ex) {
        return core::Error{core::ErrorCode::kInternal, ex.what()};
    }
}

}  // namespace

RemoteMetadataStore::RemoteMetadataStore(std::string base_url, std::string service_auth_token)
    : base_url_(std::move(base_url)),
      service_auth_token_(std::move(service_auth_token)) {
}

core::Result<Bucket> RemoteMetadataStore::CreateBucket(const std::string& name) {
    auto call = CallJson("POST", JoinUrl(base_url_, "/internal/v1/buckets/create"),
                         service_auth_token_,
                         [&](Poco::JSON::Object::Ptr root) { root->set("name", name); });
    if (!call.ok()) {
        return call.error();
    }
    if (call.value().status != 200) {
        return HttpError("create bucket failed: " + call.value().body);
    }
    auto parsed = ParseObject(call.value());
    if (!parsed.ok()) {
        return parsed.error();
    }
    Bucket bucket;
    bucket.id = parsed.value()->getValue<int>("id");
    bucket.name = parsed.value()->getValue<std::string>("name");
    bucket.created_at = parsed.value()->getValue<std::string>("created_at");
    return bucket;
}

core::Result<std::vector<Bucket>> RemoteMetadataStore::ListBuckets() {
    auto call = distributed::SendHttpRequest("GET", JoinUrl(base_url_, "/internal/v1/buckets/list"),
                                             "", "", service_auth_token_, {});
    if (!call.ok()) {
        return call.error();
    }
    if (call.value().status != 200) {
        return HttpError("list buckets failed: " + call.value().body);
    }
    auto parsed = ParseObject(call.value());
    if (!parsed.ok()) {
        return parsed.error();
    }
    std::vector<Bucket> buckets;
    auto arr = parsed.value()->getArray("buckets");
    for (size_t i = 0; i < arr->size(); ++i) {
        auto item = arr->getObject(i);
        buckets.push_back(Bucket{item->getValue<int>("id"), item->getValue<std::string>("name"),
                                 item->getValue<std::string>("created_at")});
    }
    return buckets;
}

core::Result<Bucket> RemoteMetadataStore::GetBucket(const std::string& name) {
    std::string encoded;
    Poco::URI::encode(name, "", encoded);
    auto call = distributed::SendHttpRequest(
        "GET", JoinUrl(base_url_, "/internal/v1/buckets/get?name=" + encoded), "", "",
        service_auth_token_, {});
    if (!call.ok()) {
        return call.error();
    }
    if (call.value().status == 404) {
        return core::Error{core::ErrorCode::kNotFound, "bucket not found"};
    }
    if (call.value().status != 200) {
        return HttpError("get bucket failed: " + call.value().body);
    }
    auto parsed = ParseObject(call.value());
    if (!parsed.ok()) {
        return parsed.error();
    }
    Bucket bucket;
    bucket.id = parsed.value()->getValue<int>("id");
    bucket.name = parsed.value()->getValue<std::string>("name");
    bucket.created_at = parsed.value()->getValue<std::string>("created_at");
    return bucket;
}

core::Result<ObjectMetadata> RemoteMetadataStore::UpsertObject(const std::string& bucket,
                                                               const ObjectMetadata& object) {
    auto call = CallJson("POST", JoinUrl(base_url_, "/internal/v1/objects/upsert"),
                         service_auth_token_, [&](Poco::JSON::Object::Ptr root) {
                             root->set("bucket", bucket);
                             root->set("name", object.name);
                             root->set("size_bytes", static_cast<Poco::UInt64>(object.size_bytes));
                             root->set("etag", object.etag);
                         });
    if (!call.ok()) {
        return call.error();
    }
    if (call.value().status != 200) {
        return HttpError("upsert object failed: " + call.value().body);
    }
    auto parsed = ParseObject(call.value());
    if (!parsed.ok()) {
        return parsed.error();
    }
    ObjectMetadata meta;
    meta.id = parsed.value()->getValue<int>("id");
    meta.bucket_id = parsed.value()->getValue<int>("bucket_id");
    meta.name = parsed.value()->getValue<std::string>("name");
    meta.size_bytes = parsed.value()->getValue<Poco::UInt64>("size_bytes");
    meta.etag = parsed.value()->getValue<std::string>("etag");
    meta.created_at = parsed.value()->getValue<std::string>("created_at");
    meta.updated_at = parsed.value()->getValue<std::string>("updated_at");
    return meta;
}

core::Result<ObjectMetadata> RemoteMetadataStore::GetObject(const std::string& bucket,
                                                            const std::string& object_name) {
    std::string bucket_enc;
    std::string object_enc;
    Poco::URI::encode(bucket, "", bucket_enc);
    Poco::URI::encode(object_name, "", object_enc);
    auto call = distributed::SendHttpRequest(
        "GET", JoinUrl(base_url_, "/internal/v1/objects/get?bucket=" + bucket_enc + "&object=" +
                                      object_enc),
        "", "", service_auth_token_, {});
    if (!call.ok()) {
        return call.error();
    }
    if (call.value().status == 404) {
        return core::Error{core::ErrorCode::kNotFound, "object not found"};
    }
    if (call.value().status != 200) {
        return HttpError("get object failed: " + call.value().body);
    }
    auto parsed = ParseObject(call.value());
    if (!parsed.ok()) {
        return parsed.error();
    }
    ObjectMetadata meta;
    meta.id = parsed.value()->getValue<int>("id");
    meta.bucket_id = parsed.value()->getValue<int>("bucket_id");
    meta.name = parsed.value()->getValue<std::string>("name");
    meta.size_bytes = parsed.value()->getValue<Poco::UInt64>("size_bytes");
    meta.etag = parsed.value()->getValue<std::string>("etag");
    meta.created_at = parsed.value()->getValue<std::string>("created_at");
    meta.updated_at = parsed.value()->getValue<std::string>("updated_at");
    return meta;
}

core::Result<std::vector<ObjectMetadata>> RemoteMetadataStore::ListObjects(
    const std::string& bucket, const std::string& prefix) {
    std::string bucket_enc;
    std::string prefix_enc;
    Poco::URI::encode(bucket, "", bucket_enc);
    Poco::URI::encode(prefix, "", prefix_enc);
    auto call = distributed::SendHttpRequest(
        "GET", JoinUrl(base_url_, "/internal/v1/objects/list?bucket=" + bucket_enc + "&prefix=" +
                                      prefix_enc),
        "", "", service_auth_token_, {});
    if (!call.ok()) {
        return call.error();
    }
    if (call.value().status != 200) {
        return HttpError("list objects failed: " + call.value().body);
    }
    auto parsed = ParseObject(call.value());
    if (!parsed.ok()) {
        return parsed.error();
    }
    std::vector<ObjectMetadata> objects;
    auto arr = parsed.value()->getArray("objects");
    for (size_t i = 0; i < arr->size(); ++i) {
        auto item = arr->getObject(i);
        ObjectMetadata meta;
        meta.id = item->getValue<int>("id");
        meta.bucket_id = item->getValue<int>("bucket_id");
        meta.name = item->getValue<std::string>("name");
        meta.size_bytes = item->getValue<Poco::UInt64>("size_bytes");
        meta.etag = item->getValue<std::string>("etag");
        meta.created_at = item->getValue<std::string>("created_at");
        meta.updated_at = item->getValue<std::string>("updated_at");
        objects.push_back(meta);
    }
    return objects;
}

core::Result<void> RemoteMetadataStore::DeleteObject(const std::string& bucket,
                                                     const std::string& object_name) {
    std::string bucket_enc;
    std::string object_enc;
    Poco::URI::encode(bucket, "", bucket_enc);
    Poco::URI::encode(object_name, "", object_enc);
    auto call = distributed::SendHttpRequest(
        "DELETE", JoinUrl(base_url_, "/internal/v1/objects/delete?bucket=" + bucket_enc +
                                         "&object=" + object_enc),
        "", "", service_auth_token_, {});
    if (!call.ok()) {
        return call.error();
    }
    if (call.value().status != 200 && call.value().status != 404) {
        return HttpError("delete object failed: " + call.value().body);
    }
    return core::Ok();
}

core::Result<MultipartUpload> RemoteMetadataStore::CreateMultipartUpload(
    const std::string&, const std::string&, const std::string&, const std::string&) {
    return core::Error{core::ErrorCode::kInternal, "multipart not supported in distributed mode"};
}

core::Result<MultipartUpload> RemoteMetadataStore::GetMultipartUpload(const std::string&) {
    return core::Error{core::ErrorCode::kInternal, "multipart not supported in distributed mode"};
}

core::Result<std::vector<MultipartUpload>> RemoteMetadataStore::ListExpiredMultipartUploads(
    const std::string&, int) {
    return core::Error{core::ErrorCode::kInternal, "multipart not supported in distributed mode"};
}

core::Result<void> RemoteMetadataStore::UpdateMultipartUploadState(const std::string&,
                                                                   const std::string&) {
    return core::Error{core::ErrorCode::kInternal, "multipart not supported in distributed mode"};
}

core::Result<void> RemoteMetadataStore::DeleteMultipartUpload(const std::string&) {
    return core::Error{core::ErrorCode::kInternal, "multipart not supported in distributed mode"};
}

core::Result<MultipartPart> RemoteMetadataStore::UpsertMultipartPart(const std::string&, int,
                                                                     std::uint64_t,
                                                                     const std::string&,
                                                                     const std::string&) {
    return core::Error{core::ErrorCode::kInternal, "multipart not supported in distributed mode"};
}

core::Result<std::vector<MultipartPart>> RemoteMetadataStore::ListMultipartParts(const std::string&) {
    return core::Error{core::ErrorCode::kInternal, "multipart not supported in distributed mode"};
}

core::Result<void> RemoteMetadataStore::DeleteMultipartParts(const std::string&) {
    return core::Error{core::ErrorCode::kInternal, "multipart not supported in distributed mode"};
}

core::Result<void> RemoteMetadataStore::ConfigureStorageNodes(
    const std::vector<std::string>& endpoints) {
    auto call = CallJson("POST", JoinUrl(base_url_, "/internal/v1/storage-nodes/configure"),
                         service_auth_token_, [&](Poco::JSON::Object::Ptr root) {
                             Poco::JSON::Array::Ptr arr = new Poco::JSON::Array();
                             for (const auto& endpoint : endpoints) {
                                 arr->add(endpoint);
                             }
                             root->set("endpoints", arr);
                         });
    if (!call.ok()) {
        return call.error();
    }
    if (call.value().status != 200) {
        return HttpError("configure storage nodes failed: " + call.value().body);
    }
    return core::Ok();
}

core::Result<AllocateWritePlan> RemoteMetadataStore::AllocateWrite(const std::string& bucket,
                                                                   const std::string& object_name,
                                                                   int replication_factor,
                                                                   const std::string& service_token) {
    auto call = CallJson("POST", JoinUrl(base_url_, "/internal/v1/objects/allocate-write"),
                         service_auth_token_, [&](Poco::JSON::Object::Ptr root) {
                             root->set("bucket", bucket);
                             root->set("object", object_name);
                             root->set("replication_factor", replication_factor);
                             root->set("service_token", service_token);
                         });
    if (!call.ok()) {
        return call.error();
    }
    if (call.value().status != 200) {
        return HttpError("allocate write failed: " + call.value().body);
    }
    auto parsed = ParseObject(call.value());
    if (!parsed.ok()) {
        return parsed.error();
    }
    AllocateWritePlan plan;
    plan.blob_id = parsed.value()->getValue<std::string>("blob_id");
    plan.write_token = parsed.value()->getValue<std::string>("write_token");
    auto replicas = parsed.value()->getArray("replicas");
    for (size_t i = 0; i < replicas->size(); ++i) {
        auto replica = replicas->getObject(i);
        plan.replicas.push_back(
            ReplicaTarget{replica->getValue<int>("node_id"), replica->getValue<int>("replica_index"),
                          replica->getValue<std::string>("endpoint")});
    }
    return plan;
}

core::Result<void> RemoteMetadataStore::CommitWrite(const std::string& bucket,
                                                    const std::string& object_name,
                                                    const std::string& blob_id,
                                                    std::uint64_t size_bytes,
                                                    const std::string& etag,
                                                    const std::vector<ReplicaTarget>& replicas) {
    auto call = CallJson("POST", JoinUrl(base_url_, "/internal/v1/objects/commit"),
                         service_auth_token_, [&](Poco::JSON::Object::Ptr root) {
                             root->set("bucket", bucket);
                             root->set("object", object_name);
                             root->set("blob_id", blob_id);
                             root->set("size_bytes", static_cast<Poco::UInt64>(size_bytes));
                             root->set("etag", etag);
                             Poco::JSON::Array::Ptr arr = new Poco::JSON::Array();
                             for (const auto& replica : replicas) {
                                 Poco::JSON::Object::Ptr item = new Poco::JSON::Object();
                                 item->set("node_id", replica.node_id);
                                 item->set("replica_index", replica.replica_index);
                                 item->set("endpoint", replica.endpoint);
                                 arr->add(item);
                             }
                             root->set("replicas", arr);
                         });
    if (!call.ok()) {
        return call.error();
    }
    if (call.value().status != 200) {
        return HttpError("commit write failed: " + call.value().body);
    }
    return core::Ok();
}

core::Result<ResolveReadPlan> RemoteMetadataStore::ResolveRead(const std::string& bucket,
                                                               const std::string& object_name) {
    std::string bucket_enc;
    std::string object_enc;
    Poco::URI::encode(bucket, "", bucket_enc);
    Poco::URI::encode(object_name, "", object_enc);
    auto call = distributed::SendHttpRequest(
        "GET", JoinUrl(base_url_, "/internal/v1/objects/resolve-read?bucket=" + bucket_enc +
                                      "&object=" + object_enc),
        "", "", service_auth_token_, {});
    if (!call.ok()) {
        return call.error();
    }
    if (call.value().status == 404) {
        return core::Error{core::ErrorCode::kNotFound, "object not found"};
    }
    if (call.value().status != 200) {
        return HttpError("resolve read failed: " + call.value().body);
    }
    auto parsed = ParseObject(call.value());
    if (!parsed.ok()) {
        return parsed.error();
    }
    ResolveReadPlan plan;
    plan.blob_id = parsed.value()->getValue<std::string>("blob_id");
    plan.etag = parsed.value()->getValue<std::string>("etag");
    plan.size_bytes = parsed.value()->getValue<Poco::UInt64>("size_bytes");
    auto replicas = parsed.value()->getArray("replicas");
    for (size_t i = 0; i < replicas->size(); ++i) {
        auto replica = replicas->getObject(i);
        plan.replicas.push_back(
            ReplicaTarget{replica->getValue<int>("node_id"), replica->getValue<int>("replica_index"),
                          replica->getValue<std::string>("endpoint")});
    }
    return plan;
}

}  // namespace nebulafs::metadata
