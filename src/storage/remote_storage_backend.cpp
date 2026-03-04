#include "nebulafs/storage/remote_storage_backend.h"

#include <array>
#include <filesystem>
#include <fstream>
#include <sstream>

#include <Poco/DigestEngine.h>
#include <Poco/SHA2Engine.h>
#include <Poco/UUIDGenerator.h>

#include "nebulafs/distributed/http_client.h"
#include "nebulafs/observability/metrics.h"

namespace nebulafs::storage {
namespace {

std::string BlobUrl(const std::string& endpoint, const std::string& blob_id) {
    if (!endpoint.empty() && endpoint.back() == '/') {
        return endpoint.substr(0, endpoint.size() - 1) + "/internal/v1/blobs/" + blob_id;
    }
    return endpoint + "/internal/v1/blobs/" + blob_id;
}

}  // namespace

RemoteStorageBackend::RemoteStorageBackend(core::DistributedConfig distributed,
                                           std::shared_ptr<metadata::MetadataBackend> metadata,
                                           std::string temp_path)
    : distributed_(std::move(distributed)),
      metadata_(std::move(metadata)),
      temp_path_(std::move(temp_path)) {
    std::filesystem::create_directories(std::filesystem::path(temp_path_) / "remote_cache");
}

core::Result<StoredObject> RemoteStorageBackend::WriteObject(const std::string& bucket,
                                                             const std::string& object,
                                                             std::istream& data) {
    std::string payload;
    payload.reserve(64 * 1024);
    std::array<char, 8192> buffer{};
    Poco::SHA2Engine256 sha256;
    std::uint64_t total = 0;
    while (data) {
        data.read(buffer.data(), buffer.size());
        const auto bytes = data.gcount();
        if (bytes <= 0) {
            break;
        }
        payload.append(buffer.data(), static_cast<size_t>(bytes));
        sha256.update(buffer.data(), static_cast<unsigned int>(bytes));
        total += static_cast<std::uint64_t>(bytes);
    }
    const auto etag = Poco::DigestEngine::digestToHex(sha256.digest());

    auto allocation = metadata_->AllocateWrite(bucket, object, distributed_.replication_factor,
                                               distributed_.service_auth_token);
    if (!allocation.ok()) {
        nebulafs::observability::RecordGatewayMetadataRpcFailure();
        return allocation.error();
    }

    std::vector<metadata::ReplicaTarget> written;
    for (const auto& replica : allocation.value().replicas) {
        auto put = distributed::SendHttpRequest(
            "PUT", BlobUrl(replica.endpoint, allocation.value().blob_id), payload,
            "application/octet-stream", distributed_.service_auth_token,
            {{"X-Placement-Token", allocation.value().write_token}});
        if (put.ok() && put.value().status == 200) {
            written.push_back(replica);
        } else {
            nebulafs::observability::RecordGatewayStoragePutFailure();
        }
    }
    if (static_cast<int>(written.size()) < distributed_.min_write_acks) {
        return core::Error{core::ErrorCode::kIoError, "insufficient storage node write acknowledgements"};
    }

    auto commit =
        metadata_->CommitWrite(bucket, object, allocation.value().blob_id, total, etag, written);
    if (!commit.ok()) {
        nebulafs::observability::RecordGatewayMetadataRpcFailure();
        return commit.error();
    }

    StoredObject stored;
    stored.size_bytes = total;
    stored.etag = etag;
    return stored;
}

core::Result<StoredObject> RemoteStorageBackend::ReadObject(const std::string& bucket,
                                                            const std::string& object) const {
    auto plan = metadata_->ResolveRead(bucket, object);
    if (!plan.ok()) {
        nebulafs::observability::RecordGatewayMetadataRpcFailure();
        return plan.error();
    }

    bool first_attempt = true;
    for (const auto& replica : plan.value().replicas) {
        auto get = distributed::SendHttpRequest("GET", BlobUrl(replica.endpoint, plan.value().blob_id),
                                                "", "", distributed_.service_auth_token, {});
        if (!get.ok() || get.value().status != 200) {
            if (first_attempt) {
                nebulafs::observability::RecordGatewayReplicaFallback();
                first_attempt = false;
            }
            continue;
        }
        const auto cache_path = (std::filesystem::path(temp_path_) / "remote_cache" /
                                 Poco::UUIDGenerator().createOne().toString())
                                    .string();
        std::ofstream out(cache_path, std::ios::binary | std::ios::trunc);
        out.write(get.value().body.data(), static_cast<std::streamsize>(get.value().body.size()));
        out.close();

        StoredObject stored;
        stored.path = cache_path;
        stored.etag = plan.value().etag;
        stored.size_bytes = static_cast<std::uint64_t>(get.value().body.size());
        return stored;
    }
    return core::Error{core::ErrorCode::kNotFound, "object not found on storage nodes"};
}

core::Result<void> RemoteStorageBackend::DeleteObject(const std::string& bucket,
                                                      const std::string& object) {
    auto plan = metadata_->ResolveRead(bucket, object);
    if (!plan.ok()) {
        // Keep delete idempotent.
        return core::Ok();
    }
    for (const auto& replica : plan.value().replicas) {
        (void)distributed::SendHttpRequest("DELETE",
                                           BlobUrl(replica.endpoint, plan.value().blob_id), "", "",
                                           distributed_.service_auth_token, {});
    }
    return core::Ok();
}

core::Result<void> RemoteStorageBackend::EnsureBucket(const std::string&) { return core::Ok(); }

}  // namespace nebulafs::storage
