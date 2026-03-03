#pragma once

#include <string>

#include "nebulafs/metadata/metadata_store.h"

namespace nebulafs::metadata {

/// @brief HTTP-backed metadata store client for distributed mode.
class RemoteMetadataStore : public MetadataStore {
public:
    RemoteMetadataStore(std::string base_url, std::string service_auth_token);

    core::Result<Bucket> CreateBucket(const std::string& name) override;
    core::Result<std::vector<Bucket>> ListBuckets() override;
    core::Result<Bucket> GetBucket(const std::string& name) override;

    core::Result<ObjectMetadata> UpsertObject(const std::string& bucket,
                                              const ObjectMetadata& object) override;
    core::Result<ObjectMetadata> GetObject(const std::string& bucket,
                                           const std::string& object) override;
    core::Result<std::vector<ObjectMetadata>> ListObjects(const std::string& bucket,
                                                          const std::string& prefix) override;
    core::Result<void> DeleteObject(const std::string& bucket,
                                    const std::string& object) override;

    core::Result<MultipartUpload> CreateMultipartUpload(const std::string& bucket,
                                                        const std::string& upload_id,
                                                        const std::string& object_name,
                                                        const std::string& expires_at) override;
    core::Result<MultipartUpload> GetMultipartUpload(const std::string& upload_id) override;
    core::Result<std::vector<MultipartUpload>> ListExpiredMultipartUploads(
        const std::string& expires_before, int limit) override;
    core::Result<void> UpdateMultipartUploadState(const std::string& upload_id,
                                                  const std::string& state) override;
    core::Result<void> DeleteMultipartUpload(const std::string& upload_id) override;
    core::Result<MultipartPart> UpsertMultipartPart(const std::string& upload_id,
                                                    int part_number,
                                                    std::uint64_t size_bytes,
                                                    const std::string& etag,
                                                    const std::string& temp_path) override;
    core::Result<std::vector<MultipartPart>> ListMultipartParts(
        const std::string& upload_id) override;
    core::Result<void> DeleteMultipartParts(const std::string& upload_id) override;

    core::Result<void> ConfigureStorageNodes(
        const std::vector<std::string>& endpoints) override;
    core::Result<AllocateWritePlan> AllocateWrite(const std::string& bucket,
                                                  const std::string& object_name,
                                                  int replication_factor,
                                                  const std::string& service_token) override;
    core::Result<void> CommitWrite(const std::string& bucket,
                                   const std::string& object_name,
                                   const std::string& blob_id,
                                   std::uint64_t size_bytes,
                                   const std::string& etag,
                                   const std::vector<ReplicaTarget>& replicas) override;
    core::Result<ResolveReadPlan> ResolveRead(const std::string& bucket,
                                              const std::string& object_name) override;

private:
    std::string base_url_;
    std::string service_auth_token_;
};

}  // namespace nebulafs::metadata
