#pragma once

#include <cstdint>
#include <string>

#include <Poco/Data/Session.h>

#include "nebulafs/metadata/metadata_store.h"

namespace nebulafs::metadata {

/// @brief SQLite-backed metadata store for single-node mode.
class SqliteMetadataStore : public MetadataStore {
public:
    explicit SqliteMetadataStore(const std::string& db_path);

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

private:
    // Schema creation is done once per store instance; in production this will be migrated.
    void InitSchema();
    Poco::Data::Session session_;
};

}  // namespace nebulafs::metadata
