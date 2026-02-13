#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include "nebulafs/core/error.h"
#include "nebulafs/core/result.h"

namespace nebulafs::metadata {

/// @brief Bucket metadata record.
struct Bucket {
    int id{0};
    std::string name;
    std::string created_at;
};

/// @brief Object metadata record stored in the DB.
struct ObjectMetadata {
    int id{0};
    int bucket_id{0};
    std::string name;
    std::uint64_t size_bytes{0};
    std::string etag;
    std::string created_at;
    std::string updated_at;
};

/// @brief In-progress multipart upload metadata.
struct MultipartUpload {
    int id{0};
    std::string upload_id;
    int bucket_id{0};
    std::string object_name;
    std::string state;
    std::string expires_at;
    std::string created_at;
    std::string updated_at;
};

/// @brief Metadata for a single uploaded part.
struct MultipartPart {
    int id{0};
    std::string upload_id;
    int part_number{0};
    std::uint64_t size_bytes{0};
    std::string etag;
    std::string temp_path;
    std::string created_at;
};

/// @brief Abstract metadata store interface for buckets and objects.
class MetadataStore {
public:
    virtual ~MetadataStore() = default;

    virtual core::Result<Bucket> CreateBucket(const std::string& name) = 0;
    virtual core::Result<std::vector<Bucket>> ListBuckets() = 0;
    virtual core::Result<Bucket> GetBucket(const std::string& name) = 0;

    virtual core::Result<ObjectMetadata> UpsertObject(const std::string& bucket,
                                                      const ObjectMetadata& object) = 0;
    virtual core::Result<ObjectMetadata> GetObject(const std::string& bucket,
                                                   const std::string& object) = 0;
    virtual core::Result<std::vector<ObjectMetadata>> ListObjects(const std::string& bucket,
                                                                   const std::string& prefix) = 0;
    virtual core::Result<void> DeleteObject(const std::string& bucket,
                                            const std::string& object) = 0;

    virtual core::Result<MultipartUpload> CreateMultipartUpload(const std::string& bucket,
                                                                const std::string& upload_id,
                                                                const std::string& object_name,
                                                                const std::string& expires_at) = 0;
    virtual core::Result<MultipartUpload> GetMultipartUpload(const std::string& upload_id) = 0;
    virtual core::Result<std::vector<MultipartUpload>> ListExpiredMultipartUploads(
        const std::string& expires_before, int limit) = 0;
    virtual core::Result<void> UpdateMultipartUploadState(const std::string& upload_id,
                                                          const std::string& state) = 0;
    virtual core::Result<void> DeleteMultipartUpload(const std::string& upload_id) = 0;

    virtual core::Result<MultipartPart> UpsertMultipartPart(const std::string& upload_id,
                                                            int part_number,
                                                            std::uint64_t size_bytes,
                                                            const std::string& etag,
                                                            const std::string& temp_path) = 0;
    virtual core::Result<std::vector<MultipartPart>> ListMultipartParts(
        const std::string& upload_id) = 0;
    virtual core::Result<void> DeleteMultipartParts(const std::string& upload_id) = 0;
};

}  // namespace nebulafs::metadata
