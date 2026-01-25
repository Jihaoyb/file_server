#pragma once

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
};

}  // namespace nebulafs::metadata
