#pragma once

#include <cstdint>
#include <istream>
#include <string>

#include "nebulafs/core/result.h"

namespace nebulafs::storage {

/// @brief Stored object attributes used for metadata updates.
struct StoredObject {
    std::string path;
    std::string etag;
    std::uint64_t size_bytes{0};
};

/// @brief Abstract byte-storage backend used by HTTP handlers.
class StorageBackend {
public:
    virtual ~StorageBackend() = default;

    virtual core::Result<StoredObject> WriteObject(const std::string& bucket,
                                                   const std::string& object,
                                                   std::istream& data) = 0;
    virtual core::Result<StoredObject> ReadObject(const std::string& bucket,
                                                  const std::string& object) const = 0;
    virtual core::Result<void> DeleteObject(const std::string& bucket,
                                            const std::string& object) = 0;
    virtual core::Result<void> EnsureBucket(const std::string& bucket) = 0;

    virtual const std::string& base_path() const = 0;
    virtual const std::string& temp_path() const = 0;
};

}  // namespace nebulafs::storage
