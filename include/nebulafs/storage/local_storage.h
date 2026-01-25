#pragma once

#include <cstdint>
#include <string>

#include "nebulafs/core/error.h"
#include "nebulafs/core/result.h"

namespace nebulafs::storage {

/// @brief Stored object attributes used for metadata updates.
struct StoredObject {
    std::string path;
    std::string etag;
    std::uint64_t size_bytes{0};
};

/// @brief Local filesystem storage with atomic writes.
class LocalStorage {
public:
    LocalStorage(std::string base_path, std::string temp_path);

    core::Result<StoredObject> WriteObject(const std::string& bucket, const std::string& object,
                                           std::istream& data);
    core::Result<StoredObject> ReadObject(const std::string& bucket, const std::string& object) const;
    core::Result<void> DeleteObject(const std::string& bucket, const std::string& object);

    core::Result<void> EnsureBucket(const std::string& bucket);

    const std::string& base_path() const { return base_path_; }
    const std::string& temp_path() const { return temp_path_; }

    static bool IsSafeName(const std::string& name);
    static std::string BuildObjectPath(const std::string& base_path, const std::string& bucket,
                                       const std::string& object);

private:
    std::string base_path_;
    std::string temp_path_;
};

}  // namespace nebulafs::storage
