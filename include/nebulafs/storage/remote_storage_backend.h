#pragma once

#include <memory>
#include <string>

#include "nebulafs/core/config.h"
#include "nebulafs/metadata/metadata_backend.h"
#include "nebulafs/storage/storage_backend.h"

namespace nebulafs::storage {

/// @brief Distributed storage backend using metadata placement and storage-node APIs.
class RemoteStorageBackend : public StorageBackend {
public:
    RemoteStorageBackend(core::DistributedConfig distributed,
                         std::shared_ptr<metadata::MetadataBackend> metadata,
                         std::string temp_path);

    core::Result<StoredObject> WriteObject(const std::string& bucket, const std::string& object,
                                           std::istream& data) override;
    core::Result<StoredObject> ReadObject(const std::string& bucket,
                                          const std::string& object) const override;
    core::Result<void> DeleteObject(const std::string& bucket,
                                    const std::string& object) override;
    core::Result<void> EnsureBucket(const std::string& bucket) override;

    const std::string& base_path() const override { return base_path_placeholder_; }
    const std::string& temp_path() const override { return temp_path_; }

private:
    core::DistributedConfig distributed_;
    std::shared_ptr<metadata::MetadataBackend> metadata_;
    std::string temp_path_;
    std::string base_path_placeholder_{"distributed"};
};

}  // namespace nebulafs::storage
