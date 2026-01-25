#pragma once

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

private:
    // Schema creation is done once per store instance; in production this will be migrated.
    void InitSchema();
    Poco::Data::Session session_;
};

}  // namespace nebulafs::metadata
