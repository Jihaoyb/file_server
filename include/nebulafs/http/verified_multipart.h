#pragma once

#include <memory>

#include "nebulafs/http/router.h"
#include "nebulafs/metadata/sqlite_metadata_store.h"
#include "nebulafs/storage/storage_backend.h"

namespace nebulafs::http {

// Register literal pilot routes ahead of the generic bucket API.
void RegisterVerifiedMultipart(Router& router, std::shared_ptr<metadata::SqliteMetadataStore> store,
                               std::shared_ptr<storage::StorageBackend> storage, int ttl_seconds);

}  // namespace nebulafs::http
