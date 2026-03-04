#pragma once

#include <memory>

#include "nebulafs/core/config.h"
#include "nebulafs/http/router.h"
#include "nebulafs/metadata/metadata_backend.h"
#include "nebulafs/storage/storage_backend.h"

namespace nebulafs::http {

/// Registers the server's HTTP routes into the provided router.
void RegisterDefaultRoutes(Router& router, std::shared_ptr<metadata::MetadataBackend> metadata,
                           std::shared_ptr<storage::StorageBackend> storage,
                           const core::Config& config);

}  // namespace nebulafs::http
