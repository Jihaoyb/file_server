#pragma once

#include <memory>

#include "nebulafs/core/config.h"
#include "nebulafs/http/router.h"

namespace nebulafs::metadata {
class MetadataStore;
}

namespace nebulafs::storage {
class LocalStorage;
}

namespace nebulafs::http {

/// Registers the server's HTTP routes into the provided router.
void RegisterDefaultRoutes(Router& router, std::shared_ptr<metadata::MetadataStore> metadata,
                           std::shared_ptr<storage::LocalStorage> storage,
                           const core::Config& config);

}  // namespace nebulafs::http
