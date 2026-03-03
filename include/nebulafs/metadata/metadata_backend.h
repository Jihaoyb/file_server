#pragma once

#include "nebulafs/metadata/metadata_store.h"

namespace nebulafs::metadata {

// Milestone 6 keeps the existing store contract and introduces a backend naming
// alias so gateway wiring can switch between local and remote implementations.
using MetadataBackend = MetadataStore;

}  // namespace nebulafs::metadata
