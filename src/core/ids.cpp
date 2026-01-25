#include "nebulafs/core/ids.h"

#include <Poco/UUIDGenerator.h>

namespace nebulafs::core {

std::string GenerateRequestId() {
    return Poco::UUIDGenerator().createOne().toString();
}

}  // namespace nebulafs::core
