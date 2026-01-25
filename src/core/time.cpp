#include "nebulafs/core/time.h"

#include <Poco/DateTimeFormatter.h>
#include <Poco/DateTimeFormat.h>
#include <Poco/Timestamp.h>

namespace nebulafs::core {

std::string NowIso8601() {
    return Poco::DateTimeFormatter::format(Poco::Timestamp(), Poco::DateTimeFormat::ISO8601_FORMAT);
}

}  // namespace nebulafs::core
