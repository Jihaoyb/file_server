#include "nebulafs/core/time.h"

#include <Poco/DateTimeFormatter.h>
#include <Poco/DateTimeFormat.h>
#include <Poco/Timestamp.h>
#include <Poco/Timespan.h>

namespace nebulafs::core {

std::string NowIso8601() {
    return Poco::DateTimeFormatter::format(Poco::Timestamp(), Poco::DateTimeFormat::ISO8601_FORMAT);
}

std::string NowIso8601WithOffsetSeconds(int delta_seconds) {
    Poco::Timestamp ts;
    ts += Poco::Timespan(delta_seconds, 0);
    return Poco::DateTimeFormatter::format(ts, Poco::DateTimeFormat::ISO8601_FORMAT);
}

}  // namespace nebulafs::core
