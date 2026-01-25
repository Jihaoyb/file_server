#include "nebulafs/core/logger.h"

#include <Poco/AutoPtr.h>
#include <Poco/ConsoleChannel.h>
#include <Poco/FormattingChannel.h>
#include <Poco/Logger.h>
#include <Poco/PatternFormatter.h>

namespace nebulafs::core {

namespace {
Poco::Logger& RootLogger() {
    return Poco::Logger::get("nebulafs");
}

std::string EscapeJson(const std::string& value) {
    std::string out;
    out.reserve(value.size());
    for (char ch : value) {
        switch (ch) {
            case '\"':
                out += "\\\"";
                break;
            case '\\':
                out += "\\\\";
                break;
            case '\n':
                out += "\\n";
                break;
            case '\r':
                out += "\\r";
                break;
            case '\t':
                out += "\\t";
                break;
            default:
                out += ch;
                break;
        }
    }
    return out;
}

int ToPocoLevel(const std::string& level) {
    if (level == "debug") {
        return Poco::Message::PRIO_DEBUG;
    }
    if (level == "error") {
        return Poco::Message::PRIO_ERROR;
    }
    if (level == "trace") {
        return Poco::Message::PRIO_TRACE;
    }
    return Poco::Message::PRIO_INFORMATION;
}
}  // namespace

void InitLogging(const std::string& level) {
    Poco::AutoPtr<Poco::ConsoleChannel> console(new Poco::ConsoleChannel());
    Poco::AutoPtr<Poco::PatternFormatter> formatter(
        new Poco::PatternFormatter("%Y-%m-%dT%H:%M:%S.%iZ [%p] %t"));
    Poco::AutoPtr<Poco::FormattingChannel> channel(new Poco::FormattingChannel(formatter, console));
    RootLogger().setChannel(channel);
    RootLogger().setLevel(ToPocoLevel(level));
}

void LogInfo(const std::string& message) { RootLogger().information(message); }
void LogError(const std::string& message) { RootLogger().error(message); }
void LogDebug(const std::string& message) { RootLogger().debug(message); }
void LogRequest(const std::string& request_id,
                const std::string& method,
                const std::string& target,
                const std::string& remote,
                int status,
                long long latency_ms) {
    std::string message =
        "{\"event\":\"http_request\",\"request_id\":\"" + EscapeJson(request_id) +
        "\",\"method\":\"" + EscapeJson(method) +
        "\",\"target\":\"" + EscapeJson(target) +
        "\",\"remote\":\"" + EscapeJson(remote) +
        "\",\"status\":" + std::to_string(status) +
        ",\"latency_ms\":" + std::to_string(latency_ms) + "}";
    LogInfo(message);
}

}  // namespace nebulafs::core
