#pragma once

#include <string>

namespace nebulafs::core {

void InitLogging(const std::string& level);
void LogInfo(const std::string& message);
void LogError(const std::string& message);
void LogDebug(const std::string& message);
/// @brief Log a structured JSON line for HTTP requests.
void LogRequest(const std::string& request_id,
                const std::string& method,
                const std::string& target,
                const std::string& remote,
                int status,
                long long latency_ms);

}  // namespace nebulafs::core
