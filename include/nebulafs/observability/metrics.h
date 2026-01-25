#pragma once

#include <string>

namespace nebulafs::observability {

/// @brief Render Prometheus-style metrics for a minimal `/metrics` endpoint.
std::string RenderMetrics();
/// @brief Record a completed HTTP request for metrics.
void RecordRequest(int status_code, long long latency_ms);

}  // namespace nebulafs::observability
