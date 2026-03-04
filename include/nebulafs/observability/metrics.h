#pragma once

#include <string>

namespace nebulafs::observability {

/// @brief Render Prometheus-style metrics for a minimal `/metrics` endpoint.
std::string RenderMetrics();
/// @brief Record a completed HTTP request for metrics.
void RecordRequest(int status_code, long long latency_ms);
/// @brief Record a request rejected by rate limiting.
void RecordRateLimited();
/// @brief Record a request that exceeded timeout budget.
void RecordTimedOut();
/// @brief Record distributed storage PUT failures from gateway.
void RecordGatewayStoragePutFailure();
/// @brief Record distributed metadata RPC failures from gateway.
void RecordGatewayMetadataRpcFailure();
/// @brief Record replica fallback on distributed reads.
void RecordGatewayReplicaFallback();
/// @brief Record metadata allocate-write request outcome and latency.
void RecordMetadataAllocate(bool success, long long latency_ms);
/// @brief Record metadata commit request outcome and latency.
void RecordMetadataCommit(bool success, long long latency_ms);
/// @brief Record storage node blob write request outcome and latency.
void RecordStorageNodeWrite(bool success, long long latency_ms);
/// @brief Record storage node blob read request outcome and latency.
void RecordStorageNodeRead(bool success, long long latency_ms);
/// @brief Record storage node blob delete request outcome and latency.
void RecordStorageNodeDelete(bool success, long long latency_ms);

}  // namespace nebulafs::observability
