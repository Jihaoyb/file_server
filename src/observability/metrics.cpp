#include "nebulafs/observability/metrics.h"

#include <atomic>

namespace nebulafs::observability {
namespace {
std::atomic<std::uint64_t> g_total_requests{0};
std::atomic<std::uint64_t> g_requests_2xx{0};
std::atomic<std::uint64_t> g_requests_4xx{0};
std::atomic<std::uint64_t> g_requests_5xx{0};
std::atomic<std::uint64_t> g_latency_ms_total{0};
std::atomic<std::uint64_t> g_rate_limited_total{0};
std::atomic<std::uint64_t> g_timed_out_total{0};
std::atomic<std::uint64_t> g_gateway_storage_put_failures_total{0};
std::atomic<std::uint64_t> g_gateway_metadata_rpc_failures_total{0};
std::atomic<std::uint64_t> g_gateway_replica_fallback_total{0};
}  // namespace

void RecordRequest(int status_code, long long latency_ms) {
    g_total_requests.fetch_add(1, std::memory_order_relaxed);
    g_latency_ms_total.fetch_add(static_cast<std::uint64_t>(latency_ms),
                                 std::memory_order_relaxed);
    if (status_code >= 200 && status_code < 300) {
        g_requests_2xx.fetch_add(1, std::memory_order_relaxed);
    } else if (status_code >= 400 && status_code < 500) {
        g_requests_4xx.fetch_add(1, std::memory_order_relaxed);
    } else if (status_code >= 500) {
        g_requests_5xx.fetch_add(1, std::memory_order_relaxed);
    }
}

void RecordRateLimited() { g_rate_limited_total.fetch_add(1, std::memory_order_relaxed); }

void RecordTimedOut() { g_timed_out_total.fetch_add(1, std::memory_order_relaxed); }

void RecordGatewayStoragePutFailure() {
    g_gateway_storage_put_failures_total.fetch_add(1, std::memory_order_relaxed);
}

void RecordGatewayMetadataRpcFailure() {
    g_gateway_metadata_rpc_failures_total.fetch_add(1, std::memory_order_relaxed);
}

void RecordGatewayReplicaFallback() {
    g_gateway_replica_fallback_total.fetch_add(1, std::memory_order_relaxed);
}

std::string RenderMetrics() {
    const auto total = g_total_requests.load(std::memory_order_relaxed);
    const auto total_latency = g_latency_ms_total.load(std::memory_order_relaxed);
    return "# HELP nebulafs_up 1 if server is up\n"
           "# TYPE nebulafs_up gauge\n"
           "nebulafs_up 1\n"
           "# HELP nebulafs_http_requests_total Total HTTP requests processed\n"
           "# TYPE nebulafs_http_requests_total counter\n"
           "nebulafs_http_requests_total " + std::to_string(total) + "\n"
           "# HELP nebulafs_http_requests_2xx Total 2xx responses\n"
           "# TYPE nebulafs_http_requests_2xx counter\n"
           "nebulafs_http_requests_2xx " +
           std::to_string(g_requests_2xx.load(std::memory_order_relaxed)) + "\n"
           "# HELP nebulafs_http_requests_4xx Total 4xx responses\n"
           "# TYPE nebulafs_http_requests_4xx counter\n"
           "nebulafs_http_requests_4xx " +
           std::to_string(g_requests_4xx.load(std::memory_order_relaxed)) + "\n"
           "# HELP nebulafs_http_requests_5xx Total 5xx responses\n"
           "# TYPE nebulafs_http_requests_5xx counter\n"
           "nebulafs_http_requests_5xx " +
           std::to_string(g_requests_5xx.load(std::memory_order_relaxed)) + "\n"
           "# HELP nebulafs_http_request_latency_ms_sum Sum of request latencies in ms\n"
           "# TYPE nebulafs_http_request_latency_ms_sum counter\n"
           "nebulafs_http_request_latency_ms_sum " + std::to_string(total_latency) + "\n"
           "# HELP nebulafs_http_requests_rate_limited_total Total requests rejected by rate limits\n"
           "# TYPE nebulafs_http_requests_rate_limited_total counter\n"
           "nebulafs_http_requests_rate_limited_total " +
           std::to_string(g_rate_limited_total.load(std::memory_order_relaxed)) + "\n"
           "# HELP nebulafs_http_requests_timed_out_total Total requests timed out\n"
           "# TYPE nebulafs_http_requests_timed_out_total counter\n"
           "nebulafs_http_requests_timed_out_total " +
           std::to_string(g_timed_out_total.load(std::memory_order_relaxed)) + "\n"
           "# HELP nebulafs_gateway_storage_put_failures_total Total distributed storage PUT failures\n"
           "# TYPE nebulafs_gateway_storage_put_failures_total counter\n"
           "nebulafs_gateway_storage_put_failures_total " +
           std::to_string(g_gateway_storage_put_failures_total.load(std::memory_order_relaxed)) +
           "\n"
           "# HELP nebulafs_gateway_metadata_rpc_failures_total Total distributed metadata RPC failures\n"
           "# TYPE nebulafs_gateway_metadata_rpc_failures_total counter\n"
           "nebulafs_gateway_metadata_rpc_failures_total " +
           std::to_string(g_gateway_metadata_rpc_failures_total.load(std::memory_order_relaxed)) +
           "\n"
           "# HELP nebulafs_gateway_replica_fallback_total Total distributed read fallback events\n"
           "# TYPE nebulafs_gateway_replica_fallback_total counter\n"
           "nebulafs_gateway_replica_fallback_total " +
           std::to_string(g_gateway_replica_fallback_total.load(std::memory_order_relaxed)) + "\n";
}

}  // namespace nebulafs::observability
