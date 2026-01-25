#pragma once

#include <cstdint>
#include <string>

namespace nebulafs::core {

/// @brief TLS configuration for the HTTP server.
struct TlsConfig {
    bool enabled{false};
    std::string certificate;
    std::string private_key;
};

/// @brief Request/connection limits for the HTTP server.
struct LimitsConfig {
    std::uint64_t max_body_bytes{268435456};
};

/// @brief HTTP server configuration (bind address, TLS, limits).
struct ServerConfig {
    std::string host{"0.0.0.0"};
    int port{8080};
    int threads{4};
    TlsConfig tls;
    LimitsConfig limits;
};

/// @brief Storage configuration for local filesystem backend.
struct StorageConfig {
    std::string base_path{"data"};
    std::string temp_path{"data/tmp"};
};

/// @brief Observability settings (logging).
struct ObservabilityConfig {
    std::string log_level{"information"};
};

/// @brief Top-level configuration for NebulaFS.
struct Config {
    ServerConfig server;
    StorageConfig storage;
    ObservabilityConfig observability;
};

/// @brief Load server configuration from a JSON file.
Config LoadConfig(const std::string& path);
/// @brief Load SQLite metadata DB path from a JSON file.
std::string LoadDatabasePath(const std::string& path);

}  // namespace nebulafs::core
