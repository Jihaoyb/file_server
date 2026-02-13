#include "nebulafs/core/config.h"

#include <cctype>
#include <stdexcept>

#include <Poco/Util/JSONConfiguration.h>
#include <Poco/Util/PropertyFileConfiguration.h>
#include <Poco/AutoPtr.h>

namespace nebulafs::core {

namespace {

bool IsBlank(const std::string& value) {
    for (char c : value) {
        if (!std::isspace(static_cast<unsigned char>(c))) {
            return false;
        }
    }
    return true;
}

}  // namespace

Config LoadConfig(const std::string& path) {
    Poco::AutoPtr<Poco::Util::JSONConfiguration> cfg(
        new Poco::Util::JSONConfiguration(path));

    Config config;
    config.server.host = cfg->getString("server.host", "0.0.0.0");
    config.server.port = cfg->getInt("server.port", 8080);
    config.server.threads = cfg->getInt("server.threads", 4);
    config.server.tls.enabled = cfg->getBool("server.tls.enabled", false);
    config.server.tls.certificate = cfg->getString("server.tls.certificate", "");
    config.server.tls.private_key = cfg->getString("server.tls.private_key", "");
    config.server.limits.max_body_bytes =
        static_cast<std::uint64_t>(cfg->getInt64("server.limits.max_body_bytes", 268435456));

    config.storage.base_path = cfg->getString("storage.base_path", "data");
    config.storage.temp_path = cfg->getString("storage.temp_path", "data/tmp");
    config.storage.multipart.max_upload_ttl_seconds =
        cfg->getInt("storage.multipart.max_upload_ttl_seconds", 86400);

    config.cleanup.enabled = cfg->getBool("cleanup.enabled", true);
    config.cleanup.sweep_interval_seconds = cfg->getInt("cleanup.sweep_interval_seconds", 300);
    config.cleanup.grace_period_seconds = cfg->getInt("cleanup.grace_period_seconds", 60);
    config.cleanup.max_uploads_per_sweep = cfg->getInt("cleanup.max_uploads_per_sweep", 200);

    config.observability.log_level = cfg->getString("observability.log_level", "information");

    config.auth.enabled = cfg->getBool("auth.enabled", false);
    config.auth.issuer = cfg->getString("auth.issuer", "");
    config.auth.audience = cfg->getString("auth.audience", "");
    config.auth.jwks_url = cfg->getString("auth.jwks_url", "");
    config.auth.cache_ttl_seconds = cfg->getInt("auth.cache_ttl_seconds", 300);
    config.auth.clock_skew_seconds = cfg->getInt("auth.clock_skew_seconds", 60);
    config.auth.allowed_alg = cfg->getString("auth.allowed_alg", "RS256");

    if (config.auth.enabled) {
        // Fail fast so auth mode cannot run with incomplete trust configuration.
        if (IsBlank(config.auth.issuer)) {
            throw std::invalid_argument("auth.enabled=true requires non-empty auth.issuer");
        }
        if (IsBlank(config.auth.jwks_url)) {
            throw std::invalid_argument("auth.enabled=true requires non-empty auth.jwks_url");
        }
    }
    if (config.storage.multipart.max_upload_ttl_seconds <= 0) {
        throw std::invalid_argument("storage.multipart.max_upload_ttl_seconds must be positive");
    }
    if (config.cleanup.sweep_interval_seconds <= 0) {
        throw std::invalid_argument("cleanup.sweep_interval_seconds must be positive");
    }
    if (config.cleanup.max_uploads_per_sweep <= 0) {
        throw std::invalid_argument("cleanup.max_uploads_per_sweep must be positive");
    }
    return config;
}

std::string LoadDatabasePath(const std::string& path) {
    Poco::AutoPtr<Poco::Util::JSONConfiguration> cfg(
        new Poco::Util::JSONConfiguration(path));
    return cfg->getString("sqlite.path", "data/metadata.db");
}

}  // namespace nebulafs::core
