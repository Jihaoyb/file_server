#include <filesystem>
#include <fstream>
#include <string>

#include <gtest/gtest.h>
#include <Poco/UUIDGenerator.h>

#include "nebulafs/core/config.h"

namespace {

std::filesystem::path MakeTempConfigPath() {
    const auto name = "nebulafs_cfg_" + Poco::UUIDGenerator().createOne().toString() + ".json";
    return std::filesystem::temp_directory_path() / name;
}

void WriteConfig(const std::filesystem::path& path, bool auth_enabled,
                 const std::string& issuer, const std::string& jwks_url,
                 const std::string& mode = "single_node",
                 const std::string& metadata_url = "",
                 const std::string& service_token = "",
                 const std::string& storage_nodes_json = "[]") {
    std::ofstream out(path);
    out << "{\n"
        << "  \"server\": {\n"
        << "    \"host\": \"127.0.0.1\",\n"
        << "    \"port\": 8080,\n"
        << "    \"threads\": 1,\n"
        << "    \"mode\": \"" << mode << "\",\n"
        << "    \"tls\": {\"enabled\": false, \"certificate\": \"\", \"private_key\": \"\"},\n"
        << "    \"limits\": {\"max_body_bytes\": 1048576}\n"
        << "  },\n"
        << "  \"storage\": {\"base_path\": \"data\", \"temp_path\": \"data/tmp\"},\n"
        << "  \"observability\": {\"log_level\": \"warning\"},\n"
        << "  \"auth\": {\n"
        << "    \"enabled\": " << (auth_enabled ? "true" : "false") << ",\n"
        << "    \"issuer\": \"" << issuer << "\",\n"
        << "    \"audience\": \"\",\n"
        << "    \"jwks_url\": \"" << jwks_url << "\",\n"
        << "    \"cache_ttl_seconds\": 300,\n"
        << "    \"clock_skew_seconds\": 60,\n"
        << "    \"allowed_alg\": \"RS256\"\n"
        << "  },\n"
        << "  \"distributed\": {\n"
        << "    \"metadata_base_url\": \"" << metadata_url << "\",\n"
        << "    \"storage_nodes\": " << storage_nodes_json << ",\n"
        << "    \"service_auth_token\": \"" << service_token << "\",\n"
        << "    \"replication_factor\": 2,\n"
        << "    \"min_write_acks\": 2\n"
        << "  }\n"
        << "}\n";
}

}  // namespace

TEST(Config, AuthDisabledAllowsEmptyIssuerAndJwksUrl) {
    const auto path = MakeTempConfigPath();
    WriteConfig(path, false, "", "");

    EXPECT_NO_THROW({
        auto config = nebulafs::core::LoadConfig(path.string());
        EXPECT_FALSE(config.auth.enabled);
    });

    std::filesystem::remove(path);
}

TEST(Config, AuthEnabledRequiresIssuer) {
    const auto path = MakeTempConfigPath();
    WriteConfig(path, true, "", "https://issuer.example.local/jwks");

    EXPECT_THROW({ (void)nebulafs::core::LoadConfig(path.string()); }, std::invalid_argument);

    std::filesystem::remove(path);
}

TEST(Config, AuthEnabledRequiresJwksUrl) {
    const auto path = MakeTempConfigPath();
    WriteConfig(path, true, "https://issuer.example.local", "");

    EXPECT_THROW({ (void)nebulafs::core::LoadConfig(path.string()); }, std::invalid_argument);

    std::filesystem::remove(path);
}

TEST(Config, DistributedModeRequiresMetadataBaseUrl) {
    const auto path = MakeTempConfigPath();
    WriteConfig(path, false, "", "", "distributed", "", "token", "[\"http://127.0.0.1:9100\",\"http://127.0.0.1:9101\"]");

    EXPECT_THROW({ (void)nebulafs::core::LoadConfig(path.string()); }, std::invalid_argument);

    std::filesystem::remove(path);
}

TEST(Config, DistributedModeRequiresStorageNodes) {
    const auto path = MakeTempConfigPath();
    WriteConfig(path, false, "", "", "distributed", "http://127.0.0.1:9091", "token", "[]");

    EXPECT_THROW({ (void)nebulafs::core::LoadConfig(path.string()); }, std::invalid_argument);

    std::filesystem::remove(path);
}

TEST(Config, DistributedModeLoadsValidSettings) {
    const auto path = MakeTempConfigPath();
    WriteConfig(path, false, "", "", "distributed", "http://127.0.0.1:9091", "token",
                "[\"http://127.0.0.1:9100\",\"http://127.0.0.1:9101\"]");

    auto config = nebulafs::core::LoadConfig(path.string());
    EXPECT_EQ(config.server.mode, "distributed");
    EXPECT_EQ(config.distributed.metadata_base_url, "http://127.0.0.1:9091");
    EXPECT_EQ(config.distributed.service_auth_token, "token");
    ASSERT_EQ(config.distributed.storage_nodes.size(), 2u);

    std::filesystem::remove(path);
}
