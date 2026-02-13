#include <filesystem>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include <boost/asio/io_context.hpp>

#include "nebulafs/core/config.h"
#include "nebulafs/core/logger.h"
#include "nebulafs/http/http_server.h"
#include "nebulafs/http/route_registration.h"
#include "nebulafs/http/router.h"
#include "nebulafs/metadata/sqlite_metadata_store.h"
#include "nebulafs/storage/local_storage.h"

namespace {

std::string GetArgValue(int argc, char** argv, const std::string& key,
                        const std::string& default_value) {
    for (int i = 1; i < argc - 1; ++i) {
        if (argv[i] == key) {
            return argv[i + 1];
        }
    }
    return default_value;
}

}  // namespace

int main(int argc, char** argv) {
    const std::string config_path = GetArgValue(argc, argv, "--config", "config/server.json");
    const std::string db_path = GetArgValue(argc, argv, "--database", "config/database.json");

    auto config = nebulafs::core::LoadConfig(config_path);
    nebulafs::core::InitLogging(config.observability.log_level);

    auto sqlite_path = nebulafs::core::LoadDatabasePath(db_path);
    std::filesystem::create_directories(std::filesystem::path(sqlite_path).parent_path());
    auto metadata = std::make_shared<nebulafs::metadata::SqliteMetadataStore>(sqlite_path);
    auto storage = std::make_shared<nebulafs::storage::LocalStorage>(config.storage.base_path,
                                                                     config.storage.temp_path);

    nebulafs::http::Router router;
    nebulafs::http::RegisterDefaultRoutes(router, metadata, storage);

    boost::asio::io_context ioc(config.server.threads);
    nebulafs::http::HttpServer server(ioc, config, std::move(router), storage, metadata);
    server.Run();

    std::vector<std::thread> threads;
    threads.reserve(static_cast<size_t>(config.server.threads));
    for (int i = 0; i < config.server.threads; ++i) {
        threads.emplace_back([&ioc]() { ioc.run(); });
    }
    for (auto& t : threads) {
        t.join();
    }

    return 0;
}
