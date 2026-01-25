#pragma once

#include <memory>
#include <string>

#include <boost/asio/io_context.hpp>
#include <boost/asio/ssl/context.hpp>

#include "nebulafs/core/config.h"
#include "nebulafs/http/router.h"
#include "nebulafs/metadata/metadata_store.h"
#include "nebulafs/storage/local_storage.h"

namespace nebulafs::http {

/// @brief HTTP server bootstrapper (acceptor + TLS context).
class HttpServer {
public:
    HttpServer(boost::asio::io_context& ioc, const core::Config& config, Router router,
               std::shared_ptr<storage::LocalStorage> storage,
               std::shared_ptr<metadata::MetadataStore> metadata);
    void Run();

private:
    boost::asio::io_context& ioc_;
    core::Config config_;
    Router router_;
    std::shared_ptr<storage::LocalStorage> storage_;
    std::shared_ptr<metadata::MetadataStore> metadata_;
    std::unique_ptr<boost::asio::ssl::context> ssl_context_;
};

}  // namespace nebulafs::http
