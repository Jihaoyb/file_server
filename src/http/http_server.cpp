#include "nebulafs/http/http_server.h"

#include <array>
#include <cctype>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <memory>
#include <optional>
#include <sstream>
#include <string>
#include <type_traits>

#include <Poco/JSON/Object.h>
#include <Poco/JSON/Parser.h>
#include <Poco/UUIDGenerator.h>
#include <Poco/DigestEngine.h>
#include <Poco/SHA2Engine.h>

#include <boost/asio/bind_executor.hpp>
#include <boost/asio/dispatch.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/beast/core.hpp>
#include <boost/beast/http.hpp>
#include <boost/beast/ssl.hpp>

#include "nebulafs/core/ids.h"
#include "nebulafs/core/logger.h"
#include "nebulafs/core/time.h"
#include "nebulafs/observability/metrics.h"
#include "nebulafs/auth/jwt_utils.h"
#include "nebulafs/auth/jwt_verifier.h"

#ifdef _WIN32
#include <windows.h>
#else
#include <fcntl.h>
#include <unistd.h>
#endif

namespace {
namespace beast = boost::beast;
namespace http = beast::http;
namespace net = boost::asio;
using tcp = net::ip::tcp;

constexpr std::size_t kBufferSize = 8192;

struct RangeRequest {
    std::uint64_t start{0};
    std::uint64_t end{0};
};

std::string StripQuery(const std::string& target) {
    auto pos = target.find('?');
    if (pos == std::string::npos) {
        return target;
    }
    return target.substr(0, pos);
}

std::string GetQueryParam(const std::string& target, const std::string& key) {
    auto pos = target.find('?');
    if (pos == std::string::npos) {
        return "";
    }
    auto query = target.substr(pos + 1);
    std::stringstream ss(query);
    std::string item;
    while (std::getline(ss, item, '&')) {
        auto eq = item.find('=');
        if (eq == std::string::npos) {
            continue;
        }
        if (item.substr(0, eq) == key) {
            return item.substr(eq + 1);
        }
    }
    return "";
}

std::optional<RangeRequest> ParseRange(const std::string& header, std::uint64_t size) {
    // We only accept byte ranges; other units are rejected early.
    if (header.rfind("bytes=", 0) != 0) {
        return std::nullopt;
    }
    auto range = header.substr(6);
    auto dash = range.find('-');
    if (dash == std::string::npos) {
        return std::nullopt;
    }
    std::string start_str = range.substr(0, dash);
    std::string end_str = range.substr(dash + 1);

    RangeRequest req;
    if (start_str.empty()) {
        return std::nullopt;
    }
    req.start = static_cast<std::uint64_t>(std::stoull(start_str));
    if (end_str.empty()) {
        req.end = size - 1;
    } else {
        req.end = static_cast<std::uint64_t>(std::stoull(end_str));
    }
    if (req.start > req.end || req.start >= size) {
        return std::nullopt;
    }
    return req;
}

http::response<http::string_body> JsonResponse(http::status status, int version,
                                               const std::string& body) {
    http::response<http::string_body> response{status, version};
    response.set(http::field::content_type, "application/json");
    response.body() = body;
    response.prepare_payload();
    return response;
}

http::response<http::string_body> ErrorResponse(http::status status, int version,
                                                const std::string& code,
                                                const std::string& message,
                                                const std::string& request_id) {
    // Consistent error envelope for client troubleshooting.
    std::string body = "{\"error\":{\"code\":\"" + code +
                       "\",\"message\":\"" + message +
                       "\",\"request_id\":\"" + request_id + "\"}}";
    return JsonResponse(status, version, body);
}

template <typename Stream>
class Session : public std::enable_shared_from_this<Session<Stream>> {
public:
    Session(Stream&& stream, nebulafs::http::Router router, nebulafs::core::Config config,
            std::shared_ptr<nebulafs::storage::LocalStorage> storage,
            std::shared_ptr<nebulafs::metadata::MetadataStore> metadata,
            std::shared_ptr<nebulafs::auth::JwtVerifier> auth_verifier)
        : stream_(std::move(stream)),
          router_(std::move(router)),
          config_(std::move(config)),
          storage_(std::move(storage)),
          metadata_(std::move(metadata)),
          auth_verifier_(std::move(auth_verifier)) {
    }

    void Start() {
        // TLS handshake happens once per connection when enabled.
        if constexpr (std::is_same_v<Stream, beast::ssl_stream<beast::tcp_stream>>) {
            stream_.async_handshake(net::ssl::stream_base::server,
                                    beast::bind_front_handler(&Session::OnHandshake,
                                                              this->shared_from_this()));
        } else {
            DoReadHeader();
        }
    }

private:
    void OnHandshake(beast::error_code ec) {
        if (ec) {
            nebulafs::core::LogError("TLS handshake failed: " + ec.message());
            return;
        }
        DoReadHeader();
    }

    void DoReadHeader() {
        parser_.emplace();
        parser_->body_limit(config_.server.limits.max_body_bytes);
        http::async_read_header(stream_, buffer_, *parser_,
                                beast::bind_front_handler(&Session::OnReadHeader,
                                                          this->shared_from_this()));
    }

    void OnReadHeader(beast::error_code ec, std::size_t) {
        if (ec == http::error::end_of_stream) {
            return DoClose();
        }
        if (ec) {
            nebulafs::core::LogError("Read header failed: " + ec.message());
            return;
        }

        request_id_ = nebulafs::core::GenerateRequestId();
        // Clear any previous request state for reused sessions.
        auth_claims_.reset();
        request_start_ = std::chrono::steady_clock::now();
        request_method_ = std::string(parser_->get().method_string());
        request_target_ = std::string(parser_->get().target());
        request_remote_ = GetRemoteAddress();
        const auto target = request_target_;
        const auto path = StripQuery(target);
        const auto method = parser_->get().method();

        // Enforce auth early to avoid streaming uploads for unauthorized requests.
        auto auth_response = EnsureAuthorized(parser_->get(), path);
        if (auth_response) {
            return Send(std::move(*auth_response));
        }

        // Fast-path: stream uploads directly to disk to avoid buffering large bodies.
        if (method == http::verb::put && IsObjectPath(path)) {
            StartUpload(path);
            return;
        }
        if (method == http::verb::post &&
            nebulafs::http::Router::Match("/v1/buckets/{bucket}/objects", path, nullptr)) {
            nebulafs::http::RouteParams params;
            nebulafs::http::Router::Match("/v1/buckets/{bucket}/objects", path, &params);
            const auto name = GetQueryParam(target, "name");
            if (name.empty()) {
                auto response = ErrorResponse(http::status::bad_request, parser_->get().version(),
                                              "MISSING_NAME", "missing object name",
                                              request_id_);
                return Send(std::move(response));
            }
            const auto full_path = "/v1/buckets/" + params["bucket"] + "/objects/" + name;
            StartUpload(full_path);
            return;
        }

        if (parser_->is_done()) {
            body_.clear();
            return HandleRequest();
        }
        ReadBodyToString();
    }

    void ReadBodyToString() {
        body_.clear();
        if (parser_->content_length() && parser_->content_length().value() == 0) {
            return HandleRequest();
        }
        DoReadBodyChunk();
    }

    void DoReadBodyChunk() {
        parser_->get().body().data = body_buffer_.data();
        parser_->get().body().size = body_buffer_.size();
        http::async_read(stream_, buffer_, *parser_,
                         beast::bind_front_handler(&Session::OnBodyChunk,
                                                   this->shared_from_this()));
    }

    void OnBodyChunk(beast::error_code ec, std::size_t) {
        if (ec && ec != http::error::need_buffer) {
            nebulafs::core::LogError("Read body failed: " + ec.message());
            return;
        }
        const auto bytes = body_buffer_.size() - parser_->get().body().size;
        body_.append(body_buffer_.data(), bytes);
        if (parser_->is_done()) {
            return HandleRequest();
        }
        DoReadBodyChunk();
    }

    void HandleRequest() {
        // Convert buffer_body parser into a string_body request for routing handlers.
        nebulafs::http::HttpRequest request;
        request.method(parser_->get().method());
        request.target(parser_->get().target());
        request.version(parser_->get().version());
        for (const auto& field : parser_->get()) {
            request.set(field.name(), field.value());
        }
        request.body() = body_;
        request.prepare_payload();

        nebulafs::http::RequestContext ctx;
        ctx.request_id = request_id_;
        ctx.method = std::string(request.method_string());
        ctx.target = std::string(request.target());
        ctx.remote = request_remote_;
        if (auth_claims_) {
            ctx.auth = nebulafs::http::AuthContext{auth_claims_->subject, auth_claims_->issuer,
                                                   auth_claims_->audience, auth_claims_->scopes};
        }

        const auto target = std::string(request.target());
        const auto path = StripQuery(target);
        // Enforce auth for non-upload requests.
        auto auth_response = EnsureAuthorized(request, path);
        if (auth_response) {
            return Send(std::move(*auth_response));
        }
        if (request.method() == http::verb::get && IsObjectPath(path)) {
            return HandleDownload(request, path);
        }

        auto result = router_.Route(ctx, request);
        if (!result.ok()) {
            auto response = ErrorResponse(http::status::internal_server_error, request.version(),
                                          "INTERNAL", result.error().message, request_id_);
            return Send(std::move(response));
        }
        Send(std::move(result.value()));
    }

    bool IsObjectPath(const std::string& path) {
        nebulafs::http::RouteParams params;
        return nebulafs::http::Router::Match("/v1/buckets/{bucket}/objects/{object}", path,
                                             &params);
    }

    bool IsPublicPath(const std::string& path) const {
        // Keep health endpoints public for liveness checks.
        return path == "/healthz" || path == "/readyz";
    }

    template <typename Request>
    std::optional<std::string> ExtractBearerToken(const Request& request) {
        // Expect "Authorization: Bearer <token>".
        auto it = request.find(http::field::authorization);
        if (it == request.end()) {
            return std::nullopt;
        }
        // Convert header value from Beast string_view in a portable way.
        std::string value = nebulafs::auth::Trim(std::string(it->value()));
        if (value.size() < 7) {
            return std::nullopt;
        }
        std::string prefix = value.substr(0, 7);
        for (auto& c : prefix) {
            c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
        }
        if (prefix != "bearer ") {
            return std::nullopt;
        }
        return nebulafs::auth::Trim(value.substr(7));
    }

    template <typename Request>
    std::optional<http::response<http::string_body>> EnsureAuthorized(const Request& request,
                                                                      const std::string& path) {
        // Skip auth when disabled or for public endpoints.
        if (!config_.auth.enabled || IsPublicPath(path)) {
            return std::nullopt;
        }
        auto token = ExtractBearerToken(request);
        if (!token || token->empty()) {
            return ErrorResponse(http::status::unauthorized, request.version(), "UNAUTHORIZED",
                                 "missing bearer token", request_id_);
        }
        auto result = auth_verifier_->Verify(*token);
        if (!result.ok()) {
            return ErrorResponse(http::status::unauthorized, request.version(), "UNAUTHORIZED",
                                 result.error().message, request_id_);
        }
        auth_claims_ = result.value();
        return std::nullopt;
    }

    void StartUpload(const std::string& path) {
        nebulafs::http::RouteParams params;
        nebulafs::http::Router::Match("/v1/buckets/{bucket}/objects/{object}", path, &params);
        const auto bucket = params["bucket"];
        const auto object = params["object"];

        if (!nebulafs::storage::LocalStorage::IsSafeName(bucket) ||
            !nebulafs::storage::LocalStorage::IsSafeName(object)) {
            auto response = ErrorResponse(http::status::bad_request, parser_->get().version(),
                                          "INVALID_NAME", "invalid bucket/object", request_id_);
            return Send(std::move(response));
        }

        auto bucket_result = metadata_->GetBucket(bucket);
        if (!bucket_result.ok()) {
            auto response = ErrorResponse(http::status::not_found, parser_->get().version(),
                                          "BUCKET_NOT_FOUND", "bucket not found", request_id_);
            return Send(std::move(response));
        }

        storage_->EnsureBucket(bucket);

        upload_bucket_ = bucket;
        upload_object_ = object;
        upload_temp_path_ =
            (std::filesystem::path(storage_->temp_path()) /
             Poco::UUIDGenerator().createOne().toString())
                .string();

#ifdef _WIN32
        upload_stream_.open(upload_temp_path_, std::ios::binary | std::ios::trunc);
        if (!upload_stream_.is_open()) {
            auto response = ErrorResponse(http::status::internal_server_error,
                                          parser_->get().version(), "IO_ERROR",
                                          "failed to open temp file", request_id_);
            return Send(std::move(response));
        }
#else
        upload_fd_ = ::open(upload_temp_path_.c_str(), O_CREAT | O_WRONLY | O_TRUNC, 0644);
        if (upload_fd_ < 0) {
            auto response = ErrorResponse(http::status::internal_server_error,
                                          parser_->get().version(), "IO_ERROR",
                                          "failed to open temp file", request_id_);
            return Send(std::move(response));
        }
#endif

        upload_hasher_.emplace();
        upload_total_ = 0;
        DoReadUploadChunk();
    }

    void DoReadUploadChunk() {
        parser_->get().body().data = body_buffer_.data();
        parser_->get().body().size = body_buffer_.size();
        http::async_read(stream_, buffer_, *parser_,
                         beast::bind_front_handler(&Session::OnUploadChunk,
                                                   this->shared_from_this()));
    }

    void OnUploadChunk(beast::error_code ec, std::size_t) {
        if (ec && ec != http::error::need_buffer) {
            nebulafs::core::LogError("Read upload failed: " + ec.message());
            return;
        }
        const auto bytes = body_buffer_.size() - parser_->get().body().size;
        if (bytes > 0) {
#ifdef _WIN32
            upload_stream_.write(body_buffer_.data(), bytes);
#else
            ssize_t written = ::write(upload_fd_, body_buffer_.data(), bytes);
            if (written < 0) {
                return FailUpload("failed to write temp file");
            }
#endif
            upload_hasher_->update(body_buffer_.data(), static_cast<unsigned int>(bytes));
            upload_total_ += static_cast<std::uint64_t>(bytes);
        }

        if (parser_->is_done()) {
            return FinishUpload();
        }
        DoReadUploadChunk();
    }

    void FinishUpload() {
#ifdef _WIN32
        upload_stream_.flush();
        upload_stream_.close();
#else
        ::fsync(upload_fd_);
        ::close(upload_fd_);
#endif
        // Atomic rename ensures readers never see partial files.
        const auto final_path = nebulafs::storage::LocalStorage::BuildObjectPath(
            storage_->base_path(), upload_bucket_, upload_object_);
        std::filesystem::create_directories(std::filesystem::path(final_path).parent_path());
        std::filesystem::rename(upload_temp_path_, final_path);

        nebulafs::metadata::ObjectMetadata meta;
        meta.name = upload_object_;
        meta.size_bytes = upload_total_;
        meta.etag = Poco::DigestEngine::digestToHex(upload_hasher_->digest());

        auto result = metadata_->UpsertObject(upload_bucket_, meta);
        if (!result.ok()) {
            auto response = ErrorResponse(http::status::internal_server_error,
                                          parser_->get().version(), "METADATA_ERROR",
                                          result.error().message, request_id_);
            return Send(std::move(response));
        }

        auto response = JsonResponse(http::status::ok, parser_->get().version(),
                                     "{\"etag\":\"" + meta.etag + "\",\"size\":" +
                                         std::to_string(meta.size_bytes) + "}");
        Send(std::move(response));
    }

    void FailUpload(const std::string& message) {
#ifdef _WIN32
        if (upload_stream_.is_open()) {
            upload_stream_.close();
        }
#else
        if (upload_fd_ >= 0) {
            ::close(upload_fd_);
        }
#endif
        std::filesystem::remove(upload_temp_path_);
        auto response = ErrorResponse(http::status::internal_server_error, parser_->get().version(),
                                      "IO_ERROR", message, request_id_);
        Send(std::move(response));
    }

    void HandleDownload(const nebulafs::http::HttpRequest& request, const std::string& path) {
        nebulafs::http::RouteParams params;
        nebulafs::http::Router::Match("/v1/buckets/{bucket}/objects/{object}", path, &params);
        const auto bucket = params["bucket"];
        const auto object = params["object"];

        auto storage_result = storage_->ReadObject(bucket, object);
        if (!storage_result.ok()) {
            auto response = ErrorResponse(http::status::not_found, request.version(),
                                          "OBJECT_NOT_FOUND", "object not found", request_id_);
            return Send(std::move(response));
        }

        beast::error_code ec;
        http::response<http::file_body> response{http::status::ok, request.version()};
        response.body().open(storage_result.value().path.c_str(), beast::file_mode::scan, ec);
        if (ec) {
            auto err = ErrorResponse(http::status::internal_server_error, request.version(),
                                     "IO_ERROR", "failed to open file", request_id_);
            return Send(std::move(err));
        }

        const auto size = response.body().size();
        response.set(http::field::content_type, "application/octet-stream");
        response.set(http::field::accept_ranges, "bytes");

        // Support HTTP Range for large object reads and resumable downloads.
        auto range_header = request[http::field::range];
        if (!range_header.empty()) {
            auto range = ParseRange(std::string(range_header), size);
            if (!range) {
                auto err = ErrorResponse(http::status::range_not_satisfiable, request.version(),
                                         "INVALID_RANGE", "invalid range", request_id_);
                err.set(http::field::content_range, "bytes */" + std::to_string(size));
                return Send(std::move(err));
            }
            response.result(http::status::partial_content);
            response.body().seek(range->start, ec);
            if (ec) {
                auto err = ErrorResponse(http::status::internal_server_error, request.version(),
                                         "IO_ERROR", "failed to seek file", request_id_);
                return Send(std::move(err));
            }
            const auto length = range->end - range->start + 1;
            response.content_length(length);
            response.set(http::field::content_range,
                         "bytes " + std::to_string(range->start) + "-" +
                             std::to_string(range->end) + "/" + std::to_string(size));
        } else {
            response.content_length(size);
        }

        Send(std::move(response));
    }

    template <typename Body>
    void Send(http::response<Body>&& response) {
        response.set(http::field::server, "NebulaFS");
        response.set("X-Request-Id", request_id_);
        const auto latency = std::chrono::duration_cast<std::chrono::milliseconds>(
                                 std::chrono::steady_clock::now() - request_start_)
                                 .count();
        nebulafs::core::LogRequest(request_id_, request_method_, request_target_, request_remote_,
                                   response.result_int(), latency);
        nebulafs::observability::RecordRequest(response.result_int(), latency);
        auto sp = std::make_shared<http::response<Body>>(std::move(response));
        http::async_write(stream_, *sp,
                          beast::bind_front_handler(&Session::OnWrite<Body>,
                                                    this->shared_from_this(), sp->need_eof(), sp));
    }

    template <typename Body>
    void OnWrite(bool close, std::shared_ptr<http::response<Body>>,
                 beast::error_code ec, std::size_t) {
        if (ec) {
            nebulafs::core::LogError("Write failed: " + ec.message());
            return;
        }
        if (close) {
            return DoClose();
        }
        DoReadHeader();
    }

    void DoClose() {
        beast::error_code ec;
        if constexpr (std::is_same_v<Stream, beast::ssl_stream<beast::tcp_stream>>) {
            stream_.shutdown(ec);
        } else {
            stream_.socket().shutdown(tcp::socket::shutdown_send, ec);
        }
    }

    std::string GetRemoteAddress() const {
        beast::error_code ec;
        auto endpoint = beast::get_lowest_layer(stream_).socket().remote_endpoint(ec);
        if (ec) {
            return "unknown";
        }
        return endpoint.address().to_string() + ":" + std::to_string(endpoint.port());
    }

    Stream stream_;
    beast::flat_buffer buffer_;
    std::optional<http::request_parser<http::buffer_body>> parser_;
    std::array<char, kBufferSize> body_buffer_{};

    nebulafs::http::Router router_;
    nebulafs::core::Config config_;
    std::shared_ptr<nebulafs::storage::LocalStorage> storage_;
    std::shared_ptr<nebulafs::metadata::MetadataStore> metadata_;
    std::shared_ptr<nebulafs::auth::JwtVerifier> auth_verifier_;

    std::string request_id_;
    std::string request_method_;
    std::string request_target_;
    std::string request_remote_;
    std::chrono::steady_clock::time_point request_start_{};
    std::string body_;
    std::optional<nebulafs::auth::JwtClaims> auth_claims_;

    std::string upload_bucket_;
    std::string upload_object_;
    std::string upload_temp_path_;
    std::optional<Poco::SHA2Engine256> upload_hasher_;
    std::uint64_t upload_total_{0};
#ifdef _WIN32
    std::ofstream upload_stream_;
#else
    int upload_fd_{-1};
#endif
};

class Listener : public std::enable_shared_from_this<Listener> {
public:
    Listener(net::io_context& ioc, tcp::endpoint endpoint, nebulafs::http::Router router,
             nebulafs::core::Config config,
             std::shared_ptr<nebulafs::storage::LocalStorage> storage,
             std::shared_ptr<nebulafs::metadata::MetadataStore> metadata,
             std::shared_ptr<nebulafs::auth::JwtVerifier> auth_verifier,
             net::ssl::context* ssl_ctx)
        : acceptor_(ioc),
          router_(std::move(router)),
          config_(std::move(config)),
          storage_(std::move(storage)),
          metadata_(std::move(metadata)),
          auth_verifier_(std::move(auth_verifier)),
          ssl_ctx_(ssl_ctx) {
        beast::error_code ec;
        acceptor_.open(endpoint.protocol(), ec);
        acceptor_.set_option(net::socket_base::reuse_address(true), ec);
        acceptor_.bind(endpoint, ec);
        acceptor_.listen(net::socket_base::max_listen_connections, ec);
    }

    void Run() { DoAccept(); }

private:
    void DoAccept() {
        acceptor_.async_accept(beast::bind_front_handler(&Listener::OnAccept,
                                                         shared_from_this()));
    }

    void OnAccept(beast::error_code ec, tcp::socket socket) {
        if (ec) {
            nebulafs::core::LogError("Accept failed: " + ec.message());
        } else {
            if (ssl_ctx_) {
                auto stream = beast::ssl_stream<beast::tcp_stream>(std::move(socket), *ssl_ctx_);
                std::make_shared<Session<beast::ssl_stream<beast::tcp_stream>>>(
                    std::move(stream), router_, config_, storage_, metadata_, auth_verifier_)
                    ->Start();
            } else {
                auto stream = beast::tcp_stream(std::move(socket));
                std::make_shared<Session<beast::tcp_stream>>(std::move(stream), router_, config_,
                                                             storage_, metadata_, auth_verifier_)
                    ->Start();
            }
        }
        DoAccept();
    }

    tcp::acceptor acceptor_;
    nebulafs::http::Router router_;
    nebulafs::core::Config config_;
    std::shared_ptr<nebulafs::storage::LocalStorage> storage_;
    std::shared_ptr<nebulafs::metadata::MetadataStore> metadata_;
    std::shared_ptr<nebulafs::auth::JwtVerifier> auth_verifier_;
    net::ssl::context* ssl_ctx_{nullptr};
};

}  // namespace

namespace nebulafs::http {

HttpServer::HttpServer(boost::asio::io_context& ioc, const core::Config& config, Router router,
                       std::shared_ptr<storage::LocalStorage> storage,
                       std::shared_ptr<metadata::MetadataStore> metadata)
    : ioc_(ioc),
      config_(config),
      router_(std::move(router)),
      storage_(std::move(storage)),
      metadata_(std::move(metadata)) {
    auth_verifier_ = std::make_shared<nebulafs::auth::JwtVerifier>(config_.auth);
    if (config_.server.tls.enabled) {
        ssl_context_ = std::make_unique<net::ssl::context>(net::ssl::context::tlsv12_server);
        ssl_context_->use_certificate_chain_file(config_.server.tls.certificate);
        ssl_context_->use_private_key_file(config_.server.tls.private_key, net::ssl::context::pem);
    }
}

void HttpServer::Run() {
    StartCleanupJob();

    const auto address = net::ip::make_address(config_.server.host);
    const tcp::endpoint endpoint{address, static_cast<unsigned short>(config_.server.port)};

    std::make_shared<Listener>(ioc_, endpoint, router_, config_, storage_, metadata_,
                               auth_verifier_,
                               ssl_context_ ? ssl_context_.get() : nullptr)
        ->Run();
}

void HttpServer::StartCleanupJob() {
    if (!config_.cleanup.enabled) {
        return;
    }
    cleanup_timer_ = std::make_unique<net::steady_timer>(ioc_);
    ScheduleCleanupSweep();
}

void HttpServer::ScheduleCleanupSweep() {
    if (!cleanup_timer_) {
        return;
    }
    cleanup_timer_->expires_after(
        std::chrono::seconds(config_.cleanup.sweep_interval_seconds));
    cleanup_timer_->async_wait([this](const beast::error_code& ec) {
        if (ec) {
            return;
        }
        RunCleanupSweep();
        ScheduleCleanupSweep();
    });
}

void HttpServer::RunCleanupSweep() {
    const auto cutoff = nebulafs::core::NowIso8601WithOffsetSeconds(
        -config_.cleanup.grace_period_seconds);
    auto expired = metadata_->ListExpiredMultipartUploads(
        cutoff, config_.cleanup.max_uploads_per_sweep);
    if (!expired.ok()) {
        nebulafs::core::LogError("Cleanup sweep failed to list uploads: " +
                                 expired.error().message);
        return;
    }

    for (const auto& upload : expired.value()) {
        (void)metadata_->UpdateMultipartUploadState(upload.upload_id, "expired");
        (void)metadata_->DeleteMultipartParts(upload.upload_id);
        (void)metadata_->DeleteMultipartUpload(upload.upload_id);

        std::error_code ec;
        const auto path = std::filesystem::path(storage_->temp_path()) / "multipart" /
                          upload.upload_id;
        std::filesystem::remove_all(path, ec);
    }
}

}  // namespace nebulafs::http
