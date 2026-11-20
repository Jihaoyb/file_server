#include "nebulafs/http/http_server.h"

#include <algorithm>
#include <array>
#include <cctype>
#include <charconv>
#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <memory>
#include <mutex>
#include <optional>
#include <sstream>
#include <stdexcept>
#include <string>
#include <type_traits>
#include <vector>

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
#include <openssl/crypto.h>

#include "nebulafs/core/ids.h"
#include "nebulafs/core/logger.h"
#include "nebulafs/core/time.h"
#include "nebulafs/distributed/http_client.h"
#include "nebulafs/observability/metrics.h"
#include "nebulafs/auth/jwt_utils.h"
#include "nebulafs/auth/jwt_verifier.h"
#include "nebulafs/storage/local_storage.h"
#include "nebulafs/metadata/sqlite_metadata_store.h"
#include "pilot_download_token.h"
#include "pilot_web_assets.h"

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
constexpr std::int64_t kDownloadLifetime = 24 * 60 * 60;
constexpr std::int64_t kFileRetention = 7 * kDownloadLifetime;

std::int64_t EpochSeconds() {
    return std::chrono::duration_cast<std::chrono::seconds>(
               std::chrono::system_clock::now().time_since_epoch()).count();
}

class DownloadFile : public beast::file {
public:
    void SetEnd(std::uint64_t end) { end_ = end; }

    std::uint64_t size(beast::error_code& ec) const {
        const auto size = beast::file::size(ec);
        return end_ ? std::min(size, *end_) : size;
    }

private:
    std::optional<std::uint64_t> end_;
};

std::optional<nebulafs::http::pilot_web::Asset> FindPilotAsset(http::verb method,
                                                              const std::string& path) {
    if (method != http::verb::get) {
        return std::nullopt;
    }
    for (const auto& asset : nebulafs::http::pilot_web::kAssets) {
        if (asset.path == path) {
            return asset;
        }
    }
    return std::nullopt;
}

bool IsPilotUploadRoute(http::verb method, const std::string& path) {
    using nebulafs::http::Router;
    constexpr auto kSession = "/v1/buckets/pilot/multipart-uploads/{upload_id}";
    switch (method) {
    case http::verb::post:
        return Router::Match("/v1/buckets/pilot/multipart-uploads", path, nullptr) ||
               Router::Match("/v1/buckets/pilot/multipart-uploads/{upload_id}/complete", path,
                             nullptr) ||
               Router::Match("/v1/buckets/pilot/multipart-uploads/{upload_id}/download-link", path,
                             nullptr);
    case http::verb::put:
        return Router::Match("/v1/buckets/pilot/multipart-uploads/{upload_id}/parts/{part_number}",
                             path, nullptr);
    case http::verb::get:
        return Router::Match("/v1/buckets/pilot/multipart-uploads/{upload_id}/parts", path,
                             nullptr) ||
               Router::Match("/v1/buckets/pilot/multipart-uploads/{upload_id}/status", path,
                             nullptr);
    case http::verb::delete_:
        return Router::Match(kSession, path, nullptr);
    default:
        return false;
    }
}

class RateLimiter {
public:
    RateLimiter(int rps, int burst)
        : enabled_(rps > 0),
          rate_per_second_(static_cast<double>(std::max(0, rps))),
          capacity_(static_cast<double>(std::max(0, burst > 0 ? burst : rps))),
          tokens_(capacity_),
          last_refill_(std::chrono::steady_clock::now()) {
    }

    bool Allow() {
        if (!enabled_) {
            return true;
        }

        std::lock_guard<std::mutex> lock(mu_);
        RefillLocked();
        if (tokens_ < 1.0) {
            return false;
        }
        tokens_ -= 1.0;
        return true;
    }

private:
    void RefillLocked() {
        const auto now = std::chrono::steady_clock::now();
        const auto elapsed =
            std::chrono::duration_cast<std::chrono::duration<double>>(now - last_refill_).count();
        last_refill_ = now;
        tokens_ = std::min(capacity_, tokens_ + elapsed * rate_per_second_);
    }

    bool enabled_{false};
    double rate_per_second_{0.0};
    double capacity_{0.0};
    double tokens_{0.0};
    std::chrono::steady_clock::time_point last_refill_{};
    std::mutex mu_;
};

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

std::string BlobUrl(const std::string& endpoint, const std::string& blob_id) {
    if (!endpoint.empty() && endpoint.back() == '/') {
        return endpoint.substr(0, endpoint.size() - 1) + "/internal/v1/blobs/" + blob_id;
    }
    return endpoint + "/internal/v1/blobs/" + blob_id;
}

struct DistributedPartLocator {
    std::string blob_id;
    std::vector<std::string> endpoints;
};

std::optional<DistributedPartLocator> DecodePartLocator(const std::string& encoded) {
    const auto sep = encoded.find('|');
    if (sep == std::string::npos) {
        return std::nullopt;
    }

    DistributedPartLocator locator;
    locator.blob_id = encoded.substr(0, sep);
    std::stringstream ss(encoded.substr(sep + 1));
    std::string endpoint;
    while (std::getline(ss, endpoint, ',')) {
        if (!endpoint.empty()) {
            locator.endpoints.push_back(endpoint);
        }
    }
    if (locator.blob_id.empty() || locator.endpoints.empty()) {
        return std::nullopt;
    }
    return locator;
}

std::optional<RangeRequest> ParseRange(const std::string& header, std::uint64_t size) {
    if (!header.starts_with("bytes=") || size == 0) {
        return std::nullopt;
    }
    const auto range = std::string_view(header).substr(6);
    auto dash = range.find('-');
    if (dash == std::string::npos) {
        return std::nullopt;
    }
    const auto parse_offset = [](std::string_view value, std::uint64_t& offset) {
        const auto end = value.data() + value.size();
        const auto parsed = std::from_chars(value.data(), end, offset);
        return parsed.ec == std::errc{} && parsed.ptr == end;
    };
    const auto start = range.substr(0, dash);
    const auto end = range.substr(dash + 1);
    RangeRequest request{0, size - 1};
    if (start.empty()) {
        std::uint64_t suffix_size = 0;
        if (!parse_offset(end, suffix_size) || suffix_size == 0) {
            return std::nullopt;
        }
        request.start = size - std::min(size, suffix_size);
        return request;
    }
    if (!parse_offset(start, request.start) || request.start >= size) {
        return std::nullopt;
    }
    if (!end.empty() && (!parse_offset(end, request.end) || request.end < request.start)) {
        return std::nullopt;
    }
    request.end = std::min(request.end, size - 1);
    return request;
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

bool IsTimeoutError(const beast::error_code& ec) {
    return ec == beast::error::timeout || ec == net::error::timed_out;
}

template <typename Stream>
class Session : public std::enable_shared_from_this<Session<Stream>> {
public:
    Session(Stream&& stream, nebulafs::http::Router router, nebulafs::core::Config config,
            std::shared_ptr<nebulafs::storage::StorageBackend> storage,
            std::shared_ptr<nebulafs::metadata::MetadataBackend> metadata,
            std::shared_ptr<nebulafs::auth::JwtVerifier> auth_verifier,
            std::shared_ptr<const std::string> pilot_upload_token,
            std::shared_ptr<const std::string> pilot_download_key,
            std::shared_ptr<RateLimiter> rate_limiter)
        : stream_(std::move(stream)),
          router_(std::move(router)),
          config_(std::move(config)),
          storage_(std::move(storage)),
          metadata_(std::move(metadata)),
          auth_verifier_(std::move(auth_verifier)),
          pilot_upload_token_(std::move(pilot_upload_token)),
          pilot_download_key_(std::move(pilot_download_key)),
          rate_limiter_(std::move(rate_limiter)) {
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
        beast::get_lowest_layer(stream_).expires_after(
            std::chrono::milliseconds(config_.server.limits.request_timeout_ms));
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
            if (IsTimeoutError(ec)) {
                nebulafs::observability::RecordTimedOut();
            }
            nebulafs::core::LogError("Read header failed: " + ec.message());
            return;
        }

        request_id_ = nebulafs::core::GenerateRequestId();
        // Clear any previous request state for reused sessions.
        auth_claims_.reset();
        request_start_ = std::chrono::steady_clock::now();
        timeout_response_sent_ = false;
        request_method_ = std::string(parser_->get().method_string());
        request_target_ = std::string(parser_->get().target());
        request_remote_ = GetRemoteAddress();
        const auto target = request_target_;
        const auto path = StripQuery(target);
        const auto method = parser_->get().method();

        if (rate_limiter_ && !rate_limiter_->Allow()) {
            nebulafs::observability::RecordRateLimited();
            auto response = ErrorResponse(http::status::too_many_requests, parser_->get().version(),
                                          "RATE_LIMITED", "rate limit exceeded", request_id_);
            // Keep Retry-After deterministic for clients and integration tests.
            response.set(http::field::retry_after, "1");
            response.keep_alive(false);
            return Send(std::move(response));
        }

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
        beast::get_lowest_layer(stream_).expires_after(
            std::chrono::milliseconds(config_.server.limits.request_timeout_ms));
        parser_->get().body().data = body_buffer_.data();
        parser_->get().body().size = body_buffer_.size();
        http::async_read(stream_, buffer_, *parser_,
                         beast::bind_front_handler(&Session::OnBodyChunk,
                                                   this->shared_from_this()));
    }

    void OnBodyChunk(beast::error_code ec, std::size_t) {
        if (ec && ec != http::error::need_buffer) {
            if (IsTimeoutError(ec)) {
                return SendRequestTimeout(parser_ ? parser_->get().version() : 11);
            }
            nebulafs::core::LogError("Read body failed: " + ec.message());
            return;
        }
        if (RequestTimedOut()) {
            return SendRequestTimeout(parser_->get().version());
        }
        const auto bytes = body_buffer_.size() - parser_->get().body().size;
        body_.append(body_buffer_.data(), bytes);
        if (parser_->is_done()) {
            return HandleRequest();
        }
        DoReadBodyChunk();
    }

    void HandleRequest() {
        if (RequestTimedOut()) {
            return SendRequestTimeout(parser_->get().version());
        }
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
        if (pilot_upload_token_) {
            auto asset = FindPilotAsset(request.method(), path);
            if (asset) {
                http::response<http::string_body> response{http::status::ok, request.version()};
                response.set(http::field::content_type, asset->content_type.data());
                // Keep intermediaries from injecting scripts into the pilot page.
                response.set(http::field::cache_control, "no-store, no-transform");
                response.set("X-Content-Type-Options", "nosniff");
                response.set("Referrer-Policy", "no-referrer");
                response.set("Content-Security-Policy",
                             "default-src 'none'; script-src 'self'; style-src 'self'; "
                             "worker-src 'self'; connect-src 'self'; base-uri 'none'; "
                             "form-action 'none'; frame-ancestors 'none'");
                response.body() = asset->body;
                response.prepare_payload();
                return Send(std::move(response));
            }
        }
        if (pilot_download_key_ && request.method() == http::verb::get &&
            path.starts_with("/d/")) {
            return HandlePilotDownload(request, path.substr(3));
        }
        nebulafs::http::RouteParams share_params;
        if (pilot_download_key_ && request.method() == http::verb::post &&
            nebulafs::http::Router::Match(
                "/v1/buckets/pilot/multipart-uploads/{upload_id}/download-link", path,
                &share_params)) {
            return CreatePilotDownload(request, share_params.at("upload_id"));
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
        // Keep health endpoints public; /metrics stays protected when auth is enabled.
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
        if (IsPublicPath(path) ||
            (pilot_download_key_ && request.method() == http::verb::get && path.starts_with("/d/")) ||
            (pilot_upload_token_ && FindPilotAsset(request.method(), path))) {
            return std::nullopt;
        }
        if (!config_.auth.enabled && !pilot_upload_token_) {
            return std::nullopt;
        }
        auto token = ExtractBearerToken(request);
        if (!token || token->empty()) {
            return ErrorResponse(http::status::unauthorized, request.version(), "UNAUTHORIZED",
                                 "missing bearer token", request_id_);
        }
        if (pilot_upload_token_ && IsPilotUploadRoute(request.method(), path) &&
            token->size() == pilot_upload_token_->size() &&
            CRYPTO_memcmp(token->data(), pilot_upload_token_->data(), token->size()) == 0) {
            auth_claims_ = nebulafs::auth::JwtClaims{
                "invite", "nebulafs:pilot", {}, {}};
            return std::nullopt;
        }
        if (!config_.auth.enabled) {
            return ErrorResponse(http::status::unauthorized, request.version(), "UNAUTHORIZED",
                                 "invalid upload code or unavailable route", request_id_);
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

        auto ensure_bucket = storage_->EnsureBucket(bucket);
        if (!ensure_bucket.ok()) {
            auto response = ErrorResponse(http::status::internal_server_error,
                                          parser_->get().version(), "STORAGE_ERROR",
                                          ensure_bucket.error().message, request_id_);
            return Send(std::move(response));
        }

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
        beast::get_lowest_layer(stream_).expires_after(
            std::chrono::milliseconds(config_.server.limits.request_timeout_ms));
        parser_->get().body().data = body_buffer_.data();
        parser_->get().body().size = body_buffer_.size();
        http::async_read(stream_, buffer_, *parser_,
                         beast::bind_front_handler(&Session::OnUploadChunk,
                                                   this->shared_from_this()));
    }

    void OnUploadChunk(beast::error_code ec, std::size_t) {
        if (ec && ec != http::error::need_buffer) {
            if (IsTimeoutError(ec)) {
                return SendRequestTimeout(parser_ ? parser_->get().version() : 11);
            }
            nebulafs::core::LogError("Read upload failed: " + ec.message());
            return;
        }
        if (RequestTimedOut()) {
            return SendRequestTimeout(parser_->get().version());
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
        std::ifstream input(upload_temp_path_, std::ios::binary);
        if (!input.is_open()) {
            auto response = ErrorResponse(http::status::internal_server_error,
                                          parser_->get().version(), "IO_ERROR",
                                          "failed to reopen upload temp file", request_id_);
            return Send(std::move(response));
        }
        auto stored = storage_->WriteObject(upload_bucket_, upload_object_, input);
        input.close();
        std::filesystem::remove(upload_temp_path_);
        if (!stored.ok()) {
            auto response =
                ErrorResponse(http::status::internal_server_error, parser_->get().version(),
                              "STORAGE_ERROR", stored.error().message, request_id_);
            return Send(std::move(response));
        }

        nebulafs::metadata::ObjectMetadata meta;
        meta.name = upload_object_;
        meta.size_bytes = stored.value().size_bytes;
        meta.etag = stored.value().etag;

        if (config_.server.mode != "distributed") {
            auto result = metadata_->UpsertObject(upload_bucket_, meta);
            if (!result.ok()) {
                auto response = ErrorResponse(http::status::internal_server_error,
                                              parser_->get().version(), "METADATA_ERROR",
                                              result.error().message, request_id_);
                return Send(std::move(response));
            }
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

        SendDownload(request, bucket, object);
    }

    std::optional<nebulafs::metadata::VerifiedUpload> FindPilotDownload(
        const std::string& upload_id, std::int64_t now) {
        const auto store = std::dynamic_pointer_cast<nebulafs::metadata::SqliteMetadataStore>(
            metadata_);
        if (!store) {
            return std::nullopt;
        }
        try {
            auto result = store->GetVerifiedUpload("pilot", upload_id);
            if (result.ok() && result.value().state == "completed" &&
                result.value().completed_at + kFileRetention > now) {
                return std::move(result.value());
            }
        } catch (const std::exception& e) {
            nebulafs::core::LogError("Could not read the verified download receipt: " +
                                     std::string(e.what()));
        }
        return std::nullopt;
    }

    void CreatePilotDownload(const nebulafs::http::HttpRequest& request,
                             const std::string& upload_id) {
        const auto now = EpochSeconds();
        const auto upload = FindPilotDownload(upload_id, now);
        if (!upload) {
            return PilotDownloadNotFound(request);
        }
        const auto expiry = std::min(now + kDownloadLifetime,
                                     upload->completed_at + kFileRetention);
        const auto token = nebulafs::http::pilot_download::Create(upload->upload_id, expiry,
                                                                 *pilot_download_key_);
        if (token.empty()) {
            return Send(ErrorResponse(http::status::internal_server_error, request.version(),
                                      "IO_ERROR", "could not create download link", request_id_));
        }
        auto response = JsonResponse(http::status::ok, request.version(),
                                     "{\"url\":\"/d/" + token + "\",\"expires_at\":" +
                                         std::to_string(expiry) + "}");
        response.set(http::field::cache_control, "no-store");
        Send(std::move(response));
    }

    void PilotDownloadNotFound(const nebulafs::http::HttpRequest& request) {
        auto response = ErrorResponse(http::status::not_found, request.version(),
                                      "DOWNLOAD_NOT_FOUND", "download unavailable", request_id_);
        response.set(http::field::cache_control, "no-store");
        Send(std::move(response));
    }

    void HandlePilotDownload(const nebulafs::http::HttpRequest& request, const std::string& token) {
        const auto now = EpochSeconds();
        const auto id = nebulafs::http::pilot_download::Verify(token, now, *pilot_download_key_);
        const auto upload = id ? FindPilotDownload(*id, now) : std::nullopt;
        if (!upload) {
            return PilotDownloadNotFound(request);
        }
        try {
            SendDownload(request, "pilot", upload->upload_id, upload->display_name);
        } catch (const std::filesystem::filesystem_error& e) {
            nebulafs::core::LogError("Could not read the verified file: " +
                                     std::string(e.what()));
            PilotDownloadNotFound(request);
        }
    }

    void SendDownload(const nebulafs::http::HttpRequest& request, const std::string& bucket,
                      const std::string& object, const std::string& display_name = {}) {
        auto storage_result = storage_->ReadObject(bucket, object);
        if (!storage_result.ok()) {
            auto response = ErrorResponse(http::status::not_found, request.version(),
                                          "OBJECT_NOT_FOUND", "object not found", request_id_);
            return Send(std::move(response));
        }

        beast::error_code ec;
        http::response<http::basic_file_body<DownloadFile>> response{http::status::ok,
                                                                    request.version()};
        response.body().open(storage_result.value().path.c_str(), beast::file_mode::scan, ec);
        if (ec) {
            auto err = ErrorResponse(http::status::internal_server_error, request.version(),
                                     "IO_ERROR", "failed to open file", request_id_);
            return Send(std::move(err));
        }

        const auto size = response.body().size();
        response.set(http::field::content_type, "application/octet-stream");
        response.set(http::field::accept_ranges, "bytes");
        if (!display_name.empty()) {
            // Encode the sender-controlled filename before placing it in a response header.
            constexpr std::string_view hex = "0123456789ABCDEF";
            std::string disposition = "attachment; filename*=UTF-8''";
            for (const auto ch : display_name) {
                const auto byte = static_cast<unsigned char>(ch);
                disposition.push_back('%');
                disposition.push_back(hex[byte >> 4]);
                disposition.push_back(hex[byte & 15]);
            }
            response.set(http::field::content_disposition, disposition);
            response.set(http::field::cache_control, "private, no-store");
            response.set("X-Content-Type-Options", "nosniff");
            response.set("Referrer-Policy", "no-referrer");
        }


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
            // Bound the serializer as well as Content-Length for a closed byte range.
            response.body().file().SetEnd(range->end + 1);
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
        // Disable read deadline while writing a response to avoid write-side timeout races.
        beast::get_lowest_layer(stream_).expires_never();
        response.set(http::field::server, "NebulaFS");
        response.set("X-Request-Id", request_id_);
        const auto latency = std::chrono::duration_cast<std::chrono::milliseconds>(
                                 std::chrono::steady_clock::now() - request_start_)
                                 .count();
        nebulafs::core::LogRequest(request_id_, request_method_,
                                   request_target_.starts_with("/d/") ? "/d/[redacted]" : request_target_,
                                   request_remote_,
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

    bool RequestTimedOut() const {
        const auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
                                 std::chrono::steady_clock::now() - request_start_)
                                 .count();
        return elapsed > config_.server.limits.request_timeout_ms;
    }

    void SendRequestTimeout(int version) {
        if (timeout_response_sent_) {
            return;
        }
        timeout_response_sent_ = true;
        nebulafs::observability::RecordTimedOut();
        auto response = ErrorResponse(http::status::request_timeout, version, "REQUEST_TIMEOUT",
                                      "request exceeded timeout budget", request_id_);
        response.keep_alive(false);
        Send(std::move(response));
    }

    Stream stream_;
    beast::flat_buffer buffer_;
    std::optional<http::request_parser<http::buffer_body>> parser_;
    std::array<char, kBufferSize> body_buffer_{};

    nebulafs::http::Router router_;
    nebulafs::core::Config config_;
    std::shared_ptr<nebulafs::storage::StorageBackend> storage_;
    std::shared_ptr<nebulafs::metadata::MetadataBackend> metadata_;
    std::shared_ptr<nebulafs::auth::JwtVerifier> auth_verifier_;
    std::shared_ptr<const std::string> pilot_upload_token_;
    std::shared_ptr<const std::string> pilot_download_key_;
    std::shared_ptr<RateLimiter> rate_limiter_;

    std::string request_id_;
    std::string request_method_;
    std::string request_target_;
    std::string request_remote_;
    std::chrono::steady_clock::time_point request_start_{};
    bool timeout_response_sent_{false};
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
             std::shared_ptr<nebulafs::storage::StorageBackend> storage,
             std::shared_ptr<nebulafs::metadata::MetadataBackend> metadata,
             std::shared_ptr<nebulafs::auth::JwtVerifier> auth_verifier,
             std::shared_ptr<const std::string> pilot_upload_token,
            std::shared_ptr<const std::string> pilot_download_key,
             std::shared_ptr<RateLimiter> rate_limiter,
             net::ssl::context* ssl_ctx)
        : acceptor_(ioc),
          router_(std::move(router)),
          config_(std::move(config)),
          storage_(std::move(storage)),
          metadata_(std::move(metadata)),
          auth_verifier_(std::move(auth_verifier)),
          pilot_upload_token_(std::move(pilot_upload_token)),
          pilot_download_key_(std::move(pilot_download_key)),
          rate_limiter_(std::move(rate_limiter)),
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
                    std::move(stream), router_, config_, storage_, metadata_, auth_verifier_,
                    pilot_upload_token_, pilot_download_key_, rate_limiter_)
                    ->Start();
            } else {
                auto stream = beast::tcp_stream(std::move(socket));
                std::make_shared<Session<beast::tcp_stream>>(std::move(stream), router_, config_,
                                                             storage_, metadata_, auth_verifier_,
                                                             pilot_upload_token_, pilot_download_key_, rate_limiter_)
                    ->Start();
            }
        }
        DoAccept();
    }

    tcp::acceptor acceptor_;
    nebulafs::http::Router router_;
    nebulafs::core::Config config_;
    std::shared_ptr<nebulafs::storage::StorageBackend> storage_;
    std::shared_ptr<nebulafs::metadata::MetadataBackend> metadata_;
    std::shared_ptr<nebulafs::auth::JwtVerifier> auth_verifier_;
    std::shared_ptr<const std::string> pilot_upload_token_;
    std::shared_ptr<const std::string> pilot_download_key_;
    std::shared_ptr<RateLimiter> rate_limiter_;
    net::ssl::context* ssl_ctx_{nullptr};
};

}  // namespace

namespace nebulafs::http {

HttpServer::HttpServer(boost::asio::io_context& ioc, const core::Config& config, Router router,
                       std::shared_ptr<storage::StorageBackend> storage,
                       std::shared_ptr<metadata::MetadataBackend> metadata)
    : ioc_(ioc),
      config_(config),
      router_(std::move(router)),
      storage_(std::move(storage)),
      metadata_(std::move(metadata)) {
    auth_verifier_ = std::make_shared<nebulafs::auth::JwtVerifier>(config_.auth);
    if (const char* raw_token = std::getenv("NEBULAFS_PILOT_UPLOAD_TOKEN")) {
        std::string token(raw_token);
        if (token.size() != 64 || token.find_first_not_of("0123456789abcdefABCDEF") !=
                                      std::string::npos ||
            config_.server.mode != "single_node") {
            throw std::invalid_argument(
                "NEBULAFS_PILOT_UPLOAD_TOKEN requires a single-node server and 64 hex digits");
        }
        pilot_upload_token_ = std::make_shared<const std::string>(std::move(token));
    }
    if (const char* raw_key = std::getenv("NEBULAFS_PILOT_DOWNLOAD_KEY")) {
        std::string key(raw_key);
        if (!pilot_upload_token_ || key == *pilot_upload_token_ || key.size() != 64 ||
            key.find_first_not_of("0123456789abcdefABCDEF") != std::string::npos) {
            throw std::invalid_argument(
                "NEBULAFS_PILOT_DOWNLOAD_KEY requires an independent 64-digit hex key");
        }
        pilot_download_key_ = std::make_shared<const std::string>(std::move(key));
    }
    if (config_.server.tls.enabled) {
        ssl_context_ = std::make_unique<net::ssl::context>(net::ssl::context::tlsv12_server);
        ssl_context_->use_certificate_chain_file(config_.server.tls.certificate);
        ssl_context_->use_private_key_file(config_.server.tls.private_key, net::ssl::context::pem);
    }
}

void HttpServer::Run() {
    StartCleanupJob();
    auto rate_limiter = std::make_shared<RateLimiter>(config_.server.limits.rate_limit_rps,
                                                      config_.server.limits.rate_limit_burst);

    const auto address = net::ip::make_address(config_.server.host);
    const tcp::endpoint endpoint{address, static_cast<unsigned short>(config_.server.port)};

    std::make_shared<Listener>(ioc_, endpoint, router_, config_, storage_, metadata_,
                               auth_verifier_, pilot_upload_token_, pilot_download_key_, rate_limiter,
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
        try {
            RunCleanupSweep();
        } catch (const std::exception& e) {
            core::LogError("Cleanup sweep failed: " + std::string(e.what()));
        }
        ScheduleCleanupSweep();
    });
}

void HttpServer::RunCleanupSweep() {
    const auto sqlite = std::dynamic_pointer_cast<metadata::SqliteMetadataStore>(metadata_);
    if (sqlite) {
        try {
            const auto completed = sqlite->ListExpiredVerifiedCompletions(
                EpochSeconds() - kFileRetention, config_.cleanup.max_uploads_per_sweep);
            for (const auto& id : completed) {
                const std::lock_guard lock(sqlite->upload_mutex());
                std::error_code ec;
                std::filesystem::remove_all(std::filesystem::path(storage_->temp_path()) /
                                                "multipart" / id, ec);
                if (!ec) {
                    std::filesystem::remove(storage::LocalStorage::BuildObjectPath(
                                                storage_->base_path(), "pilot", id), ec);
                }
                if (ec) {
                    core::LogError("Could not remove expired verified file: " + ec.message());
                    continue;
                }
                sqlite->DeleteExpiredVerifiedCompletion(id);
            }
        } catch (const std::exception& e) {
            core::LogError("Verified file retention sweep failed: " + std::string(e.what()));
        }
    }
    const auto cutoff = nebulafs::core::NowIso8601WithOffsetSeconds(
        -config_.cleanup.grace_period_seconds);
    auto expired = [&] {
        std::unique_lock<std::mutex> upload_lock;
        if (sqlite) {
            upload_lock = std::unique_lock(sqlite->upload_mutex());
        }
        return metadata_->ListExpiredMultipartUploads(cutoff,
                                                      config_.cleanup.max_uploads_per_sweep);
    }();
    if (!expired.ok()) {
        nebulafs::core::LogError("Cleanup sweep failed to list uploads: " +
                                 expired.error().message);
        return;
    }

    for (const auto& upload : expired.value()) {
        std::unique_lock<std::mutex> upload_lock;
        if (sqlite) {
            upload_lock = std::unique_lock(sqlite->upload_mutex());
            const auto current = metadata_->GetMultipartUpload(upload.upload_id);
            if (!current.ok() || current.value().expires_at >= cutoff ||
                (current.value().state != "initiated" && current.value().state != "uploading")) {
                continue;
            }
            if (sqlite->GetVerifiedUpload("pilot", upload.upload_id).ok()) {
                std::error_code ec;
                const auto staging = std::filesystem::path(storage_->temp_path()) /
                                     "multipart" / upload.upload_id;
                std::filesystem::remove_all(staging, ec);
                if (ec) {
                    core::LogError("Could not remove expired verified staging: " + ec.message());
                    continue;
                }
                const auto removed = sqlite->DeleteMultipartUpload(upload.upload_id);
                if (!removed.ok()) {
                    core::LogError("Could not remove expired verified upload metadata: " +
                                   removed.error().message);
                }
                continue;
            }
        }
        bool upload_success = true;
        if (config_.server.mode == "distributed") {
            auto parts = metadata_->ListMultipartParts(upload.upload_id);
            if (!parts.ok()) {
                upload_success = false;
                nebulafs::core::LogError("Cleanup sweep failed to list upload parts for " +
                                         upload.upload_id + ": " + parts.error().message);
            } else {
                for (const auto& part : parts.value()) {
                    // Distributed multipart parts encode blob id + replica endpoints in temp_path.
                    auto locator = DecodePartLocator(part.temp_path);
                    if (!locator.has_value()) {
                        upload_success = false;
                        nebulafs::core::LogError(
                            "Cleanup sweep found invalid distributed part locator for upload " +
                            upload.upload_id + ", part " + std::to_string(part.part_number));
                        continue;
                    }
                    for (const auto& endpoint : locator->endpoints) {
                        auto del = distributed::SendHttpRequest(
                            "DELETE", BlobUrl(endpoint, locator->blob_id), "", "",
                            config_.distributed.service_auth_token, {});
                        const bool success =
                            del.ok() && (del.value().status == 200 || del.value().status == 404);
                        observability::RecordGatewayDistributedCleanupBlobDelete(success);
                        if (!success) {
                            upload_success = false;
                            if (!del.ok()) {
                                nebulafs::core::LogError(
                                    "Cleanup sweep failed to delete blob replica for upload " +
                                    upload.upload_id + ": " + del.error().message);
                            } else {
                                nebulafs::core::LogError(
                                    "Cleanup sweep delete returned status " +
                                    std::to_string(del.value().status) + " for upload " +
                                    upload.upload_id);
                            }
                        }
                    }
                }
            }
        }

        auto mark_expired = metadata_->UpdateMultipartUploadState(upload.upload_id, "expired");
        if (!mark_expired.ok()) {
            upload_success = false;
            nebulafs::core::LogError("Cleanup sweep failed to mark upload expired for " +
                                     upload.upload_id + ": " + mark_expired.error().message);
        }
        auto delete_parts = metadata_->DeleteMultipartParts(upload.upload_id);
        if (!delete_parts.ok()) {
            upload_success = false;
            nebulafs::core::LogError("Cleanup sweep failed to delete multipart parts for " +
                                     upload.upload_id + ": " + delete_parts.error().message);
        }
        auto delete_upload = metadata_->DeleteMultipartUpload(upload.upload_id);
        if (!delete_upload.ok()) {
            upload_success = false;
            nebulafs::core::LogError("Cleanup sweep failed to delete multipart upload " +
                                     upload.upload_id + ": " + delete_upload.error().message);
        }

        std::error_code ec;
        const auto path = std::filesystem::path(storage_->temp_path()) / "multipart" /
                          upload.upload_id;
        std::filesystem::remove_all(path, ec);

        if (config_.server.mode == "distributed") {
            observability::RecordGatewayDistributedCleanupUpload(upload_success);
        }
    }
}

}  // namespace nebulafs::http
