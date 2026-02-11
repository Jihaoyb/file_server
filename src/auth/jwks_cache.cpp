#include "nebulafs/auth/jwks_cache.h"

#include <fstream>
#include <mutex>
#include <cctype>
#include <utility>

#include <Poco/JSON/Array.h>
#include <Poco/JSON/Object.h>
#include <Poco/JSON/Parser.h>
#include <Poco/Net/HTTPClientSession.h>
#include <Poco/Net/HTTPRequest.h>
#include <Poco/Net/HTTPResponse.h>
#include <Poco/Net/HTTPSClientSession.h>
#include <Poco/Net/SSLManager.h>
#include <Poco/Net/Context.h>
#include <Poco/Net/RejectCertificateHandler.h>
#include <Poco/StreamCopier.h>
#include <Poco/URI.h>

#include <openssl/rsa.h>

#include "nebulafs/auth/jwt_utils.h"
#include "nebulafs/core/error.h"

namespace nebulafs::auth {

namespace {

JwksCache::KeyPtr MakeRsaKey(const std::string& n_b64u, const std::string& e_b64u) {
    // Build RSA public key from base64url-encoded modulus/exponent.
    auto n_bytes = Base64UrlDecode(n_b64u);
    auto e_bytes = Base64UrlDecode(e_b64u);
    if (n_bytes.empty() || e_bytes.empty()) {
        return {};
    }

    BIGNUM* n = BN_bin2bn(n_bytes.data(), static_cast<int>(n_bytes.size()), nullptr);
    BIGNUM* e = BN_bin2bn(e_bytes.data(), static_cast<int>(e_bytes.size()), nullptr);
    if (!n || !e) {
        if (n) BN_free(n);
        if (e) BN_free(e);
        return {};
    }

    RSA* rsa = RSA_new();
    if (!rsa) {
        BN_free(n);
        BN_free(e);
        return {};
    }
    if (RSA_set0_key(rsa, n, e, nullptr) != 1) {
        RSA_free(rsa);
        BN_free(n);
        BN_free(e);
        return {};
    }

    EVP_PKEY* pkey = EVP_PKEY_new();
    if (!pkey) {
        RSA_free(rsa);
        return {};
    }
    if (EVP_PKEY_assign_RSA(pkey, rsa) != 1) {
        EVP_PKEY_free(pkey);
        RSA_free(rsa);
        return {};
    }

    return JwksCache::KeyPtr(pkey, EVP_PKEY_free);
}

}  // namespace

JwksCache::JwksCache(std::string url, std::chrono::seconds ttl)
    : url_(std::move(url)), ttl_(ttl) {}

core::Result<JwksCache::KeyPtr> JwksCache::GetKey(const std::string& kid) {
    // Fast-path cached key; refresh on expiry or unknown kid.
    std::lock_guard<std::mutex> lock(mutex_);
    const auto now = std::chrono::system_clock::now();
    if (keys_.empty() || now >= expires_at_) {
        auto refresh = Refresh();
        if (!refresh.ok()) {
            return refresh.error();
        }
    }

    auto it = keys_.find(kid);
    if (it != keys_.end()) {
        return it->second;
    }

    auto refresh = Refresh();
    if (!refresh.ok()) {
        return refresh.error();
    }
    it = keys_.find(kid);
    if (it == keys_.end()) {
        return core::Error{core::ErrorCode::kUnauthorized, "kid not found in jwks"};
    }
    return it->second;
}

core::Result<void> JwksCache::Refresh() {
    // Refresh cache from JWKS endpoint or local file.
    auto body_result = FetchJwksBody();
    if (!body_result.ok()) {
        return body_result.error();
    }
    auto load = LoadFromBody(body_result.value());
    if (!load.ok()) {
        return load.error();
    }
    expires_at_ = std::chrono::system_clock::now() + ttl_;
    return core::Ok();
}

core::Result<void> JwksCache::LoadFromBody(const std::string& body) {
    // Parse JWKS and keep only RSA keys with kid.
    try {
        Poco::JSON::Parser parser;
        auto root = parser.parse(body).extract<Poco::JSON::Object::Ptr>();
        auto keys = root->getArray("keys");
        if (!keys) {
            return core::Error{core::ErrorCode::kUnauthorized, "jwks keys missing"};
        }

        std::unordered_map<std::string, KeyPtr> next;
        for (size_t i = 0; i < keys->size(); ++i) {
            auto obj = keys->getObject(i);
            if (!obj) {
                continue;
            }
            const auto kty = obj->getValue<std::string>("kty");
            if (kty != "RSA") {
                continue;
            }
            const auto kid = obj->getValue<std::string>("kid");
            const auto n = obj->getValue<std::string>("n");
            const auto e = obj->getValue<std::string>("e");
            auto key = MakeRsaKey(n, e);
            if (!kid.empty() && key) {
                next.emplace(kid, std::move(key));
            }
        }

        if (next.empty()) {
            return core::Error{core::ErrorCode::kUnauthorized, "jwks contained no rsa keys"};
        }
        keys_ = std::move(next);
        return core::Ok();
    } catch (const std::exception& ex) {
        return core::Error{core::ErrorCode::kUnauthorized, ex.what()};
    }
}

core::Result<std::string> JwksCache::FetchJwksBody() {
    // Support file:// for tests/dev and http(s) for real providers.
    if (url_.empty()) {
        return core::Error{core::ErrorCode::kUnauthorized, "jwks url missing"};
    }

    // Parse file URLs manually first to avoid platform-specific URI parser edge cases.
    if (url_.rfind("file://", 0) == 0) {
        std::string path = url_.substr(7);
#ifdef _WIN32
        // Normalize file:///C:/... to C:/... for Windows filesystem APIs.
        if (path.size() >= 3 && path[0] == '/' &&
            std::isalpha(static_cast<unsigned char>(path[1])) && path[2] == ':') {
            path.erase(0, 1);
        }
#endif
        std::ifstream in(path);
        if (!in.is_open()) {
            return core::Error{core::ErrorCode::kUnauthorized, "failed to open jwks file"};
        }
        std::string body((std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>());
        return body;
    }

    // Allow direct filesystem paths without file:// scheme for local tests.
    if (!url_.empty() &&
        (url_.front() == '/'
#ifdef _WIN32
         || (url_.size() >= 2 &&
             std::isalpha(static_cast<unsigned char>(url_[0])) && url_[1] == ':')
#endif
        )) {
        std::ifstream in(url_);
        if (!in.is_open()) {
            return core::Error{core::ErrorCode::kUnauthorized, "failed to open jwks file"};
        }
        std::string body((std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>());
        return body;
    }

    Poco::URI uri(url_);
    const auto scheme = uri.getScheme();

    const std::string host = uri.getHost();
    const std::string path = uri.getPathEtc().empty() ? "/" : uri.getPathEtc();
    const bool is_https = scheme == "https";
    const int port = uri.getPort() > 0 ? uri.getPort() : (is_https ? 443 : 80);

    try {
        std::unique_ptr<Poco::Net::HTTPClientSession> session;
        if (is_https) {
            // Initialize Poco SSL once for HTTPS JWKS fetches.
            static std::once_flag ssl_once;
            std::call_once(ssl_once, []() {
                Poco::Net::initializeSSL();
                Poco::Net::Context::Ptr context =
                    new Poco::Net::Context(Poco::Net::Context::CLIENT_USE, "", "", "",
                                           Poco::Net::Context::VERIFY_STRICT);
                Poco::SharedPtr<Poco::Net::InvalidCertificateHandler> handler(
                    new Poco::Net::RejectCertificateHandler(false));
                Poco::Net::SSLManager::instance().initializeClient(nullptr, handler, context);
            });
            session = std::make_unique<Poco::Net::HTTPSClientSession>(host, port);
        } else {
            session = std::make_unique<Poco::Net::HTTPClientSession>(host, port);
        }

        Poco::Net::HTTPRequest req(Poco::Net::HTTPRequest::HTTP_GET, path,
                                   Poco::Net::HTTPMessage::HTTP_1_1);
        req.set("Host", host);
        req.set("User-Agent", "nebulafs-jwks-cache");
        session->sendRequest(req);

        Poco::Net::HTTPResponse res;
        std::istream& rs = session->receiveResponse(res);
        if (res.getStatus() != Poco::Net::HTTPResponse::HTTP_OK) {
            return core::Error{core::ErrorCode::kUnauthorized, "jwks fetch failed"};
        }
        std::string body;
        Poco::StreamCopier::copyToString(rs, body);
        return body;
    } catch (const std::exception& ex) {
        return core::Error{core::ErrorCode::kUnauthorized, ex.what()};
    }
}

}  // namespace nebulafs::auth
