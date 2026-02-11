#include "nebulafs/auth/jwt_verifier.h"

#include <chrono>

#include <Poco/JSON/Array.h>
#include <Poco/JSON/Object.h>
#include <Poco/JSON/Parser.h>
#include <Poco/Dynamic/Var.h>

#include <openssl/evp.h>

#include "nebulafs/auth/jwt_utils.h"
#include "nebulafs/core/error.h"

namespace nebulafs::auth {

namespace {

bool ContainsAudience(const std::vector<std::string>& aud, const std::string& expected) {
    // Minimal audience match helper.
    for (const auto& item : aud) {
        if (item == expected) {
            return true;
        }
    }
    return false;
}

std::vector<std::string> ParseAudience(const Poco::Dynamic::Var& var) {
    // "aud" can be a string or array per JWT spec.
    std::vector<std::string> aud;
    if (var.isString()) {
        aud.push_back(var.convert<std::string>());
        return aud;
    }
    if (var.type() == typeid(Poco::JSON::Array::Ptr)) {
        auto arr = var.extract<Poco::JSON::Array::Ptr>();
        if (arr) {
            for (size_t i = 0; i < arr->size(); ++i) {
                aud.push_back(arr->getElement<std::string>(i));
            }
        }
    }
    return aud;
}

std::vector<std::string> ParseScopes(const Poco::JSON::Object::Ptr& payload) {
    // Support "scope" (space-delimited) and "scp" (array).
    std::vector<std::string> scopes;
    if (!payload) {
        return scopes;
    }
    if (payload->has("scope")) {
        auto scope_str = payload->getValue<std::string>("scope");
        for (const auto& part : Split(scope_str, ' ')) {
            if (!part.empty()) {
                scopes.push_back(part);
            }
        }
    }
    if (payload->has("scp")) {
        auto var = payload->get("scp");
        if (var.type() == typeid(Poco::JSON::Array::Ptr)) {
            auto arr = var.extract<Poco::JSON::Array::Ptr>();
            if (arr) {
                for (size_t i = 0; i < arr->size(); ++i) {
                    scopes.push_back(arr->getElement<std::string>(i));
                }
            }
        }
    }
    return scopes;
}

core::Result<void> VerifySignature(const std::string& message,
                                   const std::string& signature_b64u,
                                   const JwksCache::KeyPtr& key) {
    // Verify RS256 signature over "header.payload".
    if (!key) {
        return core::Error{core::ErrorCode::kUnauthorized, "missing jwk key"};
    }
    auto signature = Base64UrlDecode(signature_b64u);
    if (signature.empty()) {
        return core::Error{core::ErrorCode::kUnauthorized, "invalid signature encoding"};
    }

    EVP_MD_CTX* ctx = EVP_MD_CTX_new();
    if (!ctx) {
        return core::Error{core::ErrorCode::kInternal, "crypto ctx init failed"};
    }

    int ok = EVP_DigestVerifyInit(ctx, nullptr, EVP_sha256(), nullptr, key.get());
    if (ok != 1) {
        EVP_MD_CTX_free(ctx);
        return core::Error{core::ErrorCode::kUnauthorized, "signature init failed"};
    }
    ok = EVP_DigestVerify(ctx,
                          signature.data(),
                          signature.size(),
                          reinterpret_cast<const unsigned char*>(message.data()),
                          message.size());
    EVP_MD_CTX_free(ctx);
    if (ok != 1) {
        return core::Error{core::ErrorCode::kUnauthorized, "signature verification failed"};
    }
    return core::Ok();
}

}  // namespace

JwtVerifier::JwtVerifier(core::AuthConfig config)
    : config_(std::move(config)),
      jwks_(std::make_shared<JwksCache>(config_.jwks_url,
                                        std::chrono::seconds(config_.cache_ttl_seconds))) {}

core::Result<JwtClaims> JwtVerifier::Verify(const std::string& token) {
    // Reject when auth disabled to avoid accidental enforcement.
    if (!config_.enabled) {
        return JwtClaims{};
    }
    auto parts = Split(token, '.');
    if (parts.size() != 3) {
        return core::Error{core::ErrorCode::kUnauthorized, "invalid token format"};
    }

    const auto& header_b64 = parts[0];
    const auto& payload_b64 = parts[1];
    const auto& signature_b64 = parts[2];

    Poco::JSON::Parser parser;
    Poco::JSON::Object::Ptr header;
    Poco::JSON::Object::Ptr payload;
    try {
        header = parser.parse(Base64UrlDecodeToString(header_b64)).extract<Poco::JSON::Object::Ptr>();
        payload = parser.parse(Base64UrlDecodeToString(payload_b64)).extract<Poco::JSON::Object::Ptr>();
    } catch (const std::exception& ex) {
        return core::Error{core::ErrorCode::kUnauthorized, ex.what()};
    }

    const auto alg = header->getValue<std::string>("alg");
    if (alg != config_.allowed_alg) {
        return core::Error{core::ErrorCode::kUnauthorized, "unsupported alg"};
    }
    const auto kid = header->getValue<std::string>("kid");
    if (kid.empty()) {
        return core::Error{core::ErrorCode::kUnauthorized, "missing kid"};
    }

    const auto issuer = payload->getValue<std::string>("iss");
    if (!config_.issuer.empty() && issuer != config_.issuer) {
        return core::Error{core::ErrorCode::kUnauthorized, "issuer mismatch"};
    }

    std::vector<std::string> aud;
    if (payload->has("aud")) {
        aud = ParseAudience(payload->get("aud"));
    }
    if (!config_.audience.empty()) {
        if (aud.empty() || !ContainsAudience(aud, config_.audience)) {
            return core::Error{core::ErrorCode::kUnauthorized, "audience mismatch"};
        }
    }

    const auto now = std::chrono::system_clock::now();
    const auto now_sec =
        std::chrono::duration_cast<std::chrono::seconds>(now.time_since_epoch()).count();
    const long skew = config_.clock_skew_seconds;

    if (!payload->has("exp")) {
        return core::Error{core::ErrorCode::kUnauthorized, "missing exp"};
    }
    const auto exp = payload->getValue<long>("exp");
    if (now_sec > exp + skew) {
        return core::Error{core::ErrorCode::kUnauthorized, "token expired"};
    }
    if (payload->has("nbf")) {
        const auto nbf = payload->getValue<long>("nbf");
        if (now_sec + skew < nbf) {
            return core::Error{core::ErrorCode::kUnauthorized, "token not yet valid"};
        }
    }

    const auto message = header_b64 + "." + payload_b64;
    auto key_result = jwks_->GetKey(kid);
    if (!key_result.ok()) {
        return key_result.error();
    }
    auto verify = VerifySignature(message, signature_b64, key_result.value());
    if (!verify.ok()) {
        return verify.error();
    }

    JwtClaims claims;
    if (payload->has("sub")) {
        claims.subject = payload->getValue<std::string>("sub");
    }
    claims.issuer = issuer;
    claims.audience = aud;
    claims.scopes = ParseScopes(payload);
    return claims;
}

}  // namespace nebulafs::auth
