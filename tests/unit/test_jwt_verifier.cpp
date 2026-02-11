#include <chrono>
#include <filesystem>
#include <fstream>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include <Poco/UUIDGenerator.h>

#include <openssl/bn.h>
#include <openssl/evp.h>
#include <openssl/rsa.h>

#include "nebulafs/auth/jwt_verifier.h"
#include "nebulafs/core/config.h"

namespace {

using KeyPtr = std::unique_ptr<EVP_PKEY, decltype(&EVP_PKEY_free)>;

KeyPtr GenerateKey() {
    // Generate small RSA key for unit test signing.
    BIGNUM* e = BN_new();
    if (!e) {
        return KeyPtr(nullptr, EVP_PKEY_free);
    }
    BN_set_word(e, RSA_F4);

    RSA* rsa = RSA_new();
    if (!rsa) {
        BN_free(e);
        return KeyPtr(nullptr, EVP_PKEY_free);
    }
    RSA_generate_key_ex(rsa, 1024, e, nullptr);
    BN_free(e);

    EVP_PKEY* pkey = EVP_PKEY_new();
    if (!pkey) {
        RSA_free(rsa);
        return KeyPtr(nullptr, EVP_PKEY_free);
    }
    EVP_PKEY_assign_RSA(pkey, rsa);
    return KeyPtr(pkey, EVP_PKEY_free);
}

std::string Base64UrlEncode(const unsigned char* data, size_t len) {
    // Convert to base64url without padding.
    std::string b64((len + 2) / 3 * 4, '\0');
    int out_len = EVP_EncodeBlock(reinterpret_cast<unsigned char*>(&b64[0]), data,
                                  static_cast<int>(len));
    b64.resize(static_cast<size_t>(out_len));
    for (auto& c : b64) {
        if (c == '+') c = '-';
        else if (c == '/') c = '_';
    }
    while (!b64.empty() && b64.back() == '=') {
        b64.pop_back();
    }
    return b64;
}

std::string Base64UrlEncode(const std::string& input) {
    return Base64UrlEncode(reinterpret_cast<const unsigned char*>(input.data()), input.size());
}

std::string BuildJwks(const std::string& kid, EVP_PKEY* pkey) {
    // Emit JWKS with one RSA key.
    const RSA* rsa = EVP_PKEY_get0_RSA(pkey);
    const BIGNUM* n = nullptr;
    const BIGNUM* e = nullptr;
    RSA_get0_key(rsa, &n, &e, nullptr);

    std::vector<unsigned char> n_buf(static_cast<size_t>(BN_num_bytes(n)));
    std::vector<unsigned char> e_buf(static_cast<size_t>(BN_num_bytes(e)));
    BN_bn2bin(n, n_buf.data());
    BN_bn2bin(e, e_buf.data());

    const auto n_b64 = Base64UrlEncode(n_buf.data(), n_buf.size());
    const auto e_b64 = Base64UrlEncode(e_buf.data(), e_buf.size());

    return std::string("{\"keys\":[{\"kty\":\"RSA\",\"kid\":\"") + kid +
           "\",\"n\":\"" + n_b64 + "\",\"e\":\"" + e_b64 + "\"}]}";
}

std::string SignJwt(const std::string& header, const std::string& payload, EVP_PKEY* pkey) {
    // RS256 sign header.payload for a minimal JWT.
    const auto header_b64 = Base64UrlEncode(header);
    const auto payload_b64 = Base64UrlEncode(payload);
    const auto message = header_b64 + "." + payload_b64;

    EVP_MD_CTX* ctx = EVP_MD_CTX_new();
    EVP_DigestSignInit(ctx, nullptr, EVP_sha256(), nullptr, pkey);
    EVP_DigestSignUpdate(ctx, message.data(), message.size());
    size_t sig_len = 0;
    EVP_DigestSignFinal(ctx, nullptr, &sig_len);
    std::vector<unsigned char> sig(sig_len);
    EVP_DigestSignFinal(ctx, sig.data(), &sig_len);
    EVP_MD_CTX_free(ctx);

    const auto sig_b64 = Base64UrlEncode(sig.data(), sig_len);
    return message + "." + sig_b64;
}

}  // namespace

TEST(JwtVerifier, ValidToken) {
    // Full verify path using a temp JWKS file.
    auto key = GenerateKey();
    ASSERT_TRUE(key);

    const std::string kid = "test-key";
    const auto jwks = BuildJwks(kid, key.get());

    const auto jwks_path =
        std::filesystem::temp_directory_path() /
        ("nebulafs_jwks_" + Poco::UUIDGenerator().createOne().toString() + ".json");
    {
        std::ofstream out(jwks_path);
        out << jwks;
    }

    const auto now = std::chrono::system_clock::now();
    const auto now_sec =
        std::chrono::duration_cast<std::chrono::seconds>(now.time_since_epoch()).count();
    const auto exp = now_sec + 300;
    const auto nbf = now_sec - 10;

    const std::string header = "{\"alg\":\"RS256\",\"kid\":\"" + kid + "\",\"typ\":\"JWT\"}";
    const std::string payload = std::string("{\"iss\":\"issuer\",\"aud\":\"aud\",\"sub\":\"user\",") +
                                "\"exp\":" + std::to_string(exp) +
                                ",\"nbf\":" + std::to_string(nbf) +
                                ",\"scope\":\"read write\"}";

    const auto token = SignJwt(header, payload, key.get());

    nebulafs::core::AuthConfig config;
    config.enabled = true;
    config.issuer = "issuer";
    config.audience = "aud";
    config.jwks_url = "file://" + jwks_path.string();
    config.allowed_alg = "RS256";
    config.cache_ttl_seconds = 300;
    config.clock_skew_seconds = 30;

    nebulafs::auth::JwtVerifier verifier(config);
    auto result = verifier.Verify(token);
    ASSERT_TRUE(result.ok());
    EXPECT_EQ(result.value().subject, "user");
    EXPECT_EQ(result.value().issuer, "issuer");
    ASSERT_FALSE(result.value().audience.empty());
    EXPECT_EQ(result.value().audience[0], "aud");

    std::filesystem::remove(jwks_path);
}
