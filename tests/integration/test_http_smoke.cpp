#include <gtest/gtest.h>

#include <boost/asio.hpp>
#include <boost/beast.hpp>
#include <Poco/JSON/Object.h>
#include <Poco/JSON/Parser.h>
#include <Poco/Process.h>
#include <Poco/UUIDGenerator.h>

#include <chrono>
#include <filesystem>
#include <fstream>
#include <exception>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include <openssl/bn.h>
#include <openssl/evp.h>
#include <openssl/rsa.h>

#ifdef _WIN32
#include <windows.h>
#else
#include <unistd.h>
#endif

namespace {
namespace net = boost::asio;
namespace beast = boost::beast;
namespace http = boost::beast::http;
using tcp = net::ip::tcp;
using KeyPtr = std::unique_ptr<EVP_PKEY, decltype(&EVP_PKEY_free)>;

struct AuthConfig {
    bool enabled{false};
    std::string issuer;
    std::string audience;
    std::string jwks_url;
    int cache_ttl_seconds{300};
    int clock_skew_seconds{60};
    std::string allowed_alg{"RS256"};
};

class ServerProcess {
public:
    explicit ServerProcess(Poco::ProcessHandle handle) : handle_(std::move(handle)) {}

    ~ServerProcess() {
        try {
            if (Poco::Process::isRunning(handle_)) {
                Poco::Process::kill(handle_);
                Poco::Process::wait(handle_);
            }
        } catch (const std::exception&) {
            // Best-effort shutdown; test cleanup should not throw.
        }
    }

    ServerProcess(const ServerProcess&) = delete;
    ServerProcess& operator=(const ServerProcess&) = delete;

private:
    Poco::ProcessHandle handle_;
};

unsigned short FindFreePort() {
    net::io_context ioc;
    tcp::acceptor acceptor(ioc, {tcp::v4(), 0});
    return acceptor.local_endpoint().port();
}

std::filesystem::path MakeTempDir() {
    const auto base = std::filesystem::temp_directory_path();
    const auto now = std::chrono::steady_clock::now().time_since_epoch().count();
#ifdef _WIN32
    const auto pid = static_cast<long>(GetCurrentProcessId());
#else
    const auto pid = static_cast<long>(getpid());
#endif
    const std::string name = "nebulafs_it_" + std::to_string(pid) + "_" + std::to_string(now);
    auto dir = base / name;
    std::filesystem::create_directories(dir);
    return dir;
}

void CleanupTempDir(const std::filesystem::path& dir) {
    std::error_code ec;
    for (int i = 0; i < 5; ++i) {
        std::filesystem::remove_all(dir, ec);
        if (!ec) {
            return;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
}

std::filesystem::path WriteServerConfig(const std::filesystem::path& dir,
                                        unsigned short port,
                                        const AuthConfig& auth = {}) {
    const auto storage_dir = dir / "storage";
    const auto temp_dir = storage_dir / "tmp";
    std::filesystem::create_directories(temp_dir);

    const auto config_path = dir / "server.json";
    std::ofstream out(config_path);
    out << "{\n"
        << "  \"server\": {\n"
        << "    \"host\": \"127.0.0.1\",\n"
        << "    \"port\": " << port << ",\n"
        << "    \"threads\": 1,\n"
        << "    \"tls\": {\n"
        << "      \"enabled\": false,\n"
        << "      \"certificate\": \"\",\n"
        << "      \"private_key\": \"\"\n"
        << "    },\n"
        << "    \"limits\": {\n"
        << "      \"max_body_bytes\": 1048576\n"
        << "    }\n"
        << "  },\n"
        << "  \"storage\": {\n"
        << "    \"base_path\": \"" << storage_dir.generic_string() << "\",\n"
        << "    \"temp_path\": \"" << temp_dir.generic_string() << "\"\n"
        << "  },\n"
        << "  \"observability\": {\n"
        << "    \"log_level\": \"warning\"\n"
        << "  },\n"
        << "  \"auth\": {\n"
        << "    \"enabled\": " << (auth.enabled ? "true" : "false") << ",\n"
        << "    \"issuer\": \"" << auth.issuer << "\",\n"
        << "    \"audience\": \"" << auth.audience << "\",\n"
        << "    \"jwks_url\": \"" << auth.jwks_url << "\",\n"
        << "    \"cache_ttl_seconds\": " << auth.cache_ttl_seconds << ",\n"
        << "    \"clock_skew_seconds\": " << auth.clock_skew_seconds << ",\n"
        << "    \"allowed_alg\": \"" << auth.allowed_alg << "\"\n"
        << "  }\n"
        << "}\n";
    return config_path;
}

KeyPtr GenerateKey() {
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
    std::string b64((len + 2) / 3 * 4, '\0');
    const int out_len =
        EVP_EncodeBlock(reinterpret_cast<unsigned char*>(&b64[0]), data, static_cast<int>(len));
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
    const RSA* rsa = EVP_PKEY_get0_RSA(pkey);
    const BIGNUM* n = nullptr;
    const BIGNUM* e = nullptr;
    RSA_get0_key(rsa, &n, &e, nullptr);

    std::vector<unsigned char> n_buf(static_cast<size_t>(BN_num_bytes(n)));
    std::vector<unsigned char> e_buf(static_cast<size_t>(BN_num_bytes(e)));
    BN_bn2bin(n, n_buf.data());
    BN_bn2bin(e, e_buf.data());

    return std::string("{\"keys\":[{\"kty\":\"RSA\",\"kid\":\"") + kid +
           "\",\"n\":\"" + Base64UrlEncode(n_buf.data(), n_buf.size()) +
           "\",\"e\":\"" + Base64UrlEncode(e_buf.data(), e_buf.size()) + "\"}]}";
}

std::filesystem::path WriteJwksFile(const std::filesystem::path& dir, const std::string& body) {
    const auto jwks_path = dir / "jwks.json";
    std::ofstream out(jwks_path);
    out << body;
    return jwks_path;
}

std::string ToFileUrl(const std::filesystem::path& path) {
    // Build a portable file URL for JWKS loading in tests.
    const auto generic = path.generic_string();
    if (generic.empty()) {
        return "file://";
    }
    if (generic.front() == '/') {
        return "file://" + generic;
    }
    return "file:///" + generic;
}

std::string SignJwt(const std::string& header, const std::string& payload, EVP_PKEY* pkey) {
    const auto message = Base64UrlEncode(header) + "." + Base64UrlEncode(payload);

    EVP_MD_CTX* ctx = EVP_MD_CTX_new();
    EVP_DigestSignInit(ctx, nullptr, EVP_sha256(), nullptr, pkey);
    EVP_DigestSignUpdate(ctx, message.data(), message.size());
    size_t sig_len = 0;
    EVP_DigestSignFinal(ctx, nullptr, &sig_len);
    std::vector<unsigned char> sig(sig_len);
    EVP_DigestSignFinal(ctx, sig.data(), &sig_len);
    EVP_MD_CTX_free(ctx);

    return message + "." + Base64UrlEncode(sig.data(), sig_len);
}

std::string MakeValidToken(const std::string& issuer,
                           const std::string& audience,
                           const std::string& kid,
                           EVP_PKEY* key) {
    const auto now = std::chrono::system_clock::now();
    const auto now_sec =
        std::chrono::duration_cast<std::chrono::seconds>(now.time_since_epoch()).count();

    const std::string header = "{\"alg\":\"RS256\",\"kid\":\"" + kid + "\",\"typ\":\"JWT\"}";
    const std::string payload = std::string("{\"iss\":\"") + issuer +
                                "\",\"aud\":\"" + audience +
                                "\",\"sub\":\"it-user\"" +
                                ",\"exp\":" + std::to_string(now_sec + 300) +
                                ",\"nbf\":" + std::to_string(now_sec - 10) + "}";
    return SignJwt(header, payload, key);
}

std::filesystem::path WriteDatabaseConfig(const std::filesystem::path& dir) {
    const auto db_path = dir / "metadata.db";
    const auto config_path = dir / "database.json";
    std::ofstream out(config_path);
    out << "{\n"
        << "  \"sqlite\": {\n"
        << "    \"path\": \"" << db_path.generic_string() << "\"\n"
        << "  }\n"
        << "}\n";
    return config_path;
}

http::response<http::string_body> SendRequest(
    http::verb method,
    const std::string& host,
    unsigned short port,
    const std::string& target,
    const std::string& body,
    const std::string& content_type,
    const std::vector<std::pair<std::string, std::string>>& headers = {}) {
    net::io_context ioc;
    tcp::resolver resolver(ioc);
    beast::tcp_stream stream(ioc);
    auto const results = resolver.resolve(host, std::to_string(port));
    stream.connect(results);

    http::request<http::string_body> req{method, target, 11};
    req.set(http::field::host, host);
    req.set(http::field::user_agent, "nebulafs-integration-tests");
    if (!content_type.empty()) {
        req.set(http::field::content_type, content_type);
    }
    for (const auto& header : headers) {
        req.set(header.first, header.second);
    }
    req.body() = body;
    req.prepare_payload();

    http::write(stream, req);

    beast::flat_buffer buffer;
    http::response<http::string_body> res;
    http::read(stream, buffer, res);

    beast::error_code ec;
    stream.socket().shutdown(tcp::socket::shutdown_both, ec);
    return res;
}

bool WaitForHealth(const std::string& host, unsigned short port) {
    for (int i = 0; i < 30; ++i) {
        try {
            auto res = SendRequest(http::verb::get, host, port, "/healthz", "", "");
            if (res.result() == http::status::ok) {
                return true;
            }
        } catch (const std::exception&) {
            // Server may not be ready yet.
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    return false;
}

}  // namespace

TEST(IntegrationHttp, BasicCrudSmoke) {
    const auto port = FindFreePort();
    const auto temp_dir = MakeTempDir();
    const auto config_path = WriteServerConfig(temp_dir, port);
    const auto db_path = WriteDatabaseConfig(temp_dir);

    std::vector<std::string> args = {"--config", config_path.string(), "--database",
                                     db_path.string()};
    auto handle = Poco::Process::launch(NEBULAFS_SERVER_PATH, args);
    {
        ServerProcess server(std::move(handle));

        ASSERT_TRUE(WaitForHealth("127.0.0.1", port));

        auto create_bucket = SendRequest(http::verb::post, "127.0.0.1", port, "/v1/buckets",
                                         R"({"name":"demo"})", "application/json");
        EXPECT_EQ(create_bucket.result(), http::status::ok);

        const std::string payload = "hello integration tests";
        auto upload = SendRequest(http::verb::put, "127.0.0.1", port,
                                  "/v1/buckets/demo/objects/readme.txt", payload, "");
        EXPECT_EQ(upload.result(), http::status::ok);

        auto list = SendRequest(http::verb::get, "127.0.0.1", port,
                                "/v1/buckets/demo/objects?prefix=read", "", "");
        EXPECT_EQ(list.result(), http::status::ok);
        Poco::JSON::Parser parser;
        auto list_json = parser.parse(list.body()).extract<Poco::JSON::Object::Ptr>();
        auto objects = list_json->getArray("objects");
        ASSERT_TRUE(objects);
        ASSERT_FALSE(objects->empty());

        auto download = SendRequest(http::verb::get, "127.0.0.1", port,
                                    "/v1/buckets/demo/objects/readme.txt", "", "");
        EXPECT_EQ(download.result(), http::status::ok);
        EXPECT_EQ(download.body(), payload);

        auto range = SendRequest(http::verb::get, "127.0.0.1", port,
                                 "/v1/buckets/demo/objects/readme.txt", "", "",
                                 {{"Range", "bytes=0-4"}});
        EXPECT_EQ(range.result(), http::status::partial_content);
        EXPECT_EQ(range.body(), "hello");

        auto del = SendRequest(http::verb::delete_, "127.0.0.1", port,
                               "/v1/buckets/demo/objects/readme.txt", "", "");
        EXPECT_EQ(del.result(), http::status::ok);

        auto missing = SendRequest(http::verb::get, "127.0.0.1", port,
                                   "/v1/buckets/demo/objects/readme.txt", "", "");
        EXPECT_EQ(missing.result(), http::status::not_found);
    }

    CleanupTempDir(temp_dir);
}

TEST(IntegrationHttp, AuthValidation) {
    const auto port = FindFreePort();
    const auto temp_dir = MakeTempDir();
    const auto db_path = WriteDatabaseConfig(temp_dir);

    auto key = GenerateKey();
    ASSERT_TRUE(key);
    const std::string kid = "integration-test-key";
    const auto jwks = BuildJwks(kid, key.get());
    const auto jwks_path = WriteJwksFile(temp_dir, jwks);

    AuthConfig auth;
    auth.enabled = true;
    auth.issuer = "https://issuer.integration.local";
    auth.audience = "nebulafs-it";
    auth.jwks_url = ToFileUrl(jwks_path);
    const auto config_path = WriteServerConfig(temp_dir, port, auth);

    std::vector<std::string> args = {"--config", config_path.string(), "--database",
                                     db_path.string()};
    auto handle = Poco::Process::launch(NEBULAFS_SERVER_PATH, args);
    {
        ServerProcess server(std::move(handle));
        ASSERT_TRUE(WaitForHealth("127.0.0.1", port));

        auto without_token = SendRequest(http::verb::get, "127.0.0.1", port, "/v1/buckets", "", "");
        EXPECT_EQ(without_token.result(), http::status::unauthorized);

        auto bad_token = SendRequest(http::verb::get, "127.0.0.1", port, "/v1/buckets", "", "",
                                     {{"Authorization", "Bearer invalid.token"}});
        EXPECT_EQ(bad_token.result(), http::status::unauthorized);

        const auto token = MakeValidToken(auth.issuer, auth.audience, kid, key.get());
        auto with_token = SendRequest(http::verb::get, "127.0.0.1", port, "/v1/buckets", "", "",
                                      {{"Authorization", "Bearer " + token}});
        EXPECT_EQ(with_token.result(), http::status::ok);

        auto create_bucket = SendRequest(http::verb::post, "127.0.0.1", port, "/v1/buckets",
                                         R"({"name":"authdemo"})", "application/json",
                                         {{"Authorization", "Bearer " + token}});
        EXPECT_EQ(create_bucket.result(), http::status::ok);
    }

    CleanupTempDir(temp_dir);
}
