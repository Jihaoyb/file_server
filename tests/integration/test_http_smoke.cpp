#include <gtest/gtest.h>

#include <boost/asio.hpp>
#include <boost/beast.hpp>
#include <Poco/JSON/Object.h>
#include <Poco/JSON/Parser.h>
#include <Poco/Process.h>
#include <Poco/UUIDGenerator.h>

#include <chrono>
#include <array>
#include <cstdlib>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <exception>
#include <memory>
#include <optional>
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

struct LimitConfig {
    std::uint64_t max_body_bytes{1048576};
    int request_timeout_ms{30000};
    int rate_limit_rps{0};
    int rate_limit_burst{0};
};

class ServerProcess {
public:
    explicit ServerProcess(Poco::ProcessHandle handle) : handle_(std::move(handle)) {}

    ~ServerProcess() { Stop(); }

    void Stop() {
        if (stopped_) {
            return;
        }
        try {
            if (Poco::Process::isRunning(handle_)) {
                Poco::Process::kill(handle_);
                Poco::Process::wait(handle_);
            }
        } catch (const std::exception&) {
            // Best-effort shutdown; test cleanup should not throw.
        }
        stopped_ = true;
    }

    ServerProcess(const ServerProcess&) = delete;
    ServerProcess& operator=(const ServerProcess&) = delete;

private:
    Poco::ProcessHandle handle_;
    bool stopped_{false};
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

std::uint64_t CountRegularFiles(const std::filesystem::path& dir) {
    if (!std::filesystem::exists(dir)) {
        return 0;
    }
    std::uint64_t count = 0;
    for (const auto& entry : std::filesystem::directory_iterator(dir)) {
        if (entry.is_regular_file()) {
            ++count;
        }
    }
    return count;
}

std::filesystem::path WriteServerConfig(const std::filesystem::path& dir,
                                        unsigned short port,
                                        const AuthConfig& auth = {},
                                        const LimitConfig& limits = {}) {
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
        << "      \"max_body_bytes\": " << limits.max_body_bytes << ",\n"
        << "      \"request_timeout_ms\": " << limits.request_timeout_ms << ",\n"
        << "      \"rate_limit_rps\": " << limits.rate_limit_rps << ",\n"
        << "      \"rate_limit_burst\": " << limits.rate_limit_burst << "\n"
        << "    }\n"
        << "  },\n"
        << "  \"storage\": {\n"
        << "    \"base_path\": \"" << storage_dir.generic_string() << "\",\n"
        << "    \"temp_path\": \"" << temp_dir.generic_string() << "\",\n"
        << "    \"multipart\": {\n"
        << "      \"max_upload_ttl_seconds\": 3600\n"
        << "    }\n"
        << "  },\n"
        << "  \"cleanup\": {\n"
        << "    \"enabled\": true,\n"
        << "    \"sweep_interval_seconds\": 300,\n"
        << "    \"grace_period_seconds\": 60,\n"
        << "    \"max_uploads_per_sweep\": 200\n"
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

std::string ToJsonArray(const std::vector<std::string>& values) {
    std::ostringstream ss;
    ss << "[";
    for (size_t i = 0; i < values.size(); ++i) {
        if (i > 0) {
            ss << ",";
        }
        ss << "\"" << values[i] << "\"";
    }
    ss << "]";
    return ss.str();
}

struct DistributedGatewayConfigOptions {
    int replication_factor{2};
    int min_write_acks{2};
    bool cleanup_enabled{false};
    int cleanup_sweep_interval_seconds{300};
    int cleanup_grace_period_seconds{60};
    int cleanup_max_uploads_per_sweep{200};
    int multipart_max_upload_ttl_seconds{3600};
};

std::filesystem::path WriteGatewayDistributedConfig(const std::filesystem::path& dir,
                                                    unsigned short port,
                                                    const std::string& metadata_base_url,
                                                    const std::vector<std::string>& storage_nodes,
                                                    const std::string& token,
                                                    const DistributedGatewayConfigOptions& options =
                                                        {}) {
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
        << "    \"mode\": \"distributed\",\n"
        << "    \"tls\": {\"enabled\": false, \"certificate\": \"\", \"private_key\": \"\"},\n"
        << "    \"limits\": {\n"
        << "      \"max_body_bytes\": 10485760,\n"
        << "      \"request_timeout_ms\": 30000,\n"
        << "      \"rate_limit_rps\": 0,\n"
        << "      \"rate_limit_burst\": 0\n"
        << "    }\n"
        << "  },\n"
        << "  \"storage\": {\n"
        << "    \"base_path\": \"" << storage_dir.generic_string() << "\",\n"
        << "    \"temp_path\": \"" << temp_dir.generic_string() << "\",\n"
        << "    \"multipart\": {\n"
        << "      \"max_upload_ttl_seconds\": " << options.multipart_max_upload_ttl_seconds << "\n"
        << "    }\n"
        << "  },\n"
        << "  \"cleanup\": {\"enabled\": " << (options.cleanup_enabled ? "true" : "false")
        << ", \"sweep_interval_seconds\": " << options.cleanup_sweep_interval_seconds
        << ", \"grace_period_seconds\": " << options.cleanup_grace_period_seconds
        << ", \"max_uploads_per_sweep\": " << options.cleanup_max_uploads_per_sweep << "},\n"
        << "  \"observability\": {\"log_level\": \"warning\"},\n"
        << "  \"auth\": {\"enabled\": false, \"issuer\": \"\", \"audience\": \"\", "
           "\"jwks_url\": \"\", \"cache_ttl_seconds\": 300, \"clock_skew_seconds\": 60, "
           "\"allowed_alg\": \"RS256\"},\n"
        << "  \"distributed\": {\n"
        << "    \"metadata_base_url\": \"" << metadata_base_url << "\",\n"
        << "    \"storage_nodes\": " << ToJsonArray(storage_nodes) << ",\n"
        << "    \"service_auth_token\": \"" << token << "\",\n"
        << "    \"replication_factor\": " << options.replication_factor << ",\n"
        << "    \"min_write_acks\": " << options.min_write_acks << "\n"
        << "  }\n"
        << "}\n";
    return config_path;
}

std::filesystem::path WriteMetadataServiceConfig(const std::filesystem::path& dir,
                                                 unsigned short port,
                                                 const std::string& token,
                                                 const std::vector<std::string>& storage_nodes) {
    std::filesystem::create_directories(dir);
    const auto config_path = dir / "metadata_server.json";
    std::ofstream out(config_path);
    out << "{\n"
        << "  \"server\": {\n"
        << "    \"host\": \"127.0.0.1\",\n"
        << "    \"port\": " << port << ",\n"
        << "    \"threads\": 1,\n"
        << "    \"mode\": \"single_node\",\n"
        << "    \"tls\": {\"enabled\": false, \"certificate\": \"\", \"private_key\": \"\"},\n"
        << "    \"limits\": {\"max_body_bytes\": 1048576, \"request_timeout_ms\": 30000, "
           "\"rate_limit_rps\": 0, \"rate_limit_burst\": 0}\n"
        << "  },\n"
        << "  \"storage\": {\"base_path\": \"data\", \"temp_path\": \"data/tmp\"},\n"
        << "  \"cleanup\": {\"enabled\": false, \"sweep_interval_seconds\": 300, "
           "\"grace_period_seconds\": 60, \"max_uploads_per_sweep\": 200},\n"
        << "  \"observability\": {\"log_level\": \"warning\"},\n"
        << "  \"auth\": {\"enabled\": false, \"issuer\": \"\", \"audience\": \"\", "
           "\"jwks_url\": \"\", \"cache_ttl_seconds\": 300, \"clock_skew_seconds\": 60, "
           "\"allowed_alg\": \"RS256\"},\n"
        << "  \"distributed\": {\n"
        << "    \"metadata_base_url\": \"\",\n"
        << "    \"storage_nodes\": " << ToJsonArray(storage_nodes) << ",\n"
        << "    \"service_auth_token\": \"" << token << "\",\n"
        << "    \"replication_factor\": 2,\n"
        << "    \"min_write_acks\": 2\n"
        << "  }\n"
        << "}\n";
    return config_path;
}

std::filesystem::path WriteStorageNodeConfig(const std::filesystem::path& dir,
                                             unsigned short port,
                                             const std::string& token) {
    const auto storage_dir = dir / "storage";
    const auto temp_dir = storage_dir / "tmp";
    std::filesystem::create_directories(temp_dir);

    const auto config_path = dir / "storage_node_server.json";
    std::ofstream out(config_path);
    out << "{\n"
        << "  \"server\": {\n"
        << "    \"host\": \"127.0.0.1\",\n"
        << "    \"port\": " << port << ",\n"
        << "    \"threads\": 1,\n"
        << "    \"mode\": \"single_node\",\n"
        << "    \"tls\": {\"enabled\": false, \"certificate\": \"\", \"private_key\": \"\"},\n"
        << "    \"limits\": {\"max_body_bytes\": 10485760, \"request_timeout_ms\": 30000, "
           "\"rate_limit_rps\": 0, \"rate_limit_burst\": 0}\n"
        << "  },\n"
        << "  \"storage\": {\n"
        << "    \"base_path\": \"" << storage_dir.generic_string() << "\",\n"
        << "    \"temp_path\": \"" << temp_dir.generic_string() << "\"\n"
        << "  },\n"
        << "  \"cleanup\": {\"enabled\": false, \"sweep_interval_seconds\": 300, "
           "\"grace_period_seconds\": 60, \"max_uploads_per_sweep\": 200},\n"
        << "  \"observability\": {\"log_level\": \"warning\"},\n"
        << "  \"auth\": {\"enabled\": false, \"issuer\": \"\", \"audience\": \"\", "
           "\"jwks_url\": \"\", \"cache_ttl_seconds\": 300, \"clock_skew_seconds\": 60, "
           "\"allowed_alg\": \"RS256\"},\n"
        << "  \"distributed\": {\n"
        << "    \"metadata_base_url\": \"\",\n"
        << "    \"storage_nodes\": [],\n"
        << "    \"service_auth_token\": \"" << token << "\",\n"
        << "    \"replication_factor\": 2,\n"
        << "    \"min_write_acks\": 2\n"
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

std::optional<long long> ParseMetricCounter(const std::string& metrics, const std::string& name) {
    const std::string prefix = name + " ";
    std::stringstream ss(metrics);
    std::string line;
    while (std::getline(ss, line)) {
        if (line.rfind(prefix, 0) == 0) {
            try {
                return std::stoll(line.substr(prefix.size()));
            } catch (const std::exception&) {
                return std::nullopt;
            }
        }
    }
    return std::nullopt;
}

struct ErrorEnvelope {
    std::string code;
    std::string message;
    std::string request_id;
};

std::optional<ErrorEnvelope> ParseErrorEnvelope(const std::string& body) {
    try {
        Poco::JSON::Parser parser;
        auto root = parser.parse(body).extract<Poco::JSON::Object::Ptr>();
        auto error = root->getObject("error");
        if (!error) {
            return std::nullopt;
        }
        ErrorEnvelope envelope;
        envelope.code = error->getValue<std::string>("code");
        envelope.message = error->getValue<std::string>("message");
        envelope.request_id = error->getValue<std::string>("request_id");
        return envelope;
    } catch (const std::exception&) {
        return std::nullopt;
    }
}

void ExpectErrorEnvelope(const http::response<http::string_body>& response,
                         const std::string& expected_code) {
    const auto envelope = ParseErrorEnvelope(response.body());
    ASSERT_TRUE(envelope.has_value());
    EXPECT_EQ(envelope->code, expected_code);
    EXPECT_FALSE(envelope->request_id.empty());
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

TEST(IntegrationHttp, MultipartUploadEndpoints) {
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
        ASSERT_EQ(create_bucket.result(), http::status::ok);

        auto initiate = SendRequest(http::verb::post, "127.0.0.1", port,
                                    "/v1/buckets/demo/multipart-uploads",
                                    R"({"object":"movie.bin"})", "application/json");
        ASSERT_EQ(initiate.result(), http::status::ok);
        Poco::JSON::Parser parser;
        auto initiate_json = parser.parse(initiate.body()).extract<Poco::JSON::Object::Ptr>();
        const auto upload_id = initiate_json->getValue<std::string>("upload_id");
        ASSERT_FALSE(upload_id.empty());

        auto part1 = SendRequest(
            http::verb::put, "127.0.0.1", port,
            "/v1/buckets/demo/multipart-uploads/" + upload_id + "/parts/1", "hello-", "");
        ASSERT_EQ(part1.result(), http::status::ok);
        auto part1_json = parser.parse(part1.body()).extract<Poco::JSON::Object::Ptr>();
        const auto part1_etag = part1_json->getValue<std::string>("etag");

        auto part2 = SendRequest(
            http::verb::put, "127.0.0.1", port,
            "/v1/buckets/demo/multipart-uploads/" + upload_id + "/parts/2", "world", "");
        ASSERT_EQ(part2.result(), http::status::ok);
        auto part2_json = parser.parse(part2.body()).extract<Poco::JSON::Object::Ptr>();
        const auto part2_etag = part2_json->getValue<std::string>("etag");

        auto parts = SendRequest(http::verb::get, "127.0.0.1", port,
                                 "/v1/buckets/demo/multipart-uploads/" + upload_id + "/parts", "",
                                 "");
        ASSERT_EQ(parts.result(), http::status::ok);
        auto parts_json = parser.parse(parts.body()).extract<Poco::JSON::Object::Ptr>();
        EXPECT_EQ(parts_json->getValue<std::string>("upload_id"), upload_id);
        EXPECT_EQ(parts_json->getValue<std::string>("object"), "movie.bin");
        EXPECT_EQ(parts_json->getValue<std::string>("state"), "uploading");
        auto parts_arr = parts_json->getArray("parts");
        ASSERT_TRUE(parts_arr);
        ASSERT_EQ(parts_arr->size(), 2);

        const std::string complete_body = std::string("{\"parts\":[") +
                                          "{\"part_number\":1,\"etag\":\"" + part1_etag + "\"}," +
                                          "{\"part_number\":2,\"etag\":\"" + part2_etag + "\"}" +
                                          "]}";
        auto complete = SendRequest(
            http::verb::post, "127.0.0.1", port,
            "/v1/buckets/demo/multipart-uploads/" + upload_id + "/complete", complete_body,
            "application/json");
        ASSERT_EQ(complete.result(), http::status::ok);

        auto download = SendRequest(http::verb::get, "127.0.0.1", port,
                                    "/v1/buckets/demo/objects/movie.bin", "", "");
        EXPECT_EQ(download.result(), http::status::ok);
        EXPECT_EQ(download.body(), "hello-world");

        auto parts_after_complete =
            SendRequest(http::verb::get, "127.0.0.1", port,
                        "/v1/buckets/demo/multipart-uploads/" + upload_id + "/parts", "", "");
        EXPECT_EQ(parts_after_complete.result(), http::status::not_found);
    }

    CleanupTempDir(temp_dir);
}

TEST(IntegrationHttp, MultipartAbortEndpoint) {
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
        ASSERT_EQ(create_bucket.result(), http::status::ok);

        auto initiate = SendRequest(http::verb::post, "127.0.0.1", port,
                                    "/v1/buckets/demo/multipart-uploads",
                                    R"({"object":"cancel.bin"})", "application/json");
        ASSERT_EQ(initiate.result(), http::status::ok);
        Poco::JSON::Parser parser;
        auto initiate_json = parser.parse(initiate.body()).extract<Poco::JSON::Object::Ptr>();
        const auto upload_id = initiate_json->getValue<std::string>("upload_id");
        ASSERT_FALSE(upload_id.empty());

        auto part1 = SendRequest(
            http::verb::put, "127.0.0.1", port,
            "/v1/buckets/demo/multipart-uploads/" + upload_id + "/parts/1", "temp-data", "");
        ASSERT_EQ(part1.result(), http::status::ok);

        auto abort =
            SendRequest(http::verb::delete_, "127.0.0.1", port,
                        "/v1/buckets/demo/multipart-uploads/" + upload_id, "", "");
        EXPECT_EQ(abort.result(), http::status::no_content);

        auto parts_after_abort =
            SendRequest(http::verb::get, "127.0.0.1", port,
                        "/v1/buckets/demo/multipart-uploads/" + upload_id + "/parts", "", "");
        EXPECT_EQ(parts_after_abort.result(), http::status::not_found);
    }

    CleanupTempDir(temp_dir);
}

TEST(IntegrationHttp, RateLimiting) {
    const auto port = FindFreePort();
    const auto temp_dir = MakeTempDir();
    LimitConfig limits;
    limits.rate_limit_rps = 1;
    limits.rate_limit_burst = 1;
    const auto config_path = WriteServerConfig(temp_dir, port, {}, limits);
    const auto db_path = WriteDatabaseConfig(temp_dir);

    std::vector<std::string> args = {"--config", config_path.string(), "--database",
                                     db_path.string()};
    auto handle = Poco::Process::launch(NEBULAFS_SERVER_PATH, args);
    {
        ServerProcess server(std::move(handle));
        ASSERT_TRUE(WaitForHealth("127.0.0.1", port));
        std::this_thread::sleep_for(std::chrono::milliseconds(1200));
        auto metrics_before = SendRequest(http::verb::get, "127.0.0.1", port, "/metrics", "", "");
        ASSERT_EQ(metrics_before.result(), http::status::ok);
        const auto before_rate_limited =
            ParseMetricCounter(metrics_before.body(), "nebulafs_http_requests_rate_limited_total");
        const auto before_timed_out =
            ParseMetricCounter(metrics_before.body(), "nebulafs_http_requests_timed_out_total");
        ASSERT_TRUE(before_rate_limited.has_value());
        ASSERT_TRUE(before_timed_out.has_value());

        std::this_thread::sleep_for(std::chrono::milliseconds(1200));
        std::optional<http::response<http::string_body>> rate_limited_response;
        for (int i = 0; i < 8 && !rate_limited_response.has_value(); ++i) {
            auto response = SendRequest(http::verb::get, "127.0.0.1", port, "/healthz", "", "");
            if (response.result() == http::status::too_many_requests) {
                rate_limited_response = std::move(response);
            }
        }
        ASSERT_TRUE(rate_limited_response.has_value());
        const auto retry_after = rate_limited_response->find(http::field::retry_after);
        ASSERT_NE(retry_after, rate_limited_response->end());
        EXPECT_EQ(retry_after->value(), "1");

        std::this_thread::sleep_for(std::chrono::milliseconds(1200));
        auto metrics_after = SendRequest(http::verb::get, "127.0.0.1", port, "/metrics", "", "");
        ASSERT_EQ(metrics_after.result(), http::status::ok);
        const auto after_rate_limited =
            ParseMetricCounter(metrics_after.body(), "nebulafs_http_requests_rate_limited_total");
        const auto after_timed_out =
            ParseMetricCounter(metrics_after.body(), "nebulafs_http_requests_timed_out_total");
        ASSERT_TRUE(after_rate_limited.has_value());
        ASSERT_TRUE(after_timed_out.has_value());
        EXPECT_GT(*after_rate_limited, *before_rate_limited);
    }

    CleanupTempDir(temp_dir);
}

TEST(IntegrationHttp, RequestTimeout) {
    const auto port = FindFreePort();
    const auto temp_dir = MakeTempDir();
    LimitConfig limits;
    limits.max_body_bytes = 64 * 1024 * 1024;
    limits.request_timeout_ms = 20;
    const auto config_path = WriteServerConfig(temp_dir, port, {}, limits);
    const auto db_path = WriteDatabaseConfig(temp_dir);

    std::vector<std::string> args = {"--config", config_path.string(), "--database",
                                     db_path.string()};
    auto handle = Poco::Process::launch(NEBULAFS_SERVER_PATH, args);
    {
        ServerProcess server(std::move(handle));
        ASSERT_TRUE(WaitForHealth("127.0.0.1", port));

        auto create_bucket = SendRequest(http::verb::post, "127.0.0.1", port, "/v1/buckets",
                                         R"({"name":"demo"})", "application/json");
        ASSERT_EQ(create_bucket.result(), http::status::ok);

        auto metrics_before = SendRequest(http::verb::get, "127.0.0.1", port, "/metrics", "", "");
        ASSERT_EQ(metrics_before.result(), http::status::ok);
        const auto before_timeout =
            ParseMetricCounter(metrics_before.body(), "nebulafs_http_requests_timed_out_total");
        const auto before_rate_limited =
            ParseMetricCounter(metrics_before.body(), "nebulafs_http_requests_rate_limited_total");
        ASSERT_TRUE(before_timeout.has_value());
        ASSERT_TRUE(before_rate_limited.has_value());

        const std::string payload(8 * 1024 * 1024, 'x');
        bool timeout_observed = false;
        try {
            auto upload = SendRequest(http::verb::put, "127.0.0.1", port,
                                      "/v1/buckets/demo/objects/slow.bin", payload, "");
            timeout_observed = upload.result() == http::status::request_timeout;
        } catch (const std::exception&) {
            // Connection reset during oversized timed-out upload is acceptable;
            // timeout is validated via metrics increment below.
            timeout_observed = true;
        }
        EXPECT_TRUE(timeout_observed);

        auto metrics_after = SendRequest(http::verb::get, "127.0.0.1", port, "/metrics", "", "");
        ASSERT_EQ(metrics_after.result(), http::status::ok);
        const auto after_timeout =
            ParseMetricCounter(metrics_after.body(), "nebulafs_http_requests_timed_out_total");
        const auto after_rate_limited =
            ParseMetricCounter(metrics_after.body(), "nebulafs_http_requests_rate_limited_total");
        ASSERT_TRUE(after_timeout.has_value());
        ASSERT_TRUE(after_rate_limited.has_value());
        EXPECT_GT(*after_timeout, *before_timeout);
    }

    CleanupTempDir(temp_dir);
}

TEST(IntegrationHttp, DistributedCrudSmoke) {
    const char* enabled = std::getenv("NEBULAFS_ENABLE_DISTRIBUTED_IT");
    if (!enabled || std::string(enabled) != "1") {
        GTEST_SKIP() << "distributed integration lane is disabled";
    }

    const auto gateway_port = FindFreePort();
    const auto metadata_port = FindFreePort();
    const auto storage1_port = FindFreePort();
    const auto storage2_port = FindFreePort();
    const auto temp_dir = MakeTempDir();
    const std::string token = "distributed-test-token";

    const std::vector<std::string> storage_nodes = {
        "http://127.0.0.1:" + std::to_string(storage1_port),
        "http://127.0.0.1:" + std::to_string(storage2_port),
    };
    const auto metadata_url = "http://127.0.0.1:" + std::to_string(metadata_port);

    const auto metadata_config =
        WriteMetadataServiceConfig(temp_dir / "metadata", metadata_port, token, storage_nodes);
    const auto metadata_db = WriteDatabaseConfig(temp_dir / "metadata");
    const auto storage1_config = WriteStorageNodeConfig(temp_dir / "storage1", storage1_port, token);
    const auto storage2_config = WriteStorageNodeConfig(temp_dir / "storage2", storage2_port, token);
    const auto gateway_config = WriteGatewayDistributedConfig(temp_dir / "gateway", gateway_port,
                                                              metadata_url, storage_nodes, token);

    std::vector<std::string> metadata_args = {"--config", metadata_config.string(), "--database",
                                              metadata_db.string()};
    std::vector<std::string> storage1_args = {"--config", storage1_config.string()};
    std::vector<std::string> storage2_args = {"--config", storage2_config.string()};
    std::vector<std::string> gateway_args = {"--config", gateway_config.string(), "--database",
                                             metadata_db.string()};

    auto metadata_handle = Poco::Process::launch(NEBULAFS_METADATA_PATH, metadata_args);
    auto storage1_handle = Poco::Process::launch(NEBULAFS_STORAGE_NODE_PATH, storage1_args);
    auto storage2_handle = Poco::Process::launch(NEBULAFS_STORAGE_NODE_PATH, storage2_args);
    {
        ServerProcess metadata(std::move(metadata_handle));
        ServerProcess storage1(std::move(storage1_handle));
        ServerProcess storage2(std::move(storage2_handle));

        ASSERT_TRUE(WaitForHealth("127.0.0.1", metadata_port));
        ASSERT_TRUE(WaitForHealth("127.0.0.1", storage1_port));
        ASSERT_TRUE(WaitForHealth("127.0.0.1", storage2_port));

        auto gateway_handle = Poco::Process::launch(NEBULAFS_SERVER_PATH, gateway_args);
        ServerProcess gateway(std::move(gateway_handle));
        ASSERT_TRUE(WaitForHealth("127.0.0.1", gateway_port));

        auto create_bucket = SendRequest(http::verb::post, "127.0.0.1", gateway_port, "/v1/buckets",
                                         R"({"name":"demo"})", "application/json");
        ASSERT_EQ(create_bucket.result(), http::status::ok);

        auto upload = SendRequest(http::verb::put, "127.0.0.1", gateway_port,
                                  "/v1/buckets/demo/objects/readme.txt", "distributed-data", "");
        ASSERT_EQ(upload.result(), http::status::ok);

        auto list = SendRequest(http::verb::get, "127.0.0.1", gateway_port,
                                "/v1/buckets/demo/objects?prefix=read", "", "");
        ASSERT_EQ(list.result(), http::status::ok);
        EXPECT_NE(list.body().find("\"readme.txt\""), std::string::npos);

        auto download = SendRequest(http::verb::get, "127.0.0.1", gateway_port,
                                    "/v1/buckets/demo/objects/readme.txt", "", "");
        ASSERT_EQ(download.result(), http::status::ok);
        EXPECT_EQ(download.body(), "distributed-data");

        auto del = SendRequest(http::verb::delete_, "127.0.0.1", gateway_port,
                               "/v1/buckets/demo/objects/readme.txt", "", "");
        ASSERT_EQ(del.result(), http::status::ok);

        auto metadata_metrics =
            SendRequest(http::verb::get, "127.0.0.1", metadata_port, "/metrics", "", "");
        ASSERT_EQ(metadata_metrics.result(), http::status::ok);
        auto metadata_allocate =
            ParseMetricCounter(metadata_metrics.body(), "nebulafs_metadata_allocate_requests_total");
        auto metadata_commit =
            ParseMetricCounter(metadata_metrics.body(), "nebulafs_metadata_commit_requests_total");
        auto metadata_allocate_failures =
            ParseMetricCounter(metadata_metrics.body(), "nebulafs_metadata_allocate_failures_total");
        auto metadata_commit_failures =
            ParseMetricCounter(metadata_metrics.body(), "nebulafs_metadata_commit_failures_total");
        auto metadata_allocate_latency = ParseMetricCounter(
            metadata_metrics.body(), "nebulafs_metadata_allocate_latency_ms_sum");
        auto metadata_commit_latency = ParseMetricCounter(
            metadata_metrics.body(), "nebulafs_metadata_commit_latency_ms_sum");
        ASSERT_TRUE(metadata_allocate.has_value());
        ASSERT_TRUE(metadata_commit.has_value());
        ASSERT_TRUE(metadata_allocate_failures.has_value());
        ASSERT_TRUE(metadata_commit_failures.has_value());
        ASSERT_TRUE(metadata_allocate_latency.has_value());
        ASSERT_TRUE(metadata_commit_latency.has_value());
        EXPECT_GE(*metadata_allocate, 1);
        EXPECT_GE(*metadata_commit, 1);

        auto storage1_metrics =
            SendRequest(http::verb::get, "127.0.0.1", storage1_port, "/metrics", "", "");
        auto storage2_metrics =
            SendRequest(http::verb::get, "127.0.0.1", storage2_port, "/metrics", "", "");
        ASSERT_EQ(storage1_metrics.result(), http::status::ok);
        ASSERT_EQ(storage2_metrics.result(), http::status::ok);

        auto storage1_writes = ParseMetricCounter(storage1_metrics.body(),
                                                  "nebulafs_storage_node_blob_writes_total");
        auto storage2_writes = ParseMetricCounter(storage2_metrics.body(),
                                                  "nebulafs_storage_node_blob_writes_total");
        auto storage1_deletes = ParseMetricCounter(storage1_metrics.body(),
                                                   "nebulafs_storage_node_blob_deletes_total");
        auto storage2_deletes = ParseMetricCounter(storage2_metrics.body(),
                                                   "nebulafs_storage_node_blob_deletes_total");
        auto storage1_reads = ParseMetricCounter(storage1_metrics.body(),
                                                 "nebulafs_storage_node_blob_reads_total");
        auto storage1_write_failures = ParseMetricCounter(
            storage1_metrics.body(), "nebulafs_storage_node_blob_write_failures_total");
        auto storage1_read_failures = ParseMetricCounter(
            storage1_metrics.body(), "nebulafs_storage_node_blob_read_failures_total");
        auto storage1_delete_failures = ParseMetricCounter(
            storage1_metrics.body(), "nebulafs_storage_node_blob_delete_failures_total");
        auto storage1_write_latency = ParseMetricCounter(
            storage1_metrics.body(), "nebulafs_storage_node_blob_write_latency_ms_sum");
        auto storage1_read_latency = ParseMetricCounter(
            storage1_metrics.body(), "nebulafs_storage_node_blob_read_latency_ms_sum");
        auto storage1_delete_latency = ParseMetricCounter(
            storage1_metrics.body(), "nebulafs_storage_node_blob_delete_latency_ms_sum");
        ASSERT_TRUE(storage1_writes.has_value());
        ASSERT_TRUE(storage2_writes.has_value());
        ASSERT_TRUE(storage1_deletes.has_value());
        ASSERT_TRUE(storage2_deletes.has_value());
        ASSERT_TRUE(storage1_reads.has_value());
        ASSERT_TRUE(storage1_write_failures.has_value());
        ASSERT_TRUE(storage1_read_failures.has_value());
        ASSERT_TRUE(storage1_delete_failures.has_value());
        ASSERT_TRUE(storage1_write_latency.has_value());
        ASSERT_TRUE(storage1_read_latency.has_value());
        ASSERT_TRUE(storage1_delete_latency.has_value());
        EXPECT_GE(*storage1_writes, 1);
        EXPECT_GE(*storage2_writes, 1);
        EXPECT_GE(*storage1_deletes, 1);
        EXPECT_GE(*storage2_deletes, 1);
        EXPECT_GE(*storage1_reads, 1);
    }

    CleanupTempDir(temp_dir);
}

TEST(IntegrationHttp, DistributedLargeObjectStreamingWrite) {
    const char* enabled = std::getenv("NEBULAFS_ENABLE_DISTRIBUTED_IT");
    if (!enabled || std::string(enabled) != "1") {
        GTEST_SKIP() << "distributed integration lane is disabled";
    }

    const auto gateway_port = FindFreePort();
    const auto metadata_port = FindFreePort();
    const auto storage1_port = FindFreePort();
    const auto storage2_port = FindFreePort();
    const auto temp_dir = MakeTempDir();
    const std::string token = "distributed-test-token";

    const std::vector<std::string> storage_nodes = {
        "http://127.0.0.1:" + std::to_string(storage1_port),
        "http://127.0.0.1:" + std::to_string(storage2_port),
    };
    const auto metadata_url = "http://127.0.0.1:" + std::to_string(metadata_port);

    const auto metadata_config =
        WriteMetadataServiceConfig(temp_dir / "metadata", metadata_port, token, storage_nodes);
    const auto metadata_db = WriteDatabaseConfig(temp_dir / "metadata");
    const auto storage1_config = WriteStorageNodeConfig(temp_dir / "storage1", storage1_port, token);
    const auto storage2_config = WriteStorageNodeConfig(temp_dir / "storage2", storage2_port, token);
    const auto gateway_config = WriteGatewayDistributedConfig(temp_dir / "gateway", gateway_port,
                                                              metadata_url, storage_nodes, token);

    std::vector<std::string> metadata_args = {"--config", metadata_config.string(), "--database",
                                              metadata_db.string()};
    std::vector<std::string> storage1_args = {"--config", storage1_config.string()};
    std::vector<std::string> storage2_args = {"--config", storage2_config.string()};
    std::vector<std::string> gateway_args = {"--config", gateway_config.string(), "--database",
                                             metadata_db.string()};

    auto metadata_handle = Poco::Process::launch(NEBULAFS_METADATA_PATH, metadata_args);
    auto storage1_handle = Poco::Process::launch(NEBULAFS_STORAGE_NODE_PATH, storage1_args);
    auto storage2_handle = Poco::Process::launch(NEBULAFS_STORAGE_NODE_PATH, storage2_args);
    {
        ServerProcess metadata(std::move(metadata_handle));
        ServerProcess storage1(std::move(storage1_handle));
        ServerProcess storage2(std::move(storage2_handle));

        ASSERT_TRUE(WaitForHealth("127.0.0.1", metadata_port));
        ASSERT_TRUE(WaitForHealth("127.0.0.1", storage1_port));
        ASSERT_TRUE(WaitForHealth("127.0.0.1", storage2_port));

        auto gateway_handle = Poco::Process::launch(NEBULAFS_SERVER_PATH, gateway_args);
        ServerProcess gateway(std::move(gateway_handle));
        ASSERT_TRUE(WaitForHealth("127.0.0.1", gateway_port));

        auto create_bucket = SendRequest(http::verb::post, "127.0.0.1", gateway_port, "/v1/buckets",
                                         R"({"name":"demo"})", "application/json");
        ASSERT_EQ(create_bucket.result(), http::status::ok);

        const std::string payload(6 * 1024 * 1024, 'x');
        auto upload = SendRequest(http::verb::put, "127.0.0.1", gateway_port,
                                  "/v1/buckets/demo/objects/large.bin", payload, "");
        ASSERT_EQ(upload.result(), http::status::ok);

        auto download = SendRequest(http::verb::get, "127.0.0.1", gateway_port,
                                    "/v1/buckets/demo/objects/large.bin", "", "");
        ASSERT_EQ(download.result(), http::status::ok);
        ASSERT_EQ(download.body().size(), payload.size());
        EXPECT_EQ(download.body().substr(0, 1024), payload.substr(0, 1024));

        const auto spool_dir = temp_dir / "gateway" / "storage" / "tmp" / "remote_spool";
        bool spool_cleaned = false;
        for (int i = 0; i < 10; ++i) {
            if (CountRegularFiles(spool_dir) == 0) {
                spool_cleaned = true;
                break;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(50));
        }
        EXPECT_TRUE(spool_cleaned);
    }

    CleanupTempDir(temp_dir);
}

TEST(IntegrationHttp, DistributedMultipartUploadEndpoints) {
    const char* enabled = std::getenv("NEBULAFS_ENABLE_DISTRIBUTED_IT");
    if (!enabled || std::string(enabled) != "1") {
        GTEST_SKIP() << "distributed integration lane is disabled";
    }

    const auto gateway_port = FindFreePort();
    const auto metadata_port = FindFreePort();
    const auto storage1_port = FindFreePort();
    const auto storage2_port = FindFreePort();
    const auto temp_dir = MakeTempDir();
    const std::string token = "distributed-test-token";
    const std::vector<std::string> storage_nodes = {
        "http://127.0.0.1:" + std::to_string(storage1_port),
        "http://127.0.0.1:" + std::to_string(storage2_port),
    };
    const auto metadata_url = "http://127.0.0.1:" + std::to_string(metadata_port);

    const auto metadata_config =
        WriteMetadataServiceConfig(temp_dir / "metadata", metadata_port, token, storage_nodes);
    const auto metadata_db = WriteDatabaseConfig(temp_dir / "metadata");
    const auto storage1_config = WriteStorageNodeConfig(temp_dir / "storage1", storage1_port, token);
    const auto storage2_config = WriteStorageNodeConfig(temp_dir / "storage2", storage2_port, token);
    const auto gateway_config = WriteGatewayDistributedConfig(temp_dir / "gateway", gateway_port,
                                                              metadata_url, storage_nodes, token);

    std::vector<std::string> metadata_args = {"--config", metadata_config.string(), "--database",
                                              metadata_db.string()};
    std::vector<std::string> storage1_args = {"--config", storage1_config.string()};
    std::vector<std::string> storage2_args = {"--config", storage2_config.string()};
    std::vector<std::string> gateway_args = {"--config", gateway_config.string(), "--database",
                                             metadata_db.string()};

    auto metadata_handle = Poco::Process::launch(NEBULAFS_METADATA_PATH, metadata_args);
    auto storage1_handle = Poco::Process::launch(NEBULAFS_STORAGE_NODE_PATH, storage1_args);
    auto storage2_handle = Poco::Process::launch(NEBULAFS_STORAGE_NODE_PATH, storage2_args);
    {
        ServerProcess metadata(std::move(metadata_handle));
        ServerProcess storage1(std::move(storage1_handle));
        ServerProcess storage2(std::move(storage2_handle));

        ASSERT_TRUE(WaitForHealth("127.0.0.1", metadata_port));
        ASSERT_TRUE(WaitForHealth("127.0.0.1", storage1_port));
        ASSERT_TRUE(WaitForHealth("127.0.0.1", storage2_port));

        auto gateway_handle = Poco::Process::launch(NEBULAFS_SERVER_PATH, gateway_args);
        ServerProcess gateway(std::move(gateway_handle));
        ASSERT_TRUE(WaitForHealth("127.0.0.1", gateway_port));

        auto create_bucket = SendRequest(http::verb::post, "127.0.0.1", gateway_port, "/v1/buckets",
                                         R"({"name":"demo"})", "application/json");
        ASSERT_EQ(create_bucket.result(), http::status::ok);

        auto initiate = SendRequest(http::verb::post, "127.0.0.1", gateway_port,
                                    "/v1/buckets/demo/multipart-uploads",
                                    R"({"object":"movie.bin"})", "application/json");
        ASSERT_EQ(initiate.result(), http::status::ok);
        Poco::JSON::Parser parser;
        auto initiate_json = parser.parse(initiate.body()).extract<Poco::JSON::Object::Ptr>();
        const auto upload_id = initiate_json->getValue<std::string>("upload_id");
        ASSERT_FALSE(upload_id.empty());

        auto part1 = SendRequest(
            http::verb::put, "127.0.0.1", gateway_port,
            "/v1/buckets/demo/multipart-uploads/" + upload_id + "/parts/1", "hello-", "");
        ASSERT_EQ(part1.result(), http::status::ok);
        auto part1_json = parser.parse(part1.body()).extract<Poco::JSON::Object::Ptr>();
        const auto part1_etag = part1_json->getValue<std::string>("etag");

        auto part2 = SendRequest(
            http::verb::put, "127.0.0.1", gateway_port,
            "/v1/buckets/demo/multipart-uploads/" + upload_id + "/parts/2", "world", "");
        ASSERT_EQ(part2.result(), http::status::ok);
        auto part2_json = parser.parse(part2.body()).extract<Poco::JSON::Object::Ptr>();
        const auto part2_etag = part2_json->getValue<std::string>("etag");

        auto parts = SendRequest(http::verb::get, "127.0.0.1", gateway_port,
                                 "/v1/buckets/demo/multipart-uploads/" + upload_id + "/parts", "",
                                 "");
        ASSERT_EQ(parts.result(), http::status::ok);
        auto parts_json = parser.parse(parts.body()).extract<Poco::JSON::Object::Ptr>();
        auto parts_arr = parts_json->getArray("parts");
        ASSERT_TRUE(parts_arr);
        ASSERT_EQ(parts_arr->size(), 2);

        auto storage_metrics_before =
            SendRequest(http::verb::get, "127.0.0.1", storage1_port, "/metrics", "", "");
        ASSERT_EQ(storage_metrics_before.result(), http::status::ok);
        const auto compose_before = ParseMetricCounter(
            storage_metrics_before.body(), "nebulafs_storage_node_blob_composes_total");
        ASSERT_TRUE(compose_before.has_value());

        const std::string complete_body = std::string("{\"parts\":[") +
                                          "{\"part_number\":1,\"etag\":\"" + part1_etag + "\"}," +
                                          "{\"part_number\":2,\"etag\":\"" + part2_etag + "\"}" +
                                          "]}";
        auto complete = SendRequest(
            http::verb::post, "127.0.0.1", gateway_port,
            "/v1/buckets/demo/multipart-uploads/" + upload_id + "/complete", complete_body,
            "application/json");
        ASSERT_EQ(complete.result(), http::status::ok);
        auto complete_json = parser.parse(complete.body()).extract<Poco::JSON::Object::Ptr>();
        EXPECT_FALSE(complete_json->getValue<std::string>("etag").empty());
        EXPECT_GT(complete_json->getValue<Poco::UInt64>("size"), 0);

        auto storage_metrics_after =
            SendRequest(http::verb::get, "127.0.0.1", storage1_port, "/metrics", "", "");
        ASSERT_EQ(storage_metrics_after.result(), http::status::ok);
        const auto compose_after = ParseMetricCounter(storage_metrics_after.body(),
                                                      "nebulafs_storage_node_blob_composes_total");
        ASSERT_TRUE(compose_after.has_value());
        EXPECT_GT(*compose_after, *compose_before);

        auto download = SendRequest(http::verb::get, "127.0.0.1", gateway_port,
                                    "/v1/buckets/demo/objects/movie.bin", "", "");
        EXPECT_EQ(download.result(), http::status::ok);
        EXPECT_EQ(download.body(), "hello-world");
    }

    CleanupTempDir(temp_dir);
}

TEST(IntegrationHttp, DistributedMultipartAbortEndpoint) {
    const char* enabled = std::getenv("NEBULAFS_ENABLE_DISTRIBUTED_IT");
    if (!enabled || std::string(enabled) != "1") {
        GTEST_SKIP() << "distributed integration lane is disabled";
    }

    const auto gateway_port = FindFreePort();
    const auto metadata_port = FindFreePort();
    const auto storage1_port = FindFreePort();
    const auto storage2_port = FindFreePort();
    const auto temp_dir = MakeTempDir();
    const std::string token = "distributed-test-token";
    const std::vector<std::string> storage_nodes = {
        "http://127.0.0.1:" + std::to_string(storage1_port),
        "http://127.0.0.1:" + std::to_string(storage2_port),
    };
    const auto metadata_url = "http://127.0.0.1:" + std::to_string(metadata_port);

    const auto metadata_config =
        WriteMetadataServiceConfig(temp_dir / "metadata", metadata_port, token, storage_nodes);
    const auto metadata_db = WriteDatabaseConfig(temp_dir / "metadata");
    const auto storage1_config = WriteStorageNodeConfig(temp_dir / "storage1", storage1_port, token);
    const auto storage2_config = WriteStorageNodeConfig(temp_dir / "storage2", storage2_port, token);
    const auto gateway_config = WriteGatewayDistributedConfig(temp_dir / "gateway", gateway_port,
                                                              metadata_url, storage_nodes, token);

    std::vector<std::string> metadata_args = {"--config", metadata_config.string(), "--database",
                                              metadata_db.string()};
    std::vector<std::string> storage1_args = {"--config", storage1_config.string()};
    std::vector<std::string> storage2_args = {"--config", storage2_config.string()};
    std::vector<std::string> gateway_args = {"--config", gateway_config.string(), "--database",
                                             metadata_db.string()};

    auto metadata_handle = Poco::Process::launch(NEBULAFS_METADATA_PATH, metadata_args);
    auto storage1_handle = Poco::Process::launch(NEBULAFS_STORAGE_NODE_PATH, storage1_args);
    auto storage2_handle = Poco::Process::launch(NEBULAFS_STORAGE_NODE_PATH, storage2_args);
    {
        ServerProcess metadata(std::move(metadata_handle));
        ServerProcess storage1(std::move(storage1_handle));
        ServerProcess storage2(std::move(storage2_handle));

        ASSERT_TRUE(WaitForHealth("127.0.0.1", metadata_port));
        ASSERT_TRUE(WaitForHealth("127.0.0.1", storage1_port));
        ASSERT_TRUE(WaitForHealth("127.0.0.1", storage2_port));

        auto gateway_handle = Poco::Process::launch(NEBULAFS_SERVER_PATH, gateway_args);
        ServerProcess gateway(std::move(gateway_handle));
        ASSERT_TRUE(WaitForHealth("127.0.0.1", gateway_port));

        auto create_bucket = SendRequest(http::verb::post, "127.0.0.1", gateway_port, "/v1/buckets",
                                         R"({"name":"demo"})", "application/json");
        ASSERT_EQ(create_bucket.result(), http::status::ok);

        auto initiate = SendRequest(http::verb::post, "127.0.0.1", gateway_port,
                                    "/v1/buckets/demo/multipart-uploads",
                                    R"({"object":"cancel.bin"})", "application/json");
        ASSERT_EQ(initiate.result(), http::status::ok);
        Poco::JSON::Parser parser;
        auto initiate_json = parser.parse(initiate.body()).extract<Poco::JSON::Object::Ptr>();
        const auto upload_id = initiate_json->getValue<std::string>("upload_id");

        auto part1 = SendRequest(
            http::verb::put, "127.0.0.1", gateway_port,
            "/v1/buckets/demo/multipart-uploads/" + upload_id + "/parts/1", "temp-data", "");
        ASSERT_EQ(part1.result(), http::status::ok);

        auto abort = SendRequest(http::verb::delete_, "127.0.0.1", gateway_port,
                                 "/v1/buckets/demo/multipart-uploads/" + upload_id, "", "");
        EXPECT_EQ(abort.result(), http::status::no_content);

        auto parts_after_abort =
            SendRequest(http::verb::get, "127.0.0.1", gateway_port,
                        "/v1/buckets/demo/multipart-uploads/" + upload_id + "/parts", "", "");
        EXPECT_EQ(parts_after_abort.result(), http::status::not_found);
    }

    CleanupTempDir(temp_dir);
}

TEST(IntegrationHttp, DistributedMultipartRejectsEtagMismatchOnComplete) {
    const char* enabled = std::getenv("NEBULAFS_ENABLE_DISTRIBUTED_IT");
    if (!enabled || std::string(enabled) != "1") {
        GTEST_SKIP() << "distributed integration lane is disabled";
    }

    const auto gateway_port = FindFreePort();
    const auto metadata_port = FindFreePort();
    const auto storage1_port = FindFreePort();
    const auto storage2_port = FindFreePort();
    const auto temp_dir = MakeTempDir();
    const std::string token = "distributed-test-token";
    const std::vector<std::string> storage_nodes = {
        "http://127.0.0.1:" + std::to_string(storage1_port),
        "http://127.0.0.1:" + std::to_string(storage2_port),
    };
    const auto metadata_url = "http://127.0.0.1:" + std::to_string(metadata_port);

    const auto metadata_config =
        WriteMetadataServiceConfig(temp_dir / "metadata", metadata_port, token, storage_nodes);
    const auto metadata_db = WriteDatabaseConfig(temp_dir / "metadata");
    const auto storage1_config = WriteStorageNodeConfig(temp_dir / "storage1", storage1_port, token);
    const auto storage2_config = WriteStorageNodeConfig(temp_dir / "storage2", storage2_port, token);
    const auto gateway_config = WriteGatewayDistributedConfig(temp_dir / "gateway", gateway_port,
                                                              metadata_url, storage_nodes, token);

    std::vector<std::string> metadata_args = {"--config", metadata_config.string(), "--database",
                                              metadata_db.string()};
    std::vector<std::string> storage1_args = {"--config", storage1_config.string()};
    std::vector<std::string> storage2_args = {"--config", storage2_config.string()};
    std::vector<std::string> gateway_args = {"--config", gateway_config.string(), "--database",
                                             metadata_db.string()};

    auto metadata_handle = Poco::Process::launch(NEBULAFS_METADATA_PATH, metadata_args);
    auto storage1_handle = Poco::Process::launch(NEBULAFS_STORAGE_NODE_PATH, storage1_args);
    auto storage2_handle = Poco::Process::launch(NEBULAFS_STORAGE_NODE_PATH, storage2_args);
    {
        ServerProcess metadata(std::move(metadata_handle));
        ServerProcess storage1(std::move(storage1_handle));
        ServerProcess storage2(std::move(storage2_handle));

        ASSERT_TRUE(WaitForHealth("127.0.0.1", metadata_port));
        ASSERT_TRUE(WaitForHealth("127.0.0.1", storage1_port));
        ASSERT_TRUE(WaitForHealth("127.0.0.1", storage2_port));

        auto gateway_handle = Poco::Process::launch(NEBULAFS_SERVER_PATH, gateway_args);
        ServerProcess gateway(std::move(gateway_handle));
        ASSERT_TRUE(WaitForHealth("127.0.0.1", gateway_port));

        auto create_bucket = SendRequest(http::verb::post, "127.0.0.1", gateway_port, "/v1/buckets",
                                         R"({"name":"demo"})", "application/json");
        ASSERT_EQ(create_bucket.result(), http::status::ok);

        auto initiate = SendRequest(http::verb::post, "127.0.0.1", gateway_port,
                                    "/v1/buckets/demo/multipart-uploads",
                                    R"({"object":"broken.bin"})", "application/json");
        ASSERT_EQ(initiate.result(), http::status::ok);
        Poco::JSON::Parser parser;
        auto initiate_json = parser.parse(initiate.body()).extract<Poco::JSON::Object::Ptr>();
        const auto upload_id = initiate_json->getValue<std::string>("upload_id");

        auto part1 = SendRequest(
            http::verb::put, "127.0.0.1", gateway_port,
            "/v1/buckets/demo/multipart-uploads/" + upload_id + "/parts/1", "temp-data", "");
        ASSERT_EQ(part1.result(), http::status::ok);

        auto complete = SendRequest(
            http::verb::post, "127.0.0.1", gateway_port,
            "/v1/buckets/demo/multipart-uploads/" + upload_id + "/complete",
            R"({"parts":[{"part_number":1,"etag":"wrong-etag"}]})", "application/json");
        EXPECT_EQ(complete.result(), http::status::conflict);

        auto download = SendRequest(http::verb::get, "127.0.0.1", gateway_port,
                                    "/v1/buckets/demo/objects/broken.bin", "", "");
        EXPECT_EQ(download.result(), http::status::not_found);
    }

    CleanupTempDir(temp_dir);
}

TEST(IntegrationHttp, DistributedMultipartComposeQuorumFailureKeepsObjectInvisible) {
    const char* enabled = std::getenv("NEBULAFS_ENABLE_DISTRIBUTED_IT");
    if (!enabled || std::string(enabled) != "1") {
        GTEST_SKIP() << "distributed integration lane is disabled";
    }

    const auto gateway_port = FindFreePort();
    const auto metadata_port = FindFreePort();
    const auto storage1_port = FindFreePort();
    const auto storage2_port = FindFreePort();
    const auto temp_dir = MakeTempDir();
    const std::string token = "distributed-test-token";
    const std::vector<std::string> storage_nodes = {
        "http://127.0.0.1:" + std::to_string(storage1_port),
        "http://127.0.0.1:" + std::to_string(storage2_port),
    };
    const auto metadata_url = "http://127.0.0.1:" + std::to_string(metadata_port);

    DistributedGatewayConfigOptions options;
    options.replication_factor = 2;
    options.min_write_acks = 2;
    const auto metadata_config =
        WriteMetadataServiceConfig(temp_dir / "metadata", metadata_port, token, storage_nodes);
    const auto metadata_db = WriteDatabaseConfig(temp_dir / "metadata");
    const auto storage1_config = WriteStorageNodeConfig(temp_dir / "storage1", storage1_port, token);
    const auto storage2_config = WriteStorageNodeConfig(temp_dir / "storage2", storage2_port, token);
    const auto gateway_config = WriteGatewayDistributedConfig(
        temp_dir / "gateway", gateway_port, metadata_url, storage_nodes, token, options);

    std::vector<std::string> metadata_args = {"--config", metadata_config.string(), "--database",
                                              metadata_db.string()};
    std::vector<std::string> storage1_args = {"--config", storage1_config.string()};
    std::vector<std::string> storage2_args = {"--config", storage2_config.string()};
    std::vector<std::string> gateway_args = {"--config", gateway_config.string(), "--database",
                                             metadata_db.string()};

    auto metadata_handle = Poco::Process::launch(NEBULAFS_METADATA_PATH, metadata_args);
    auto storage1_handle = Poco::Process::launch(NEBULAFS_STORAGE_NODE_PATH, storage1_args);
    auto storage2_handle = Poco::Process::launch(NEBULAFS_STORAGE_NODE_PATH, storage2_args);
    {
        ServerProcess metadata(std::move(metadata_handle));
        ServerProcess storage1(std::move(storage1_handle));
        ServerProcess storage2(std::move(storage2_handle));

        ASSERT_TRUE(WaitForHealth("127.0.0.1", metadata_port));
        ASSERT_TRUE(WaitForHealth("127.0.0.1", storage1_port));
        ASSERT_TRUE(WaitForHealth("127.0.0.1", storage2_port));

        auto gateway_handle = Poco::Process::launch(NEBULAFS_SERVER_PATH, gateway_args);
        ServerProcess gateway(std::move(gateway_handle));
        ASSERT_TRUE(WaitForHealth("127.0.0.1", gateway_port));

        auto create_bucket = SendRequest(http::verb::post, "127.0.0.1", gateway_port, "/v1/buckets",
                                         R"({"name":"demo"})", "application/json");
        ASSERT_EQ(create_bucket.result(), http::status::ok);

        auto initiate = SendRequest(http::verb::post, "127.0.0.1", gateway_port,
                                    "/v1/buckets/demo/multipart-uploads",
                                    R"({"object":"quorum.bin"})", "application/json");
        ASSERT_EQ(initiate.result(), http::status::ok);
        Poco::JSON::Parser parser;
        auto initiate_json = parser.parse(initiate.body()).extract<Poco::JSON::Object::Ptr>();
        const auto upload_id = initiate_json->getValue<std::string>("upload_id");

        auto part1 = SendRequest(
            http::verb::put, "127.0.0.1", gateway_port,
            "/v1/buckets/demo/multipart-uploads/" + upload_id + "/parts/1", "hello-", "");
        ASSERT_EQ(part1.result(), http::status::ok);
        auto part2 = SendRequest(
            http::verb::put, "127.0.0.1", gateway_port,
            "/v1/buckets/demo/multipart-uploads/" + upload_id + "/parts/2", "world", "");
        ASSERT_EQ(part2.result(), http::status::ok);

        auto part1_json = parser.parse(part1.body()).extract<Poco::JSON::Object::Ptr>();
        auto part2_json = parser.parse(part2.body()).extract<Poco::JSON::Object::Ptr>();
        const auto part1_etag = part1_json->getValue<std::string>("etag");
        const auto part2_etag = part2_json->getValue<std::string>("etag");

        auto metrics_before = SendRequest(http::verb::get, "127.0.0.1", gateway_port, "/metrics",
                                          "", "");
        ASSERT_EQ(metrics_before.result(), http::status::ok);
        const auto compose_failures_before = ParseMetricCounter(
            metrics_before.body(), "nebulafs_gateway_multipart_compose_failures_total");
        const auto rollback_attempts_before = ParseMetricCounter(
            metrics_before.body(), "nebulafs_gateway_multipart_rollback_attempts_total");
        ASSERT_TRUE(compose_failures_before.has_value());
        ASSERT_TRUE(rollback_attempts_before.has_value());

        storage2.Stop();
        std::this_thread::sleep_for(std::chrono::milliseconds(100));

        const std::string complete_body = std::string("{\"parts\":[") +
                                          "{\"part_number\":1,\"etag\":\"" + part1_etag + "\"}," +
                                          "{\"part_number\":2,\"etag\":\"" + part2_etag + "\"}" +
                                          "]}";
        auto complete = SendRequest(
            http::verb::post, "127.0.0.1", gateway_port,
            "/v1/buckets/demo/multipart-uploads/" + upload_id + "/complete", complete_body,
            "application/json");
        ASSERT_EQ(complete.result(), http::status::internal_server_error);
        EXPECT_NE(complete.body().find("insufficient storage node compose acknowledgements"),
                  std::string::npos);

        auto missing = SendRequest(http::verb::get, "127.0.0.1", gateway_port,
                                   "/v1/buckets/demo/objects/quorum.bin", "", "");
        EXPECT_EQ(missing.result(), http::status::not_found);

        auto metrics_after = SendRequest(http::verb::get, "127.0.0.1", gateway_port, "/metrics",
                                         "", "");
        ASSERT_EQ(metrics_after.result(), http::status::ok);
        const auto compose_failures_after = ParseMetricCounter(
            metrics_after.body(), "nebulafs_gateway_multipart_compose_failures_total");
        const auto rollback_attempts_after = ParseMetricCounter(
            metrics_after.body(), "nebulafs_gateway_multipart_rollback_attempts_total");
        ASSERT_TRUE(compose_failures_after.has_value());
        ASSERT_TRUE(rollback_attempts_after.has_value());
        EXPECT_GT(*compose_failures_after, *compose_failures_before);
        EXPECT_GT(*rollback_attempts_after, *rollback_attempts_before);
    }

    CleanupTempDir(temp_dir);
}

TEST(IntegrationHttp, DistributedCleanupSweepRemovesExpiredMultipartParts) {
    const char* enabled = std::getenv("NEBULAFS_ENABLE_DISTRIBUTED_IT");
    if (!enabled || std::string(enabled) != "1") {
        GTEST_SKIP() << "distributed integration lane is disabled";
    }

    const auto gateway_port = FindFreePort();
    const auto metadata_port = FindFreePort();
    const auto storage1_port = FindFreePort();
    const auto storage2_port = FindFreePort();
    const auto temp_dir = MakeTempDir();
    const std::string token = "distributed-test-token";

    const std::vector<std::string> storage_nodes = {
        "http://127.0.0.1:" + std::to_string(storage1_port),
        "http://127.0.0.1:" + std::to_string(storage2_port),
    };
    const auto metadata_url = "http://127.0.0.1:" + std::to_string(metadata_port);

    DistributedGatewayConfigOptions options;
    options.replication_factor = 2;
    options.min_write_acks = 2;
    options.cleanup_enabled = true;
    options.cleanup_sweep_interval_seconds = 1;
    options.cleanup_grace_period_seconds = 0;
    options.cleanup_max_uploads_per_sweep = 50;
    options.multipart_max_upload_ttl_seconds = 1;

    const auto metadata_config =
        WriteMetadataServiceConfig(temp_dir / "metadata", metadata_port, token, storage_nodes);
    const auto metadata_db = WriteDatabaseConfig(temp_dir / "metadata");
    const auto storage1_config = WriteStorageNodeConfig(temp_dir / "storage1", storage1_port, token);
    const auto storage2_config = WriteStorageNodeConfig(temp_dir / "storage2", storage2_port, token);
    const auto gateway_config = WriteGatewayDistributedConfig(
        temp_dir / "gateway", gateway_port, metadata_url, storage_nodes, token, options);

    std::vector<std::string> metadata_args = {"--config", metadata_config.string(), "--database",
                                              metadata_db.string()};
    std::vector<std::string> storage1_args = {"--config", storage1_config.string()};
    std::vector<std::string> storage2_args = {"--config", storage2_config.string()};
    std::vector<std::string> gateway_args = {"--config", gateway_config.string(), "--database",
                                             metadata_db.string()};

    auto metadata_handle = Poco::Process::launch(NEBULAFS_METADATA_PATH, metadata_args);
    auto storage1_handle = Poco::Process::launch(NEBULAFS_STORAGE_NODE_PATH, storage1_args);
    auto storage2_handle = Poco::Process::launch(NEBULAFS_STORAGE_NODE_PATH, storage2_args);
    {
        ServerProcess metadata(std::move(metadata_handle));
        ServerProcess storage1(std::move(storage1_handle));
        ServerProcess storage2(std::move(storage2_handle));

        ASSERT_TRUE(WaitForHealth("127.0.0.1", metadata_port));
        ASSERT_TRUE(WaitForHealth("127.0.0.1", storage1_port));
        ASSERT_TRUE(WaitForHealth("127.0.0.1", storage2_port));

        auto gateway_handle = Poco::Process::launch(NEBULAFS_SERVER_PATH, gateway_args);
        ServerProcess gateway(std::move(gateway_handle));
        ASSERT_TRUE(WaitForHealth("127.0.0.1", gateway_port));

        auto create_bucket = SendRequest(http::verb::post, "127.0.0.1", gateway_port, "/v1/buckets",
                                         R"({"name":"demo"})", "application/json");
        ASSERT_EQ(create_bucket.result(), http::status::ok);

        auto initiate = SendRequest(http::verb::post, "127.0.0.1", gateway_port,
                                    "/v1/buckets/demo/multipart-uploads",
                                    R"({"object":"stale.bin"})", "application/json");
        ASSERT_EQ(initiate.result(), http::status::ok);
        Poco::JSON::Parser parser;
        auto initiate_json = parser.parse(initiate.body()).extract<Poco::JSON::Object::Ptr>();
        const auto upload_id = initiate_json->getValue<std::string>("upload_id");

        auto part = SendRequest(http::verb::put, "127.0.0.1", gateway_port,
                                "/v1/buckets/demo/multipart-uploads/" + upload_id + "/parts/1",
                                "stale-part", "");
        ASSERT_EQ(part.result(), http::status::ok);

        auto metrics_before = SendRequest(http::verb::get, "127.0.0.1", gateway_port, "/metrics",
                                          "", "");
        ASSERT_EQ(metrics_before.result(), http::status::ok);
        const auto cleanup_uploads_before = ParseMetricCounter(
            metrics_before.body(), "nebulafs_gateway_distributed_cleanup_uploads_total");
        const auto cleanup_blob_deletes_before = ParseMetricCounter(
            metrics_before.body(), "nebulafs_gateway_distributed_cleanup_blob_deletes_total");
        ASSERT_TRUE(cleanup_uploads_before.has_value());
        ASSERT_TRUE(cleanup_blob_deletes_before.has_value());

        const auto storage1_blob_dir = temp_dir / "storage1" / "storage" / "blobs";
        const auto storage2_blob_dir = temp_dir / "storage2" / "storage" / "blobs";
        bool cleaned = false;
        for (int i = 0; i < 40; ++i) {
            auto parts_after = SendRequest(
                http::verb::get, "127.0.0.1", gateway_port,
                "/v1/buckets/demo/multipart-uploads/" + upload_id + "/parts", "", "");
            if (parts_after.result() == http::status::not_found &&
                CountRegularFiles(storage1_blob_dir) == 0 &&
                CountRegularFiles(storage2_blob_dir) == 0) {
                cleaned = true;
                break;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(200));
        }
        EXPECT_TRUE(cleaned);

        auto metrics_after = SendRequest(http::verb::get, "127.0.0.1", gateway_port, "/metrics",
                                         "", "");
        ASSERT_EQ(metrics_after.result(), http::status::ok);
        const auto cleanup_uploads_after = ParseMetricCounter(
            metrics_after.body(), "nebulafs_gateway_distributed_cleanup_uploads_total");
        const auto cleanup_blob_deletes_after = ParseMetricCounter(
            metrics_after.body(), "nebulafs_gateway_distributed_cleanup_blob_deletes_total");
        ASSERT_TRUE(cleanup_uploads_after.has_value());
        ASSERT_TRUE(cleanup_blob_deletes_after.has_value());
        EXPECT_GT(*cleanup_uploads_after, *cleanup_uploads_before);
        EXPECT_GT(*cleanup_blob_deletes_after, *cleanup_blob_deletes_before);
    }

    CleanupTempDir(temp_dir);
}

TEST(IntegrationHttp, DistributedReadFallbackWhenPrimaryDown) {
    const char* enabled = std::getenv("NEBULAFS_ENABLE_DISTRIBUTED_IT");
    if (!enabled || std::string(enabled) != "1") {
        GTEST_SKIP() << "distributed integration lane is disabled";
    }

    const auto gateway_port = FindFreePort();
    const auto metadata_port = FindFreePort();
    const auto storage1_port = FindFreePort();
    const auto storage2_port = FindFreePort();
    const auto temp_dir = MakeTempDir();
    const std::string token = "distributed-test-token";

    const std::vector<std::string> storage_nodes = {
        "http://127.0.0.1:" + std::to_string(storage1_port),
        "http://127.0.0.1:" + std::to_string(storage2_port),
    };
    const auto metadata_url = "http://127.0.0.1:" + std::to_string(metadata_port);

    const auto metadata_config =
        WriteMetadataServiceConfig(temp_dir / "metadata", metadata_port, token, storage_nodes);
    const auto metadata_db = WriteDatabaseConfig(temp_dir / "metadata");
    const auto storage1_config = WriteStorageNodeConfig(temp_dir / "storage1", storage1_port, token);
    const auto storage2_config = WriteStorageNodeConfig(temp_dir / "storage2", storage2_port, token);
    const auto gateway_config = WriteGatewayDistributedConfig(temp_dir / "gateway", gateway_port,
                                                              metadata_url, storage_nodes, token);

    std::vector<std::string> metadata_args = {"--config", metadata_config.string(), "--database",
                                              metadata_db.string()};
    std::vector<std::string> storage1_args = {"--config", storage1_config.string()};
    std::vector<std::string> storage2_args = {"--config", storage2_config.string()};
    std::vector<std::string> gateway_args = {"--config", gateway_config.string(), "--database",
                                             metadata_db.string()};

    auto metadata_handle = Poco::Process::launch(NEBULAFS_METADATA_PATH, metadata_args);
    auto storage1_handle = Poco::Process::launch(NEBULAFS_STORAGE_NODE_PATH, storage1_args);
    auto storage2_handle = Poco::Process::launch(NEBULAFS_STORAGE_NODE_PATH, storage2_args);
    {
        ServerProcess metadata(std::move(metadata_handle));
        ServerProcess storage1(std::move(storage1_handle));
        ServerProcess storage2(std::move(storage2_handle));

        ASSERT_TRUE(WaitForHealth("127.0.0.1", metadata_port));
        ASSERT_TRUE(WaitForHealth("127.0.0.1", storage1_port));
        ASSERT_TRUE(WaitForHealth("127.0.0.1", storage2_port));

        auto gateway_handle = Poco::Process::launch(NEBULAFS_SERVER_PATH, gateway_args);
        ServerProcess gateway(std::move(gateway_handle));
        ASSERT_TRUE(WaitForHealth("127.0.0.1", gateway_port));

        auto create_bucket = SendRequest(http::verb::post, "127.0.0.1", gateway_port, "/v1/buckets",
                                         R"({"name":"demo"})", "application/json");
        ASSERT_EQ(create_bucket.result(), http::status::ok);

        auto upload = SendRequest(http::verb::put, "127.0.0.1", gateway_port,
                                  "/v1/buckets/demo/objects/readme.txt", "distributed-data", "");
        ASSERT_EQ(upload.result(), http::status::ok);

        auto metrics_before = SendRequest(http::verb::get, "127.0.0.1", gateway_port, "/metrics",
                                          "", "");
        ASSERT_EQ(metrics_before.result(), http::status::ok);
        const auto before_fallback =
            ParseMetricCounter(metrics_before.body(), "nebulafs_gateway_replica_fallback_total");
        ASSERT_TRUE(before_fallback.has_value());

        // Force primary replica failure and verify read falls back to secondary.
        storage1.Stop();
        std::this_thread::sleep_for(std::chrono::milliseconds(100));

        auto download = SendRequest(http::verb::get, "127.0.0.1", gateway_port,
                                    "/v1/buckets/demo/objects/readme.txt", "", "");
        ASSERT_EQ(download.result(), http::status::ok);
        EXPECT_EQ(download.body(), "distributed-data");

        auto metrics_after = SendRequest(http::verb::get, "127.0.0.1", gateway_port, "/metrics", "",
                                         "");
        ASSERT_EQ(metrics_after.result(), http::status::ok);
        const auto after_fallback =
            ParseMetricCounter(metrics_after.body(), "nebulafs_gateway_replica_fallback_total");
        ASSERT_TRUE(after_fallback.has_value());
        EXPECT_GT(*after_fallback, *before_fallback);
    }

    CleanupTempDir(temp_dir);
}

TEST(IntegrationHttp, DistributedWriteQuorumFailureKeepsObjectInvisible) {
    const char* enabled = std::getenv("NEBULAFS_ENABLE_DISTRIBUTED_IT");
    if (!enabled || std::string(enabled) != "1") {
        GTEST_SKIP() << "distributed integration lane is disabled";
    }

    const auto gateway_port = FindFreePort();
    const auto metadata_port = FindFreePort();
    const auto storage1_port = FindFreePort();
    const auto storage2_port = FindFreePort();
    const auto temp_dir = MakeTempDir();
    const std::string token = "distributed-test-token";

    const std::vector<std::string> storage_nodes = {
        "http://127.0.0.1:" + std::to_string(storage1_port),
        "http://127.0.0.1:" + std::to_string(storage2_port),
    };
    const auto metadata_url = "http://127.0.0.1:" + std::to_string(metadata_port);

    const auto metadata_config =
        WriteMetadataServiceConfig(temp_dir / "metadata", metadata_port, token, storage_nodes);
    const auto metadata_db = WriteDatabaseConfig(temp_dir / "metadata");
    const auto storage1_config = WriteStorageNodeConfig(temp_dir / "storage1", storage1_port, token);
    DistributedGatewayConfigOptions options;
    options.replication_factor = 2;
    options.min_write_acks = 2;
    const auto gateway_config = WriteGatewayDistributedConfig(
        temp_dir / "gateway", gateway_port, metadata_url, storage_nodes, token, options);

    std::vector<std::string> metadata_args = {"--config", metadata_config.string(), "--database",
                                              metadata_db.string()};
    std::vector<std::string> storage1_args = {"--config", storage1_config.string()};
    std::vector<std::string> gateway_args = {"--config", gateway_config.string(), "--database",
                                             metadata_db.string()};

    auto metadata_handle = Poco::Process::launch(NEBULAFS_METADATA_PATH, metadata_args);
    auto storage1_handle = Poco::Process::launch(NEBULAFS_STORAGE_NODE_PATH, storage1_args);
    {
        ServerProcess metadata(std::move(metadata_handle));
        ServerProcess storage1(std::move(storage1_handle));

        ASSERT_TRUE(WaitForHealth("127.0.0.1", metadata_port));
        ASSERT_TRUE(WaitForHealth("127.0.0.1", storage1_port));

        // Intentionally do not start storage2 to force quorum failure.
        auto gateway_handle = Poco::Process::launch(NEBULAFS_SERVER_PATH, gateway_args);
        ServerProcess gateway(std::move(gateway_handle));
        ASSERT_TRUE(WaitForHealth("127.0.0.1", gateway_port));

        auto create_bucket = SendRequest(http::verb::post, "127.0.0.1", gateway_port, "/v1/buckets",
                                         R"({"name":"demo"})", "application/json");
        ASSERT_EQ(create_bucket.result(), http::status::ok);

        auto metrics_before = SendRequest(http::verb::get, "127.0.0.1", gateway_port, "/metrics",
                                          "", "");
        ASSERT_EQ(metrics_before.result(), http::status::ok);
        const auto before_storage_failures = ParseMetricCounter(
            metrics_before.body(), "nebulafs_gateway_storage_put_failures_total");
        ASSERT_TRUE(before_storage_failures.has_value());

        auto upload = SendRequest(http::verb::put, "127.0.0.1", gateway_port,
                                  "/v1/buckets/demo/objects/readme.txt", "distributed-data", "");
        ASSERT_EQ(upload.result(), http::status::internal_server_error);
        EXPECT_NE(upload.body().find("insufficient storage node write acknowledgements"),
                  std::string::npos);

        const auto storage1_blob_dir = temp_dir / "storage1" / "storage" / "blobs";
        bool rollback_completed = false;
        for (int i = 0; i < 10; ++i) {
            if (CountRegularFiles(storage1_blob_dir) == 0) {
                rollback_completed = true;
                break;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(50));
        }
        EXPECT_TRUE(rollback_completed);

        auto metrics_after = SendRequest(http::verb::get, "127.0.0.1", gateway_port, "/metrics", "",
                                         "");
        ASSERT_EQ(metrics_after.result(), http::status::ok);
        const auto after_storage_failures =
            ParseMetricCounter(metrics_after.body(), "nebulafs_gateway_storage_put_failures_total");
        ASSERT_TRUE(after_storage_failures.has_value());
        EXPECT_GT(*after_storage_failures, *before_storage_failures);

        auto missing = SendRequest(http::verb::get, "127.0.0.1", gateway_port,
                                   "/v1/buckets/demo/objects/readme.txt", "", "");
        EXPECT_EQ(missing.result(), http::status::not_found);
    }

    CleanupTempDir(temp_dir);
}

TEST(IntegrationHttp, DistributedInternalEndpointsRejectInvalidServiceToken) {
    const char* enabled = std::getenv("NEBULAFS_ENABLE_DISTRIBUTED_IT");
    if (!enabled || std::string(enabled) != "1") {
        GTEST_SKIP() << "distributed integration lane is disabled";
    }

    const auto metadata_port = FindFreePort();
    const auto storage_port = FindFreePort();
    const auto temp_dir = MakeTempDir();
    const std::string token = "distributed-test-token";

    const std::vector<std::string> storage_nodes = {
        "http://127.0.0.1:" + std::to_string(storage_port),
    };
    const auto metadata_config =
        WriteMetadataServiceConfig(temp_dir / "metadata", metadata_port, token, storage_nodes);
    const auto metadata_db = WriteDatabaseConfig(temp_dir / "metadata");
    const auto storage_config = WriteStorageNodeConfig(temp_dir / "storage", storage_port, token);

    std::vector<std::string> metadata_args = {"--config", metadata_config.string(), "--database",
                                              metadata_db.string()};
    std::vector<std::string> storage_args = {"--config", storage_config.string()};

    auto metadata_handle = Poco::Process::launch(NEBULAFS_METADATA_PATH, metadata_args);
    auto storage_handle = Poco::Process::launch(NEBULAFS_STORAGE_NODE_PATH, storage_args);
    {
        ServerProcess metadata(std::move(metadata_handle));
        ServerProcess storage(std::move(storage_handle));

        ASSERT_TRUE(WaitForHealth("127.0.0.1", metadata_port));
        ASSERT_TRUE(WaitForHealth("127.0.0.1", storage_port));

        auto metadata_metrics_before =
            SendRequest(http::verb::get, "127.0.0.1", metadata_port, "/metrics", "", "");
        ASSERT_EQ(metadata_metrics_before.result(), http::status::ok);
        auto metadata_allocate_failures_before = ParseMetricCounter(
            metadata_metrics_before.body(), "nebulafs_metadata_allocate_failures_total");
        ASSERT_TRUE(metadata_allocate_failures_before.has_value());

        auto storage_metrics_before =
            SendRequest(http::verb::get, "127.0.0.1", storage_port, "/metrics", "", "");
        ASSERT_EQ(storage_metrics_before.result(), http::status::ok);
        auto storage_read_failures_before = ParseMetricCounter(
            storage_metrics_before.body(), "nebulafs_storage_node_blob_read_failures_total");
        auto storage_write_failures_before = ParseMetricCounter(
            storage_metrics_before.body(), "nebulafs_storage_node_blob_write_failures_total");
        auto storage_compose_failures_before = ParseMetricCounter(
            storage_metrics_before.body(), "nebulafs_storage_node_blob_compose_failures_total");
        ASSERT_TRUE(storage_read_failures_before.has_value());
        ASSERT_TRUE(storage_write_failures_before.has_value());
        ASSERT_TRUE(storage_compose_failures_before.has_value());

        auto metadata_no_token = SendRequest(http::verb::get, "127.0.0.1", metadata_port,
                                             "/internal/v1/buckets/list", "", "");
        EXPECT_EQ(metadata_no_token.result(), http::status::unauthorized);
        ExpectErrorEnvelope(metadata_no_token, "UNAUTHORIZED");

        auto metadata_bad_token = SendRequest(http::verb::get, "127.0.0.1", metadata_port,
                                              "/internal/v1/buckets/list", "", "",
                                              {{"Authorization", "Bearer wrong-token"}});
        EXPECT_EQ(metadata_bad_token.result(), http::status::unauthorized);
        ExpectErrorEnvelope(metadata_bad_token, "UNAUTHORIZED");

        auto metadata_good_token = SendRequest(http::verb::get, "127.0.0.1", metadata_port,
                                               "/internal/v1/buckets/list", "", "",
                                               {{"Authorization", "Bearer " + token}});
        EXPECT_EQ(metadata_good_token.result(), http::status::ok);

        auto metadata_allocate_invalid = SendRequest(
            http::verb::post, "127.0.0.1", metadata_port, "/internal/v1/objects/allocate-write",
            R"({"bucket":"missing","object":"ghost.txt","replication_factor":1,"service_token":"distributed-test-token"})",
            "application/json", {{"Authorization", "Bearer " + token}});
        EXPECT_EQ(metadata_allocate_invalid.result(), http::status::not_found);
        ExpectErrorEnvelope(metadata_allocate_invalid, "NOT_FOUND");

        auto storage_no_token = SendRequest(http::verb::get, "127.0.0.1", storage_port,
                                            "/internal/v1/blobs/test-blob", "", "");
        EXPECT_EQ(storage_no_token.result(), http::status::unauthorized);
        ExpectErrorEnvelope(storage_no_token, "UNAUTHORIZED");

        auto storage_bad_token = SendRequest(http::verb::get, "127.0.0.1", storage_port,
                                             "/internal/v1/blobs/test-blob", "", "",
                                             {{"Authorization", "Bearer wrong-token"}});
        EXPECT_EQ(storage_bad_token.result(), http::status::unauthorized);
        ExpectErrorEnvelope(storage_bad_token, "UNAUTHORIZED");

        auto storage_good_token = SendRequest(http::verb::get, "127.0.0.1", storage_port,
                                              "/internal/v1/blobs/test-blob", "", "",
                                              {{"Authorization", "Bearer " + token}});
        EXPECT_EQ(storage_good_token.result(), http::status::not_found);
        ExpectErrorEnvelope(storage_good_token, "NOT_FOUND");

        auto storage_missing_placement = SendRequest(
            http::verb::put, "127.0.0.1", storage_port, "/internal/v1/blobs/test-blob", "abc",
            "application/octet-stream", {{"Authorization", "Bearer " + token}});
        EXPECT_EQ(storage_missing_placement.result(), http::status::unauthorized);
        ExpectErrorEnvelope(storage_missing_placement, "UNAUTHORIZED");

        auto storage_compose_no_token = SendRequest(
            http::verb::post, "127.0.0.1", storage_port, "/internal/v1/blobs/composed/compose",
            R"({"source_blob_ids":["part-1"]})", "application/json");
        EXPECT_EQ(storage_compose_no_token.result(), http::status::unauthorized);
        ExpectErrorEnvelope(storage_compose_no_token, "UNAUTHORIZED");

        auto storage_compose_missing_placement = SendRequest(
            http::verb::post, "127.0.0.1", storage_port, "/internal/v1/blobs/composed/compose",
            R"({"source_blob_ids":["part-1"]})", "application/json",
            {{"Authorization", "Bearer " + token}});
        EXPECT_EQ(storage_compose_missing_placement.result(), http::status::unauthorized);
        ExpectErrorEnvelope(storage_compose_missing_placement, "UNAUTHORIZED");

        auto metadata_metrics_after =
            SendRequest(http::verb::get, "127.0.0.1", metadata_port, "/metrics", "", "");
        ASSERT_EQ(metadata_metrics_after.result(), http::status::ok);
        auto metadata_allocate_failures_after = ParseMetricCounter(
            metadata_metrics_after.body(), "nebulafs_metadata_allocate_failures_total");
        ASSERT_TRUE(metadata_allocate_failures_after.has_value());
        EXPECT_GT(*metadata_allocate_failures_after, *metadata_allocate_failures_before);

        auto storage_metrics_after =
            SendRequest(http::verb::get, "127.0.0.1", storage_port, "/metrics", "", "");
        ASSERT_EQ(storage_metrics_after.result(), http::status::ok);
        auto storage_read_failures_after = ParseMetricCounter(
            storage_metrics_after.body(), "nebulafs_storage_node_blob_read_failures_total");
        auto storage_write_failures_after = ParseMetricCounter(
            storage_metrics_after.body(), "nebulafs_storage_node_blob_write_failures_total");
        auto storage_compose_failures_after = ParseMetricCounter(
            storage_metrics_after.body(), "nebulafs_storage_node_blob_compose_failures_total");
        ASSERT_TRUE(storage_read_failures_after.has_value());
        ASSERT_TRUE(storage_write_failures_after.has_value());
        ASSERT_TRUE(storage_compose_failures_after.has_value());
        EXPECT_GT(*storage_read_failures_after, *storage_read_failures_before);
        EXPECT_GT(*storage_write_failures_after, *storage_write_failures_before);
        EXPECT_GT(*storage_compose_failures_after, *storage_compose_failures_before);
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

        auto metrics_without_token =
            SendRequest(http::verb::get, "127.0.0.1", port, "/metrics", "", "");
        EXPECT_EQ(metrics_without_token.result(), http::status::unauthorized);

        auto bad_token = SendRequest(http::verb::get, "127.0.0.1", port, "/v1/buckets", "", "",
                                     {{"Authorization", "Bearer invalid.token"}});
        EXPECT_EQ(bad_token.result(), http::status::unauthorized);

        auto metrics_bad_token = SendRequest(http::verb::get, "127.0.0.1", port, "/metrics", "", "",
                                             {{"Authorization", "Bearer invalid.token"}});
        EXPECT_EQ(metrics_bad_token.result(), http::status::unauthorized);

        const auto token = MakeValidToken(auth.issuer, auth.audience, kid, key.get());
        auto with_token = SendRequest(http::verb::get, "127.0.0.1", port, "/v1/buckets", "", "",
                                      {{"Authorization", "Bearer " + token}});
        EXPECT_EQ(with_token.result(), http::status::ok);

        auto metrics_with_token = SendRequest(http::verb::get, "127.0.0.1", port, "/metrics", "", "",
                                              {{"Authorization", "Bearer " + token}});
        ASSERT_EQ(metrics_with_token.result(), http::status::ok);
        EXPECT_TRUE(ParseMetricCounter(metrics_with_token.body(),
                                       "nebulafs_http_requests_rate_limited_total")
                        .has_value());
        EXPECT_TRUE(
            ParseMetricCounter(metrics_with_token.body(), "nebulafs_http_requests_timed_out_total")
                .has_value());

        auto create_bucket = SendRequest(http::verb::post, "127.0.0.1", port, "/v1/buckets",
                                         R"({"name":"authdemo"})", "application/json",
                                         {{"Authorization", "Bearer " + token}});
        EXPECT_EQ(create_bucket.result(), http::status::ok);
    }

    CleanupTempDir(temp_dir);
}
