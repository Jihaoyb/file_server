#include <gtest/gtest.h>

#include <boost/asio.hpp>
#include <boost/beast.hpp>
#include <Poco/JSON/Object.h>
#include <Poco/JSON/Parser.h>
#include <Poco/Process.h>

#include <chrono>
#include <filesystem>
#include <fstream>
#include <exception>
#include <string>
#include <thread>
#include <vector>

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

class ServerProcess {
public:
    explicit ServerProcess(Poco::ProcessHandle handle) : handle_(std::move(handle)) {}

    ~ServerProcess() {
        if (Poco::Process::isRunning(handle_)) {
            Poco::Process::kill(handle_);
            Poco::Process::wait(handle_);
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

std::filesystem::path WriteServerConfig(const std::filesystem::path& dir,
                                        unsigned short port) {
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
        << "    \"base_path\": \"" << storage_dir.string() << "\",\n"
        << "    \"temp_path\": \"" << temp_dir.string() << "\"\n"
        << "  },\n"
        << "  \"observability\": {\n"
        << "    \"log_level\": \"warning\"\n"
        << "  }\n"
        << "}\n";
    return config_path;
}

std::filesystem::path WriteDatabaseConfig(const std::filesystem::path& dir) {
    const auto db_path = dir / "metadata.db";
    const auto config_path = dir / "database.json";
    std::ofstream out(config_path);
    out << "{\n"
        << "  \"sqlite\": {\n"
        << "    \"path\": \"" << db_path.string() << "\"\n"
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

    std::filesystem::remove_all(temp_dir);
}
