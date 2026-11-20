#include "nebulafs/http/verified_multipart.h"

#include <algorithm>
#include <array>
#include <cerrno>
#include <charconv>
#include <chrono>
#include <cstdio>
#include <filesystem>
#include <fstream>
#include <memory>
#include <sstream>
#include <stdexcept>
#include <system_error>

#ifdef _WIN32
#include <io.h>
#include <windows.h>
#else
#include <fcntl.h>
#include <unistd.h>
#endif

#include <boost/asio/post.hpp>
#include <boost/asio/thread_pool.hpp>
#include <Poco/DigestEngine.h>
#include <Poco/JSON/Array.h>
#include <Poco/JSON/Object.h>
#include <Poco/JSON/Parser.h>
#include <Poco/SHA2Engine.h>
#include <Poco/UUIDGenerator.h>

#include "nebulafs/core/logger.h"
#include "nebulafs/core/time.h"
#include "nebulafs/storage/local_storage.h"

namespace nebulafs::http {
namespace {
namespace fs = std::filesystem;
namespace http = boost::beast::http;
using metadata::VerifiedUpload;
using Object = Poco::JSON::Object;
using Status = http::status;
constexpr std::uint64_t kMaxFile = 12000000000ULL;
constexpr std::uint64_t kReserve = 10000000000ULL;
constexpr int kMaxParts = 16384;
constexpr char kBucket[] = "pilot";

class Failure : public std::runtime_error {
public:
    Failure(Status status, const char* message) : std::runtime_error(message), status(status) {}
    Status status;
};

HttpResponse Json(int version, const Object& obj, Status status = Status::ok) {
    std::ostringstream body;
    obj.stringify(body);
    HttpResponse response{status, version};
    response.set(http::field::content_type, "application/json");
    response.set(http::field::cache_control, "no-store");
    response.body() = body.str();
    response.prepare_payload();
    return response;
}

Object::Ptr Parse(const std::string& body) {
    try {
        return Poco::JSON::Parser().parse(body).extract<Object::Ptr>();
    } catch (const std::exception&) {
        throw Failure(Status::bad_request, "expected a JSON object");
    }
}

void RequireSpace(const fs::path& path, std::uint64_t required) {
    if (fs::space(path).available < required + kReserve) {
        throw Failure(Status::insufficient_storage, "insufficient disk space for transfer");
    }
}

void ThrowFileError(const char* operation) {
    throw std::system_error(errno ? errno : EIO, std::generic_category(), operation);
}

class DurableWriter {
public:
    explicit DurableWriter(const fs::path& path)
#ifdef _WIN32
        : file_(_wfopen(path.c_str(), L"wb"), &std::fclose) {
#else
        : file_(std::fopen(path.c_str(), "wb"), &std::fclose) {
#endif
        if (!file_) ThrowFileError("open upload file");
    }

    void Write(const char* data, std::size_t size) {
        if (std::fwrite(data, 1, size, file_.get()) != size) ThrowFileError("write upload file");
    }

    void Close() {
        if (std::fflush(file_.get()) != 0) ThrowFileError("flush upload file");
#ifdef _WIN32
        if (_commit(_fileno(file_.get())) != 0) ThrowFileError("sync upload file");
#elif defined(__APPLE__)
        if (::fcntl(fileno(file_.get()), F_FULLFSYNC) != 0) ThrowFileError("sync upload file");
#else
        if (::fsync(fileno(file_.get())) != 0) ThrowFileError("sync upload file");
#endif
        if (std::fclose(file_.release()) != 0) ThrowFileError("close upload file");
    }

private:
    std::unique_ptr<std::FILE, decltype(&std::fclose)> file_;
};

void SyncDirectory(const fs::path& dir) {
#ifndef _WIN32
    const int fd = ::open(dir.c_str(), O_RDONLY | O_DIRECTORY);
    if (fd < 0) ThrowFileError("open upload directory");
    const int sync_result = ::fsync(fd);
    const int sync_error = errno;
    const int close_result = ::close(fd);
    if (sync_result != 0) throw std::system_error(sync_error, std::generic_category(), "sync upload directory");
    if (close_result != 0) ThrowFileError("close upload directory");
#else
    // Windows uses a write-through move to persist the directory entry.
    (void)dir;
#endif
}

void RenameUploadFile(const fs::path& from, const fs::path& to) {
#ifdef _WIN32
    if (!MoveFileExW(from.c_str(), to.c_str(), MOVEFILE_REPLACE_EXISTING | MOVEFILE_WRITE_THROUGH)) {
        throw std::system_error(GetLastError(), std::system_category(), "rename upload file");
    }
#else
    fs::rename(from, to);
#endif
}

bool ValidName(const std::string& value) {
    return !value.empty() && value.size() <= 255 && value != "." && value != ".." &&
           std::none_of(value.begin(), value.end(), [](unsigned char c) {
               return c < 32 || c == 127 || c == '/' || c == '\\';
           });
}

Object Progress(const VerifiedUpload& upload) {
    Object obj;
    obj.set("state", upload.state);
    obj.set("upload_id", upload.upload_id);
    obj.set("name", upload.display_name);
    obj.set("size", static_cast<Poco::UInt64>(upload.size_bytes));
    if (upload.state == "completed") {
        obj.set("etag", upload.sha256);
        obj.set("sha256", upload.sha256);
        obj.set("finalization_ms", static_cast<Poco::Int64>(upload.finalization_ms));
    }
    if (!upload.error.empty()) {
        obj.set("error", upload.error);
    }
    return obj;
}

void VerifyFile(const fs::path& path, const VerifiedUpload& upload) {
    std::ifstream input(path, std::ios::binary);
    input.exceptions(std::ios::badbit);
    if (!input) throw std::runtime_error("could not open assembled file");
    std::array<char, 128 * 1024> buffer;
    Poco::SHA2Engine256 hash;
    std::uint64_t size = 0;
    while (input.read(buffer.data(), buffer.size()) || input.gcount()) {
        hash.update(buffer.data(), static_cast<std::size_t>(input.gcount()));
        size += static_cast<std::uint64_t>(input.gcount());
    }
    if (size != upload.size_bytes || Poco::DigestEngine::digestToHex(hash.digest()) != upload.sha256) {
        throw std::runtime_error("assembled file does not match source size and SHA-256");
    }
}

void Assemble(const fs::path& path, const std::vector<metadata::MultipartPart>& parts) {
    DurableWriter out(path);
    std::array<char, 128 * 1024> buffer;
    for (const auto& part : parts) {
        std::ifstream in(part.temp_path, std::ios::binary);
        in.exceptions(std::ios::badbit);
        if (!in) throw std::runtime_error("could not open uploaded part");
        while (in.read(buffer.data(), buffer.size()) || in.gcount()) {
            out.Write(buffer.data(), static_cast<std::size_t>(in.gcount()));
        }
    }
    out.Close();
}

struct UploadService {
    std::shared_ptr<metadata::SqliteMetadataStore> db;
    std::shared_ptr<storage::StorageBackend> storage;
    int ttl_seconds;
    // Protected by upload_mutex. SQLite may be unwritable when the worker first reports failure.
    std::string pending_rollback;

    fs::path Directory(const std::string& id) const {
        return fs::path(storage->temp_path()) / "multipart" / id;
    }

    void RetryPendingRollback() {
        if (!pending_rollback.empty()) {
            db->SetVerifiedState(pending_rollback, "uploading",
                "assembly or verification failed; retry after checking host storage and source bytes");
            pending_rollback.clear();
        }
    }

    VerifiedUpload Get(const std::string& id) {
        RetryPendingRollback();
        auto row = db->GetVerifiedUpload(kBucket, id);
        if (!row.ok()) throw Failure(Status::not_found, "upload not found");
        if (row.value().state != "completed" && row.value().state != "finalizing" &&
            row.value().expires_at <= core::NowIso8601()) {
            throw Failure(Status::gone, "upload session expired");
        }
        return row.value();
    }

    std::vector<metadata::MultipartPart> Parts(const std::string& id) const {
        auto listed = db->ListMultipartParts(id);
        if (!listed.ok()) throw std::runtime_error("could not list uploaded parts");
        return listed.value();
    }

    HttpResponse Start(const HttpRequest& req) {
        auto body = Parse(req.body());
        VerifiedUpload upload;
        const auto size = body->get("size");
        if (!body->get("object").isString() || !body->get("sha256").isString() || !size.isInteger()) {
            throw Failure(Status::bad_request, "object, integer size and sha256 are required");
        }
        upload.display_name = body->getValue<std::string>("object");
        upload.sha256 = body->getValue<std::string>("sha256");
        if (!ValidName(upload.display_name) || upload.sha256.size() != 64 ||
            upload.sha256.find_first_not_of("0123456789abcdef") != std::string::npos) {
            throw Failure(Status::bad_request, "invalid file name or SHA-256");
        }
        if (size.isSigned() && size.convert<Poco::Int64>() < 0) {
            throw Failure(Status::bad_request, "negative file size");
        }
        upload.size_bytes = size.convert<Poco::UInt64>();
        if (upload.size_bytes > kMaxFile) {
            throw Failure(Status::payload_too_large, "maximum file size is 12 GB");
        }
        upload.upload_id = Poco::UUIDGenerator::defaultGenerator().createRandom().toString();
        upload.expires_at = core::NowIso8601WithOffsetSeconds(ttl_seconds);
        std::lock_guard lock(db->upload_mutex());
        RetryPendingRollback();
        RequireSpace(storage->temp_path(), upload.size_bytes * 2);
        auto created = db->CreateVerifiedUpload(kBucket, upload);
        if (!created.ok()) throw Failure(Status::conflict, "another upload is active");
        auto obj = Progress(created.value());
        obj.set("object", upload.display_name);
        obj.set("expires_at", upload.expires_at);
        return Json(req.version(), obj);
    }

    HttpResponse PutPart(const HttpRequest& req, const RouteParams& params) {
        int number = 0;
        const auto& text = params.at("part_number");
        auto [end, ec] = std::from_chars(text.data(), text.data() + text.size(), number);
        if (ec != std::errc{} || end != text.data() + text.size() || number < 1 || number > kMaxParts) {
            throw Failure(Status::bad_request, "invalid part number");
        }
        auto size = static_cast<std::uint64_t>(req.body().size());
        Poco::SHA2Engine256 hash;
        hash.update(req.body().data(), req.body().size());
        const auto digest = Poco::DigestEngine::digestToHex(hash.digest());
        std::lock_guard lock(db->upload_mutex());
        auto upload = Get(params.at("upload_id"));
        if (upload.state != "initiated" && upload.state != "uploading") {
            throw Failure(Status::conflict, "upload is closed for writes");
        }
        if (!size && (upload.size_bytes || number != 1)) {
            throw Failure(Status::bad_request, "unexpected empty part");
        }
        const auto [other_bytes, previous_hash] = db->InspectPartSlot(upload.upload_id, number);
        if (!previous_hash.empty() && previous_hash != digest) {
            throw Failure(Status::conflict, "part already acknowledged with different bytes");
        }
        if (other_bytes + size > upload.size_bytes) {
            throw Failure(Status::payload_too_large, "parts exceed declared size");
        }
        auto dir = Directory(upload.upload_id);
        fs::create_directories(dir);
        metadata::MultipartPart part;
        part.upload_id = upload.upload_id;
        part.part_number = number;
        part.size_bytes = size;
        part.etag = digest;
        part.temp_path = (dir / ("part-" + std::to_string(number) + "-" + digest)).string();
        // A prior attempt can have left an orphan after rename and before the database commit.
        // Only committed rows are acknowledgments.
        if (previous_hash.empty()) {
            RequireSpace(dir, size);
            auto incoming = dir / "incoming";
            try {
                DurableWriter out(incoming);
                out.Write(req.body().data(), req.body().size());
                out.Close();
                RenameUploadFile(incoming, part.temp_path);
                SyncDirectory(dir);
                SyncDirectory(dir.parent_path());
            } catch (...) {
                std::error_code ignore;
                fs::remove(incoming, ignore);
                throw;
            }
        }
        db->RecordVerifiedPart(part, core::NowIso8601WithOffsetSeconds(ttl_seconds));
        Object obj;
        obj.set("upload_id", upload.upload_id);
        obj.set("part_number", number);
        obj.set("size", static_cast<Poco::UInt64>(size));
        obj.set("etag", digest);
        return Json(req.version(), obj);
    }

    HttpResponse List(const HttpRequest& req, const std::string& id) {
        std::lock_guard lock(db->upload_mutex());
        auto upload = Get(id);
        auto obj = Progress(upload);
        Poco::JSON::Array::Ptr items = new Poco::JSON::Array;
        for (const auto& part : Parts(id)) {
            Object::Ptr entry = new Object;
            entry->set("part_number", part.part_number);
            entry->set("size", static_cast<Poco::UInt64>(part.size_bytes));
            entry->set("etag", part.etag);
            items->add(entry);
        }
        obj.set("parts", items);
        obj.set("object", upload.display_name);
        return Json(req.version(), obj);
    }

    auto Manifest(const std::string& json, const VerifiedUpload& upload) {
        auto stored = Parts(upload.upload_id);
        auto requested = Parse(json)->getArray("parts");
        if (!requested || stored.empty() || requested->size() != stored.size()) {
            throw Failure(Status::conflict, "completion requires every uploaded part");
        }
        std::uint64_t total = 0;
        for (std::size_t i = 0; i < stored.size(); ++i) {
            auto part = requested->getObject(i);
            if (!part || part->optValue<int>("part_number", 0) != stored[i].part_number ||
                part->optValue<std::string>("etag", "") != stored[i].etag) {
                throw Failure(Status::conflict, "completion part manifest mismatch");
            }
            total += stored[i].size_bytes;
        }
        if (total != upload.size_bytes) throw Failure(Status::conflict, "incomplete file");
        return stored;
    }

    // Only one active pilot upload is admitted. A dedicated worker keeps polling and health
    // checks responsive even on a single-threaded HTTP server.
    void Finish(const VerifiedUpload& upload, const std::vector<metadata::MultipartPart>& parts) {
        const auto start = std::chrono::steady_clock::now();
        const auto dir = Directory(upload.upload_id);
        const auto staging = dir / "assembly";
        const fs::path target = storage::LocalStorage::BuildObjectPath(storage->base_path(), kBucket, upload.upload_id);
        bool published_to_disk = false;
        try {
            RequireSpace(dir, upload.size_bytes);
            Assemble(staging, parts);
            VerifyFile(staging, upload);
            std::lock_guard lock(db->upload_mutex());
            RenameUploadFile(staging, target);
            published_to_disk = true;
            SyncDirectory(target.parent_path());
            SyncDirectory(dir);
            auto millis = std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::steady_clock::now() - start).count();
            db->CommitVerifiedUpload(upload, millis);
            std::error_code ignore;
            fs::remove_all(dir, ignore);
        } catch (const std::exception& ex) {
            core::LogError("Pilot finalization failed for " + upload.upload_id + ": " + ex.what());
            std::lock_guard lock(db->upload_mutex());
            std::error_code ignore;
            if (published_to_disk) fs::remove(target, ignore);
            fs::remove(staging, ignore);
            pending_rollback = upload.upload_id;
            RetryPendingRollback();
        }
    }
};

template <class Action>
auto Handle(Action action) {
    return [action = std::move(action)](const RequestContext& ctx, const HttpRequest& req,
                                        const RouteParams& params) {
        Status status;
        std::string message;
        try {
            return action(req, params);
        } catch (const Failure& err) {
            status = err.status;
            message = err.what();
        } catch (const std::exception& err) {
            core::LogError("Pilot request " + ctx.request_id + ": " + err.what());
            status = Status::internal_server_error;
            message = "storage operation failed";
        }
        Object::Ptr error = new Object;
        error->set("message", message);
        error->set("request_id", ctx.request_id);
        Object body;
        body.set("error", error);
        return Json(req.version(), body, status);
    };
}

}  // namespace

void RegisterVerifiedMultipart(Router& router, std::shared_ptr<metadata::SqliteMetadataStore> db,
                               std::shared_ptr<storage::StorageBackend> storage, int ttl_seconds) {
    auto service = std::make_shared<UploadService>(UploadService{db, storage, ttl_seconds, {}});
    auto pool = std::make_shared<boost::asio::thread_pool>(1);
    if (!db->GetBucket(kBucket).ok() && !db->CreateBucket(kBucket).ok()) {
        throw std::runtime_error("could not initialize pilot bucket");
    }
    if (!storage->EnsureBucket(kBucket).ok()) throw std::runtime_error("could not initialize pilot storage");
    const fs::path object_dir = fs::path(storage->base_path()) / "buckets" / kBucket / "objects";
    SyncDirectory(object_dir.parent_path());
    SyncDirectory(object_dir.parent_path().parent_path());
    SyncDirectory(storage->base_path());
    fs::create_directories(fs::path(storage->temp_path()) / "multipart");
    SyncDirectory(storage->temp_path());
    db->RecoverVerifiedUploads(storage->base_path(), storage->temp_path());

    const std::string base = "/v1/buckets/pilot/multipart-uploads";
    const auto transfer = base + "/{upload_id}";
    router.Add("POST", base, Handle([service](const auto& req, const auto&) {
        return service->Start(req);
    }));
    router.Add("PUT", transfer + "/parts/{part_number}", Handle([service](const auto& req, const auto& params) {
        return service->PutPart(req, params);
    }));
    router.Add("GET", transfer + "/parts", Handle([service](const auto& req, const auto& params) {
        return service->List(req, params.at("upload_id"));
    }));
    router.Add("GET", transfer + "/status", Handle([service](const auto& req, const auto& params) {
        std::lock_guard lock(service->db->upload_mutex());
        auto upload = service->Get(params.at("upload_id"));
        return Json(req.version(), Progress(upload));
    }));
    router.Add("POST", transfer + "/complete", Handle([service, pool](const auto& req, const auto& params) {
        std::lock_guard lock(service->db->upload_mutex());
        auto upload = service->Get(params.at("upload_id"));
        if (upload.state == "completed") return Json(req.version(), Progress(upload));
        if (upload.state != "finalizing") {
            auto parts = service->Manifest(req.body(), upload);
            service->db->SetVerifiedState(upload.upload_id, "finalizing", "");
            upload.state = "finalizing";
            upload.error.clear();
            // If scheduling fails, the next authenticated request can reopen the upload.
            service->pending_rollback = upload.upload_id;
            boost::asio::post(*pool, [service, upload, parts = std::move(parts)] {
                try {
                    service->Finish(upload, parts);
                } catch (const std::exception& ex) {
                    core::LogError("Failed to record finalization outcome for " + upload.upload_id + ": " + ex.what());
                }
            });
            service->pending_rollback.clear();
        }
        return Json(req.version(), Progress(upload), Status::accepted);
    }));
    router.Add("DELETE", transfer, Handle([service](const auto& req, const auto& params) {
        std::lock_guard lock(service->db->upload_mutex());
        auto upload = service->Get(params.at("upload_id"));
        if (upload.state == "completed" || upload.state == "finalizing") {
            throw Failure(Status::conflict, "cannot abort finalizing or completed upload");
        }
        fs::remove_all(service->Directory(upload.upload_id));
        auto removed = service->db->DeleteMultipartUpload(upload.upload_id);
        if (!removed.ok()) throw std::runtime_error("could not abort upload");
        return HttpResponse{Status::no_content, req.version()};
    }));
}

}  // namespace nebulafs::http
