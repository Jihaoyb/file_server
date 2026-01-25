#include "nebulafs/storage/local_storage.h"

#include <array>
#include <cctype>
#include <filesystem>
#include <fstream>

#include <Poco/DigestEngine.h>
#include <Poco/SHA2Engine.h>
#include <Poco/UUIDGenerator.h>

#ifdef _WIN32
#include <windows.h>
#else
#include <fcntl.h>
#include <unistd.h>
#endif

namespace nebulafs::storage {

LocalStorage::LocalStorage(std::string base_path, std::string temp_path)
    : base_path_(std::move(base_path)), temp_path_(std::move(temp_path)) {
    std::filesystem::create_directories(base_path_);
    std::filesystem::create_directories(temp_path_);
}

core::Result<void> LocalStorage::EnsureBucket(const std::string& bucket) {
    if (!IsSafeName(bucket)) {
        return core::Error{core::ErrorCode::kInvalidArgument, "invalid bucket name"};
    }
    std::filesystem::path bucket_path = std::filesystem::path(base_path_) / "buckets" / bucket;
    // Ensure bucket and object directories exist before any writes.
    std::filesystem::create_directories(bucket_path / "objects");
    return core::Ok();
}

core::Result<StoredObject> LocalStorage::WriteObject(const std::string& bucket,
                                                     const std::string& object,
                                                     std::istream& data) {
    if (!IsSafeName(bucket) || !IsSafeName(object)) {
        return core::Error{core::ErrorCode::kInvalidArgument, "invalid object path"};
    }

    auto ensure = EnsureBucket(bucket);
    if (!ensure.ok()) {
        return ensure.error();
    }

    const auto final_path = BuildObjectPath(base_path_, bucket, object);
    // Write to a temp file first, then atomically rename into place.
    const auto temp_name = Poco::UUIDGenerator().createOne().toString();
    const auto temp_path = (std::filesystem::path(temp_path_) / temp_name).string();

    Poco::SHA2Engine256 sha256;
    std::uint64_t total = 0;

#ifdef _WIN32
    std::ofstream out(temp_path, std::ios::binary | std::ios::trunc);
    if (!out.is_open()) {
        return core::Error{core::ErrorCode::kIoError, "failed to open temp file"};
    }
    std::array<char, 8192> buffer{};
    while (data) {
        data.read(buffer.data(), buffer.size());
        const std::streamsize bytes = data.gcount();
        if (bytes <= 0) {
            break;
        }
        out.write(buffer.data(), bytes);
        sha256.update(buffer.data(), static_cast<unsigned int>(bytes));
        total += static_cast<std::uint64_t>(bytes);
    }
    out.flush();
#else
    const int fd = ::open(temp_path.c_str(), O_CREAT | O_WRONLY | O_TRUNC, 0644);
    if (fd < 0) {
        return core::Error{core::ErrorCode::kIoError, "failed to open temp file"};
    }
    std::array<char, 8192> buffer{};
    while (data) {
        data.read(buffer.data(), buffer.size());
        const std::streamsize bytes = data.gcount();
        if (bytes <= 0) {
            break;
        }
        ssize_t written = ::write(fd, buffer.data(), static_cast<size_t>(bytes));
        if (written < 0) {
            ::close(fd);
            return core::Error{core::ErrorCode::kIoError, "failed to write temp file"};
        }
        sha256.update(buffer.data(), static_cast<unsigned int>(bytes));
        total += static_cast<std::uint64_t>(bytes);
    }
    ::fsync(fd);
    ::close(fd);
#endif

    std::filesystem::create_directories(std::filesystem::path(final_path).parent_path());
    std::filesystem::rename(temp_path, final_path);

    StoredObject stored;
    stored.path = final_path;
    stored.size_bytes = total;
    stored.etag = Poco::DigestEngine::digestToHex(sha256.digest());
    return stored;
}

core::Result<StoredObject> LocalStorage::ReadObject(const std::string& bucket,
                                                    const std::string& object) const {
    if (!IsSafeName(bucket) || !IsSafeName(object)) {
        return core::Error{core::ErrorCode::kInvalidArgument, "invalid object path"};
    }
    const auto path = BuildObjectPath(base_path_, bucket, object);
    if (!std::filesystem::exists(path)) {
        return core::Error{core::ErrorCode::kNotFound, "object not found"};
    }
    StoredObject stored;
    stored.path = path;
    stored.size_bytes = static_cast<std::uint64_t>(std::filesystem::file_size(path));
    return stored;
}

core::Result<void> LocalStorage::DeleteObject(const std::string& bucket,
                                              const std::string& object) {
    if (!IsSafeName(bucket) || !IsSafeName(object)) {
        return core::Error{core::ErrorCode::kInvalidArgument, "invalid object path"};
    }
    const auto path = BuildObjectPath(base_path_, bucket, object);
    if (!std::filesystem::exists(path)) {
        return core::Error{core::ErrorCode::kNotFound, "object not found"};
    }
    std::filesystem::remove(path);
    return core::Ok();
}

bool LocalStorage::IsSafeName(const std::string& name) {
    if (name.empty() || name.size() > 255) {
        return false;
    }
    for (char c : name) {
        if (!(std::isalnum(static_cast<unsigned char>(c)) || c == '-' || c == '_' || c == '.')) {
            return false;
        }
    }
    if (name == "." || name == "..") {
        return false;
    }
    return true;
}

std::string LocalStorage::BuildObjectPath(const std::string& base_path, const std::string& bucket,
                                          const std::string& object) {
    return (std::filesystem::path(base_path) / "buckets" / bucket / "objects" / object).string();
}

}  // namespace nebulafs::storage
