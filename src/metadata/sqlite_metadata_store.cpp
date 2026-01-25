#include "nebulafs/metadata/sqlite_metadata_store.h"

#include <Poco/Data/SessionFactory.h>
#include <Poco/Data/SQLite/Connector.h>
#include <Poco/Data/Statement.h>

#include "nebulafs/core/time.h"

namespace {
using namespace Poco::Data::Keywords;
}

namespace nebulafs::metadata {

SqliteMetadataStore::SqliteMetadataStore(const std::string& db_path)
    : session_([](const std::string& path) {
          Poco::Data::SQLite::Connector::registerConnector();
          return Poco::Data::Session("SQLite", path);
      }(db_path)) {
    InitSchema();
}

void SqliteMetadataStore::InitSchema() {
    // Schema is created on startup for developer convenience; migrations will replace this later.
    session_ <<
            "CREATE TABLE IF NOT EXISTS buckets ("
            "id INTEGER PRIMARY KEY AUTOINCREMENT,"
            "name TEXT NOT NULL UNIQUE,"
            "created_at TEXT NOT NULL"
            ")",
        now;

    session_ <<
            "CREATE TABLE IF NOT EXISTS objects ("
            "id INTEGER PRIMARY KEY AUTOINCREMENT,"
            "bucket_id INTEGER NOT NULL,"
            "name TEXT NOT NULL,"
            "size_bytes INTEGER NOT NULL,"
            "etag TEXT NOT NULL,"
            "created_at TEXT NOT NULL,"
            "updated_at TEXT NOT NULL,"
            "UNIQUE(bucket_id, name),"
            "FOREIGN KEY(bucket_id) REFERENCES buckets(id) ON DELETE CASCADE"
            ")",
        now;
}

core::Result<Bucket> SqliteMetadataStore::CreateBucket(const std::string& name) {
    try {
        std::string created_at = core::NowIso8601();
        std::string name_value = name;
        session_ << "INSERT INTO buckets(name, created_at) VALUES(?, ?)", use(name_value),
            use(created_at), now;
    } catch (const Poco::Exception& ex) {
        // SQLite uniqueness errors surface here; map them to a conflict-like error.
        return core::Error{core::ErrorCode::kAlreadyExists, ex.displayText()};
    }

    return GetBucket(name);
}

core::Result<std::vector<Bucket>> SqliteMetadataStore::ListBuckets() {
    std::vector<Bucket> buckets;
    Bucket bucket;

    Poco::Data::Statement select(session_);
    select << "SELECT id, name, created_at FROM buckets ORDER BY name ASC", into(bucket.id),
        into(bucket.name), into(bucket.created_at), range(0, 1);

    while (!select.done()) {
        bucket = {};
        select.execute();
        if (select.done() && bucket.name.empty()) {
            break;
        }
        if (!bucket.name.empty()) {
            buckets.push_back(bucket);
        }
    }

    return buckets;
}

core::Result<Bucket> SqliteMetadataStore::GetBucket(const std::string& name) {
    Bucket bucket;
    std::string name_value = name;
    Poco::Data::Statement select(session_);
    select << "SELECT id, name, created_at FROM buckets WHERE name = ?", use(name_value),
        into(bucket.id), into(bucket.name), into(bucket.created_at), now;

    if (bucket.name.empty()) {
        return core::Error{core::ErrorCode::kNotFound, "bucket not found"};
    }
    return bucket;
}

core::Result<ObjectMetadata> SqliteMetadataStore::UpsertObject(const std::string& bucket,
                                                               const ObjectMetadata& object) {
    auto bucket_result = GetBucket(bucket);
    if (!bucket_result.ok()) {
        return bucket_result.error();
    }
    int bucket_id = bucket_result.value().id;

    std::string now_time = core::NowIso8601();
    try {
        std::string name_value = object.name;
        std::string etag_value = object.etag;
        std::uint64_t size_value = object.size_bytes;
        session_ <<
                "INSERT INTO objects(bucket_id, name, size_bytes, etag, created_at, updated_at) "
                "VALUES(?, ?, ?, ?, ?, ?) "
                "ON CONFLICT(bucket_id, name) DO UPDATE SET "
                "size_bytes=excluded.size_bytes, etag=excluded.etag, updated_at=excluded.updated_at",
            use(bucket_id), use(name_value), use(size_value), use(etag_value), use(now_time),
            use(now_time), now;
    } catch (const Poco::Exception& ex) {
        return core::Error{core::ErrorCode::kDbError, ex.displayText()};
    }

    return GetObject(bucket, object.name);
}

core::Result<ObjectMetadata> SqliteMetadataStore::GetObject(const std::string& bucket,
                                                            const std::string& object) {
    ObjectMetadata meta;
    std::string bucket_value = bucket;
    std::string object_value = object;
    Poco::Data::Statement select(session_);
    select <<
            "SELECT o.id, o.bucket_id, o.name, o.size_bytes, o.etag, o.created_at, o.updated_at "
            "FROM objects o JOIN buckets b ON o.bucket_id = b.id "
            "WHERE b.name = ? AND o.name = ?",
        use(bucket_value), use(object_value), into(meta.id), into(meta.bucket_id), into(meta.name),
        into(meta.size_bytes), into(meta.etag), into(meta.created_at), into(meta.updated_at), now;

    if (meta.name.empty()) {
        return core::Error{core::ErrorCode::kNotFound, "object not found"};
    }
    return meta;
}

core::Result<std::vector<ObjectMetadata>> SqliteMetadataStore::ListObjects(
    const std::string& bucket, const std::string& prefix) {
    std::vector<ObjectMetadata> objects;
    ObjectMetadata meta;

    std::string like = prefix + "%";
    std::string bucket_value = bucket;
    Poco::Data::Statement select(session_);
    select <<
            "SELECT o.id, o.bucket_id, o.name, o.size_bytes, o.etag, o.created_at, o.updated_at "
            "FROM objects o JOIN buckets b ON o.bucket_id = b.id "
            "WHERE b.name = ? AND o.name LIKE ? ORDER BY o.name ASC",
        use(bucket_value), use(like), into(meta.id), into(meta.bucket_id), into(meta.name),
        into(meta.size_bytes), into(meta.etag), into(meta.created_at), into(meta.updated_at),
        range(0, 1);

    while (!select.done()) {
        meta = {};
        select.execute();
        if (select.done() && meta.name.empty()) {
            break;
        }
        if (!meta.name.empty()) {
            objects.push_back(meta);
        }
    }

    return objects;
}

core::Result<void> SqliteMetadataStore::DeleteObject(const std::string& bucket,
                                                     const std::string& object) {
    auto bucket_result = GetBucket(bucket);
    if (!bucket_result.ok()) {
        return bucket_result.error();
    }
    int bucket_id = bucket_result.value().id;

    Poco::Data::Statement del(session_);
    std::string object_value = object;
    del << "DELETE FROM objects WHERE bucket_id = ? AND name = ?", use(bucket_id),
        use(object_value),
        now;
    return core::Ok();
}

}  // namespace nebulafs::metadata
