#include "nebulafs/metadata/sqlite_metadata_store.h"

#include <Poco/Data/SessionFactory.h>
#include <Poco/Data/SQLite/Connector.h>
#include <Poco/Data/Statement.h>
#include <Poco/UUIDGenerator.h>

#include "nebulafs/core/time.h"
#include "nebulafs/distributed/placement_token.h"
#include "nebulafs/storage/local_storage.h"

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
    session_ << "PRAGMA foreign_keys = ON", now;

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

    session_ <<
            "CREATE TABLE IF NOT EXISTS multipart_uploads ("
            "id INTEGER PRIMARY KEY AUTOINCREMENT,"
            "upload_id TEXT NOT NULL UNIQUE,"
            "bucket_id INTEGER NOT NULL,"
            "object_name TEXT NOT NULL,"
            "state TEXT NOT NULL,"
            "expires_at TEXT NOT NULL,"
            "created_at TEXT NOT NULL,"
            "updated_at TEXT NOT NULL,"
            "FOREIGN KEY(bucket_id) REFERENCES buckets(id) ON DELETE CASCADE"
            ")",
        now;

    session_ <<
            "CREATE TABLE IF NOT EXISTS multipart_parts ("
            "id INTEGER PRIMARY KEY AUTOINCREMENT,"
            "upload_id TEXT NOT NULL,"
            "part_number INTEGER NOT NULL,"
            "size_bytes INTEGER NOT NULL,"
            "etag TEXT NOT NULL,"
            "temp_path TEXT NOT NULL,"
            "created_at TEXT NOT NULL,"
            "UNIQUE(upload_id, part_number),"
            "FOREIGN KEY(upload_id) REFERENCES multipart_uploads(upload_id) ON DELETE CASCADE"
            ")",
        now;

    session_ <<
            "CREATE INDEX IF NOT EXISTS idx_multipart_uploads_expires_at "
            "ON multipart_uploads(expires_at)",
        now;
    session_ <<
            "CREATE INDEX IF NOT EXISTS idx_multipart_parts_upload_id "
            "ON multipart_parts(upload_id)",
        now;

    session_ <<
            "CREATE TABLE IF NOT EXISTS storage_nodes ("
            "id INTEGER PRIMARY KEY AUTOINCREMENT,"
            "endpoint TEXT NOT NULL UNIQUE,"
            "status TEXT NOT NULL,"
            "capacity_bytes INTEGER NOT NULL DEFAULT 0,"
            "free_bytes INTEGER NOT NULL DEFAULT 0,"
            "updated_at TEXT NOT NULL"
            ")",
        now;

    session_ <<
            "CREATE TABLE IF NOT EXISTS object_replicas ("
            "id INTEGER PRIMARY KEY AUTOINCREMENT,"
            "object_id INTEGER NOT NULL,"
            "node_id INTEGER NOT NULL,"
            "blob_id TEXT NOT NULL,"
            "replica_index INTEGER NOT NULL,"
            "state TEXT NOT NULL,"
            "checksum TEXT NOT NULL,"
            "updated_at TEXT NOT NULL,"
            "UNIQUE(object_id, replica_index),"
            "FOREIGN KEY(object_id) REFERENCES objects(id) ON DELETE CASCADE,"
            "FOREIGN KEY(node_id) REFERENCES storage_nodes(id) ON DELETE CASCADE"
            ")",
        now;
    session_ <<
            "CREATE INDEX IF NOT EXISTS idx_object_replicas_object_id "
            "ON object_replicas(object_id)",
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

core::Result<MultipartUpload> SqliteMetadataStore::CreateMultipartUpload(
    const std::string& bucket, const std::string& upload_id, const std::string& object_name,
    const std::string& expires_at) {
    auto bucket_result = GetBucket(bucket);
    if (!bucket_result.ok()) {
        return bucket_result.error();
    }
    int bucket_id = bucket_result.value().id;

    std::string now_time = core::NowIso8601();
    try {
        std::string upload_id_value = upload_id;
        std::string object_name_value = object_name;
        std::string state_value = "initiated";
        std::string expires_at_value = expires_at;
        session_ <<
                "INSERT INTO multipart_uploads(upload_id, bucket_id, object_name, state, "
                "expires_at, created_at, updated_at) VALUES(?, ?, ?, ?, ?, ?, ?)",
            use(upload_id_value), use(bucket_id), use(object_name_value), use(state_value),
            use(expires_at_value), use(now_time), use(now_time), now;
    } catch (const Poco::Exception& ex) {
        return core::Error{core::ErrorCode::kAlreadyExists, ex.displayText()};
    }

    return GetMultipartUpload(upload_id);
}

core::Result<MultipartUpload> SqliteMetadataStore::GetMultipartUpload(const std::string& upload_id) {
    MultipartUpload upload;
    std::string upload_id_value = upload_id;
    Poco::Data::Statement select(session_);
    select <<
            "SELECT id, upload_id, bucket_id, object_name, state, expires_at, created_at, "
            "updated_at FROM multipart_uploads WHERE upload_id = ?",
        use(upload_id_value), into(upload.id), into(upload.upload_id), into(upload.bucket_id),
        into(upload.object_name), into(upload.state), into(upload.expires_at),
        into(upload.created_at), into(upload.updated_at), now;

    if (upload.upload_id.empty()) {
        return core::Error{core::ErrorCode::kNotFound, "multipart upload not found"};
    }
    return upload;
}

core::Result<std::vector<MultipartUpload>> SqliteMetadataStore::ListExpiredMultipartUploads(
    const std::string& expires_before, int limit) {
    std::vector<MultipartUpload> uploads;
    MultipartUpload upload;

    std::string cutoff_value = expires_before;
    int limit_value = limit;
    Poco::Data::Statement select(session_);
    select <<
            "SELECT id, upload_id, bucket_id, object_name, state, expires_at, created_at, "
            "updated_at FROM multipart_uploads "
            "WHERE state IN ('initiated', 'uploading') AND expires_at < ? "
            "ORDER BY expires_at ASC LIMIT ?",
        use(cutoff_value), use(limit_value), into(upload.id), into(upload.upload_id),
        into(upload.bucket_id), into(upload.object_name), into(upload.state),
        into(upload.expires_at), into(upload.created_at), into(upload.updated_at), range(0, 1);

    while (!select.done()) {
        upload = {};
        select.execute();
        if (select.done() && upload.upload_id.empty()) {
            break;
        }
        if (!upload.upload_id.empty()) {
            uploads.push_back(upload);
        }
    }

    return uploads;
}

core::Result<void> SqliteMetadataStore::UpdateMultipartUploadState(const std::string& upload_id,
                                                                   const std::string& state) {
    auto upload = GetMultipartUpload(upload_id);
    if (!upload.ok()) {
        return upload.error();
    }

    std::string now_time = core::NowIso8601();
    std::string upload_id_value = upload_id;
    std::string state_value = state;
    Poco::Data::Statement update(session_);
    update << "UPDATE multipart_uploads SET state = ?, updated_at = ? WHERE upload_id = ?",
        use(state_value), use(now_time), use(upload_id_value), now;
    return core::Ok();
}

core::Result<void> SqliteMetadataStore::DeleteMultipartUpload(const std::string& upload_id) {
    std::string upload_id_value = upload_id;
    Poco::Data::Statement del(session_);
    del << "DELETE FROM multipart_uploads WHERE upload_id = ?", use(upload_id_value), now;
    return core::Ok();
}

core::Result<MultipartPart> SqliteMetadataStore::UpsertMultipartPart(
    const std::string& upload_id, int part_number, std::uint64_t size_bytes, const std::string& etag,
    const std::string& temp_path) {
    auto upload = GetMultipartUpload(upload_id);
    if (!upload.ok()) {
        return upload.error();
    }

    std::string now_time = core::NowIso8601();
    try {
        std::string upload_id_value = upload_id;
        int part_number_value = part_number;
        std::uint64_t size_bytes_value = size_bytes;
        std::string etag_value = etag;
        std::string temp_path_value = temp_path;
        session_ <<
                "INSERT INTO multipart_parts(upload_id, part_number, size_bytes, etag, temp_path, "
                "created_at) VALUES(?, ?, ?, ?, ?, ?) "
                "ON CONFLICT(upload_id, part_number) DO UPDATE SET "
                "size_bytes=excluded.size_bytes, etag=excluded.etag, temp_path=excluded.temp_path",
            use(upload_id_value), use(part_number_value), use(size_bytes_value), use(etag_value),
            use(temp_path_value), use(now_time), now;
    } catch (const Poco::Exception& ex) {
        return core::Error{core::ErrorCode::kDbError, ex.displayText()};
    }

    std::string upload_id_value = upload_id;
    int part_number_value = part_number;
    MultipartPart part;
    Poco::Data::Statement select(session_);
    select <<
            "SELECT id, upload_id, part_number, size_bytes, etag, temp_path, created_at "
            "FROM multipart_parts WHERE upload_id = ? AND part_number = ?",
        use(upload_id_value), use(part_number_value), into(part.id), into(part.upload_id),
        into(part.part_number), into(part.size_bytes), into(part.etag), into(part.temp_path),
        into(part.created_at), now;
    return part;
}

core::Result<std::vector<MultipartPart>> SqliteMetadataStore::ListMultipartParts(
    const std::string& upload_id) {
    std::vector<MultipartPart> parts;
    MultipartPart part;

    std::string upload_id_value = upload_id;
    Poco::Data::Statement select(session_);
    select <<
            "SELECT id, upload_id, part_number, size_bytes, etag, temp_path, created_at "
            "FROM multipart_parts WHERE upload_id = ? ORDER BY part_number ASC",
        use(upload_id_value), into(part.id), into(part.upload_id), into(part.part_number),
        into(part.size_bytes), into(part.etag), into(part.temp_path), into(part.created_at),
        range(0, 1);

    while (!select.done()) {
        part = {};
        select.execute();
        if (select.done() && part.upload_id.empty()) {
            break;
        }
        if (!part.upload_id.empty()) {
            parts.push_back(part);
        }
    }

    return parts;
}

core::Result<void> SqliteMetadataStore::DeleteMultipartParts(const std::string& upload_id) {
    std::string upload_id_value = upload_id;
    Poco::Data::Statement del(session_);
    del << "DELETE FROM multipart_parts WHERE upload_id = ?", use(upload_id_value), now;
    return core::Ok();
}

core::Result<void> SqliteMetadataStore::ConfigureStorageNodes(
    const std::vector<std::string>& endpoints) {
    try {
        session_ << "DELETE FROM storage_nodes", now;
        const auto now_time = core::NowIso8601();
        for (const auto& endpoint : endpoints) {
            std::string endpoint_value = endpoint;
            std::string status_value = "active";
            std::string updated_at_value = now_time;
            session_ <<
                    "INSERT INTO storage_nodes(endpoint, status, updated_at) VALUES(?, ?, ?)",
                use(endpoint_value), use(status_value), use(updated_at_value), now;
        }
        return core::Ok();
    } catch (const Poco::Exception& ex) {
        return core::Error{core::ErrorCode::kDbError, ex.displayText()};
    }
}

core::Result<AllocateWritePlan> SqliteMetadataStore::AllocateWrite(
    const std::string& bucket, const std::string& object_name, int replication_factor,
    const std::string& service_token) {
    auto bucket_result = GetBucket(bucket);
    if (!bucket_result.ok()) {
        return bucket_result.error();
    }
    if (!storage::LocalStorage::IsSafeName(object_name)) {
        return core::Error{core::ErrorCode::kInvalidArgument, "invalid object name"};
    }

    AllocateWritePlan plan;
    plan.blob_id = Poco::UUIDGenerator().createOne().toString();
    plan.write_token =
        nebulafs::distributed::CreatePlacementToken(plan.blob_id, "write", 120, service_token);

    int id = 0;
    std::string endpoint;
    Poco::Data::Statement select(session_);
    select << "SELECT id, endpoint FROM storage_nodes WHERE status = 'active' ORDER BY id ASC",
        into(id), into(endpoint), range(0, 1);

    int index = 0;
    while (!select.done() && index < replication_factor) {
        id = 0;
        endpoint.clear();
        select.execute();
        if (!endpoint.empty()) {
            plan.replicas.push_back(ReplicaTarget{id, index, endpoint});
            ++index;
        }
    }
    if (static_cast<int>(plan.replicas.size()) < replication_factor) {
        return core::Error{core::ErrorCode::kInternal, "insufficient active storage nodes"};
    }
    return plan;
}

core::Result<void> SqliteMetadataStore::CommitWrite(const std::string& bucket,
                                                    const std::string& object_name,
                                                    const std::string& blob_id,
                                                    std::uint64_t size_bytes,
                                                    const std::string& etag,
                                                    const std::vector<ReplicaTarget>& replicas) {
    nebulafs::metadata::ObjectMetadata object;
    object.name = object_name;
    object.size_bytes = size_bytes;
    object.etag = etag;
    auto upsert = UpsertObject(bucket, object);
    if (!upsert.ok()) {
        return upsert.error();
    }

    std::string now_time = core::NowIso8601();
    int object_id = upsert.value().id;
    try {
        session_ << "DELETE FROM object_replicas WHERE object_id = ?", use(object_id), now;
        for (const auto& replica : replicas) {
            int node_id_value = replica.node_id;
            int replica_index_value = replica.replica_index;
            std::string blob_id_value = blob_id;
            std::string state_value = "committed";
            std::string checksum_value = etag;
            std::string updated_at_value = now_time;
            session_ <<
                    "INSERT INTO object_replicas(object_id, node_id, blob_id, replica_index, "
                    "state, checksum, updated_at) VALUES(?, ?, ?, ?, ?, ?, ?)",
                use(object_id), use(node_id_value), use(blob_id_value), use(replica_index_value),
                use(state_value), use(checksum_value), use(updated_at_value), now;
        }
        return core::Ok();
    } catch (const Poco::Exception& ex) {
        return core::Error{core::ErrorCode::kDbError, ex.displayText()};
    }
}

core::Result<ResolveReadPlan> SqliteMetadataStore::ResolveRead(const std::string& bucket,
                                                               const std::string& object_name) {
    auto object = GetObject(bucket, object_name);
    if (!object.ok()) {
        return object.error();
    }

    ResolveReadPlan plan;
    plan.size_bytes = object.value().size_bytes;
    plan.etag = object.value().etag;

    int node_id = 0;
    int replica_index = 0;
    std::string endpoint;
    std::string blob_id;
    Poco::Data::Statement select(session_);
    select <<
            "SELECT r.node_id, r.replica_index, s.endpoint, r.blob_id "
            "FROM object_replicas r JOIN storage_nodes s ON r.node_id = s.id "
            "WHERE r.object_id = ? AND r.state = 'committed' "
            "ORDER BY r.replica_index ASC",
        use(object.value().id), into(node_id), into(replica_index), into(endpoint), into(blob_id),
        range(0, 1);

    while (!select.done()) {
        node_id = 0;
        replica_index = 0;
        endpoint.clear();
        blob_id.clear();
        select.execute();
        if (!endpoint.empty() && !blob_id.empty()) {
            if (plan.blob_id.empty()) {
                plan.blob_id = blob_id;
            }
            plan.replicas.push_back(ReplicaTarget{node_id, replica_index, endpoint});
        }
    }
    if (plan.replicas.empty()) {
        return core::Error{core::ErrorCode::kNotFound, "object has no committed replicas"};
    }
    return plan;
}

}  // namespace nebulafs::metadata
