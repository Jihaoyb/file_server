#include "nebulafs/metadata/sqlite_metadata_store.h"

#include <chrono>
#include <filesystem>

#include <Poco/Data/Statement.h>
#include <Poco/Data/Transaction.h>

#include "nebulafs/core/time.h"
#include "nebulafs/storage/local_storage.h"

namespace nebulafs::metadata {
namespace {
using namespace Poco::Data::Keywords;

Poco::Data::Session OpenSession(const std::string& path) {
    Poco::Data::Session session("SQLite", path);
    session << "PRAGMA foreign_keys = ON", now;
    session << "PRAGMA busy_timeout = 5000", now;
    return session;
}
}  // namespace

core::Result<VerifiedUpload> SqliteMetadataStore::CreateVerifiedUpload(const std::string& bucket,
                                                                      VerifiedUpload upload) {
    auto db = OpenSession(db_path_);
    Poco::Data::Transaction tx(db);
    int active = 0;
    db << "SELECT COUNT(*) FROM multipart_uploads m JOIN verified_uploads v USING(upload_id) "
          "WHERE m.state IN ('initiated','uploading','finalizing')", into(active), now;
    if (active) {
        return core::Error{core::ErrorCode::kAlreadyExists, "another upload is active"};
    }
    auto timestamp = core::NowIso8601();
    std::string bucket_name = bucket;
    db << "INSERT INTO multipart_uploads(upload_id,bucket_id,object_name,state,expires_at,"
          "created_at,updated_at) VALUES(?,(SELECT id FROM buckets WHERE name=?),?,'initiated',?,?,?)",
        use(upload.upload_id), use(bucket_name), use(upload.upload_id), use(upload.expires_at),
        use(timestamp), use(timestamp), now;
    db << "INSERT INTO verified_uploads(upload_id,display_name,sha256,size_bytes) VALUES(?,?,?,?)",
        use(upload.upload_id), use(upload.display_name), use(upload.sha256), use(upload.size_bytes), now;
    tx.commit();
    upload.state = "initiated";
    return upload;
}

core::Result<VerifiedUpload> SqliteMetadataStore::GetVerifiedUpload(const std::string& bucket,
                                                                   const std::string& upload_id) {
    auto db = OpenSession(db_path_);
    auto id = upload_id;
    auto bucket_name = bucket;
    VerifiedUpload upload;
    db << "SELECT v.upload_id,v.display_name,v.sha256,v.size_bytes,m.state,m.expires_at,"
          "v.completed_at,v.finalization_ms,v.error FROM verified_uploads v "
          "JOIN multipart_uploads m USING(upload_id) JOIN buckets b ON m.bucket_id=b.id "
          "WHERE v.upload_id=? AND b.name=?", use(id), use(bucket_name), into(upload.upload_id),
        into(upload.display_name), into(upload.sha256), into(upload.size_bytes), into(upload.state),
        into(upload.expires_at), into(upload.completed_at), into(upload.finalization_ms),
        into(upload.error), now;
    if (upload.upload_id.empty()) {
        return core::Error{core::ErrorCode::kNotFound, "upload not found"};
    }
    return upload;
}

// Bytes already committed in other slots and the current slot's acknowledged hash.
std::pair<std::uint64_t, std::string> SqliteMetadataStore::InspectPartSlot(
    const std::string& upload_id, int number) {
    auto db = OpenSession(db_path_);
    auto id = upload_id;
    std::uint64_t other_bytes = 0;
    std::string etag;
    db << "SELECT COALESCE(SUM(CASE WHEN part_number<>? THEN size_bytes END),0), "
          "COALESCE(MAX(CASE WHEN part_number=? THEN etag END),'') FROM multipart_parts "
          "WHERE upload_id=?", use(number), use(number), use(id), into(other_bytes), into(etag), now;
    return {other_bytes, etag};
}

void SqliteMetadataStore::RecordVerifiedPart(const MultipartPart& part, const std::string& expires_at) {
    auto db = OpenSession(db_path_);
    Poco::Data::Transaction tx(db);
    auto row = part;
    auto expiry = expires_at;
    auto timestamp = core::NowIso8601();
    db << "INSERT INTO multipart_parts(upload_id,part_number,size_bytes,etag,temp_path,created_at) "
          "VALUES(?,?,?,?,?,?) ON CONFLICT(upload_id,part_number) DO UPDATE SET "
          "size_bytes=excluded.size_bytes, etag=excluded.etag, temp_path=excluded.temp_path",
        use(row.upload_id), use(row.part_number), use(row.size_bytes), use(row.etag), use(row.temp_path),
        use(timestamp), now;
    db << "UPDATE multipart_uploads SET state='uploading',expires_at=?,updated_at=? WHERE upload_id=?",
        use(expiry), use(timestamp), use(row.upload_id), now;
    tx.commit();
}

void SqliteMetadataStore::SetVerifiedState(const std::string& upload_id, const std::string& state,
                                           const std::string& error) {
    auto db = OpenSession(db_path_);
    Poco::Data::Transaction tx(db);
    auto id = upload_id;
    auto new_state = state;
    auto message = error;
    auto timestamp = core::NowIso8601();
    db << "UPDATE multipart_uploads SET state=?,updated_at=? WHERE upload_id=?", use(new_state),
        use(timestamp), use(id), now;
    db << "UPDATE verified_uploads SET error=? WHERE upload_id=?", use(message), use(id), now;
    tx.commit();
}

void SqliteMetadataStore::CommitVerifiedUpload(const VerifiedUpload& upload, std::int64_t elapsed_ms) {
    auto db = OpenSession(db_path_);
    Poco::Data::Transaction tx(db);
    auto row = upload;
    auto timestamp = core::NowIso8601();
    auto epoch = std::chrono::duration_cast<std::chrono::seconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();
    db << "INSERT INTO objects(bucket_id,name,size_bytes,etag,created_at,updated_at) "
          "SELECT m.bucket_id,m.upload_id,v.size_bytes,v.sha256,?,? FROM multipart_uploads m "
          "JOIN verified_uploads v USING(upload_id) WHERE m.upload_id=? AND m.state='finalizing'",
        use(timestamp), use(timestamp), use(row.upload_id), now;
    db << "UPDATE verified_uploads SET completed_at=?,finalization_ms=?,error='' WHERE upload_id=?",
        use(epoch), use(elapsed_ms), use(row.upload_id), now;
    db << "UPDATE multipart_uploads SET state='completed',updated_at=? WHERE upload_id=?",
        use(timestamp), use(row.upload_id), now;
    db << "DELETE FROM multipart_parts WHERE upload_id=?", use(row.upload_id), now;
    tx.commit();
}

void SqliteMetadataStore::RecoverVerifiedUploads(const std::string& base_path,
                                                 const std::string& temp_path) {
    auto db = OpenSession(db_path_);
    std::vector<std::string> interrupted;
    std::vector<std::string> completed;
    db << "SELECT v.upload_id FROM verified_uploads v JOIN multipart_uploads m USING(upload_id) "
          "WHERE m.state='finalizing'", into(interrupted), now;
    db << "SELECT upload_id FROM verified_uploads WHERE completed_at>0", into(completed), now;
    for (const auto& id : interrupted) {
        // Rename precedes the SQLite commit. A crash in between leaves a verified but uncommitted
        // output; keep the acknowledged parts and allow the sender to re-run finalization.
        std::filesystem::remove(storage::LocalStorage::BuildObjectPath(base_path, "pilot", id));
        std::filesystem::remove(std::filesystem::path(temp_path) / "multipart" / id / "assembly");
        SetVerifiedState(id, "uploading", "completion interrupted; retry finalization");
    }
    for (const auto& id : completed) {
        std::error_code ec;
        std::filesystem::remove_all(std::filesystem::path(temp_path) / "multipart" / id, ec);
    }
}

std::vector<std::string> SqliteMetadataStore::ListExpiredVerifiedCompletions(std::int64_t cutoff,
                                                                           int limit) {
    auto db = OpenSession(db_path_);
    std::vector<std::string> uploads;
    db << "SELECT v.upload_id FROM verified_uploads v JOIN multipart_uploads m USING(upload_id) "
          "WHERE m.state='completed' AND v.completed_at<=? ORDER BY v.completed_at LIMIT ?",
        use(cutoff), use(limit), into(uploads), now;
    return uploads;
}

void SqliteMetadataStore::DeleteExpiredVerifiedCompletion(const std::string& upload_id) {
    auto db = OpenSession(db_path_);
    Poco::Data::Transaction tx(db);
    auto id = upload_id;
    db << "DELETE FROM objects WHERE name=? AND bucket_id=(SELECT bucket_id FROM "
          "multipart_uploads WHERE upload_id=? AND state='completed')", use(id), use(id), now;
    db << "DELETE FROM multipart_uploads WHERE upload_id=? AND state='completed'", use(id), now;
    tx.commit();
}

}  // namespace nebulafs::metadata
