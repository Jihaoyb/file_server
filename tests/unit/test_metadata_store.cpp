#include <filesystem>

#include <gtest/gtest.h>
#include <Poco/UUIDGenerator.h>

#include "nebulafs/metadata/sqlite_metadata_store.h"

namespace {

std::filesystem::path MakeTempDbPath() {
    const auto name = "nebulafs_test_" + Poco::UUIDGenerator().createOne().toString() + ".db";
    return std::filesystem::temp_directory_path() / name;
}

}  // namespace

TEST(MetadataStore, CreateAndFetchBucket) {
    const auto db_path = MakeTempDbPath();

    {
        nebulafs::metadata::SqliteMetadataStore store(db_path.string());
        auto created = store.CreateBucket("alpha");
        ASSERT_TRUE(created.ok());

        auto fetched = store.GetBucket("alpha");
        ASSERT_TRUE(fetched.ok());
        EXPECT_EQ(fetched.value().name, "alpha");
    }

    std::filesystem::remove(db_path);
}

TEST(MetadataStore, UpsertObject) {
    const auto db_path = MakeTempDbPath();

    {
        nebulafs::metadata::SqliteMetadataStore store(db_path.string());
        store.CreateBucket("beta");

        nebulafs::metadata::ObjectMetadata meta;
        meta.name = "file.txt";
        meta.size_bytes = 42;
        meta.etag = "etag";

        auto upsert = store.UpsertObject("beta", meta);
        ASSERT_TRUE(upsert.ok());

        auto fetched = store.GetObject("beta", "file.txt");
        ASSERT_TRUE(fetched.ok());
        EXPECT_EQ(fetched.value().etag, "etag");
    }

    std::filesystem::remove(db_path);
}

TEST(MetadataStore, MultipartUploadLifecycle) {
    const auto db_path = MakeTempDbPath();

    {
        nebulafs::metadata::SqliteMetadataStore store(db_path.string());
        auto bucket = store.CreateBucket("multi");
        ASSERT_TRUE(bucket.ok());

        const std::string upload_id = "upload-123";
        auto created = store.CreateMultipartUpload("multi", upload_id, "big.bin",
                                                   "2099-01-01T00:00:00Z");
        ASSERT_TRUE(created.ok());
        EXPECT_EQ(created.value().state, "initiated");
        EXPECT_EQ(created.value().object_name, "big.bin");

        auto part1 = store.UpsertMultipartPart(upload_id, 1, 5, "etag-1", "/tmp/part1");
        ASSERT_TRUE(part1.ok());
        auto part2 = store.UpsertMultipartPart(upload_id, 2, 7, "etag-2", "/tmp/part2");
        ASSERT_TRUE(part2.ok());

        auto listed = store.ListMultipartParts(upload_id);
        ASSERT_TRUE(listed.ok());
        ASSERT_EQ(listed.value().size(), 2);
        EXPECT_EQ(listed.value()[0].part_number, 1);
        EXPECT_EQ(listed.value()[1].part_number, 2);

        auto updated = store.UpdateMultipartUploadState(upload_id, "uploading");
        ASSERT_TRUE(updated.ok());
        auto fetched = store.GetMultipartUpload(upload_id);
        ASSERT_TRUE(fetched.ok());
        EXPECT_EQ(fetched.value().state, "uploading");

        auto delete_parts = store.DeleteMultipartParts(upload_id);
        ASSERT_TRUE(delete_parts.ok());
        auto listed_after_delete = store.ListMultipartParts(upload_id);
        ASSERT_TRUE(listed_after_delete.ok());
        EXPECT_TRUE(listed_after_delete.value().empty());

        auto delete_upload = store.DeleteMultipartUpload(upload_id);
        ASSERT_TRUE(delete_upload.ok());
        auto missing = store.GetMultipartUpload(upload_id);
        ASSERT_FALSE(missing.ok());
    }

    std::filesystem::remove(db_path);
}
