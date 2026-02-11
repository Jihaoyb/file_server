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
