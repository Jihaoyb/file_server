#include <filesystem>

#include <gtest/gtest.h>

#include "nebulafs/metadata/sqlite_metadata_store.h"

TEST(MetadataStore, CreateAndFetchBucket) {
    auto db_path = std::filesystem::temp_directory_path() / "nebulafs_test.db";
    std::filesystem::remove(db_path);

    nebulafs::metadata::SqliteMetadataStore store(db_path.string());
    auto created = store.CreateBucket("alpha");
    ASSERT_TRUE(created.ok());

    auto fetched = store.GetBucket("alpha");
    ASSERT_TRUE(fetched.ok());
    EXPECT_EQ(fetched.value().name, "alpha");

    std::filesystem::remove(db_path);
}

TEST(MetadataStore, UpsertObject) {
    auto db_path = std::filesystem::temp_directory_path() / "nebulafs_test2.db";
    std::filesystem::remove(db_path);

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

    std::filesystem::remove(db_path);
}
