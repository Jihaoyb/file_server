#include <gtest/gtest.h>

#include "nebulafs/storage/local_storage.h"

TEST(PathSafety, AcceptsSimpleNames) {
    EXPECT_TRUE(nebulafs::storage::LocalStorage::IsSafeName("bucket1"));
    EXPECT_TRUE(nebulafs::storage::LocalStorage::IsSafeName("obj-1.txt"));
}

TEST(PathSafety, RejectsTraversal) {
    EXPECT_FALSE(nebulafs::storage::LocalStorage::IsSafeName("../secret"));
    EXPECT_FALSE(nebulafs::storage::LocalStorage::IsSafeName(".."));
    EXPECT_FALSE(nebulafs::storage::LocalStorage::IsSafeName("a/b"));
}
