cmake_minimum_required(VERSION 3.20)
project(NebulaFS LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

option(NEBULAFS_ENABLE_TESTS "Build tests" ON)

find_package(Boost REQUIRED COMPONENTS system)
find_package(OpenSSL REQUIRED)
find_package(Poco REQUIRED COMPONENTS Foundation Util JSON Crypto Data DataSQLite Net NetSSL)

add_library(nebulafs_core
    src/core/config.cpp
    src/core/logger.cpp
    src/core/result.cpp
    src/core/ids.cpp
    src/core/time.cpp
    src/metadata/sqlite_metadata_store.cpp
    src/storage/local_storage.cpp
    src/observability/metrics.cpp
    src/http/router.cpp
    src/http/http_server.cpp
)

target_include_directories(nebulafs_core
    PUBLIC
        include
)

target_link_libraries(nebulafs_core
    PUBLIC
        Boost::system
        OpenSSL::SSL
        OpenSSL::Crypto
        Poco::Foundation
        Poco::Util
        Poco::JSON
        Poco::Crypto
        Poco::Data
        Poco::DataSQLite
        Poco::Net
        Poco::NetSSL
)

target_compile_definitions(nebulafs_core PUBLIC POCO_STATIC)

if(MSVC)
    target_compile_options(nebulafs_core PRIVATE /W4 /permissive-)
else()
    target_compile_options(nebulafs_core PRIVATE -Wall -Wextra -Wpedantic)
endif()

add_executable(nebulafs
    src/main.cpp
)

target_link_libraries(nebulafs PRIVATE nebulafs_core)

if(NEBULAFS_ENABLE_TESTS)
    enable_testing()
    find_package(GTest REQUIRED)

    add_executable(nebulafs_unit_tests
        tests/unit/test_path_safety.cpp
        tests/unit/test_metadata_store.cpp
    )
    target_link_libraries(nebulafs_unit_tests PRIVATE nebulafs_core GTest::gtest GTest::gtest_main)
    add_test(NAME nebulafs_unit_tests COMMAND nebulafs_unit_tests)

    add_executable(nebulafs_integration_tests
        tests/integration/test_http_smoke.cpp
    )
    target_link_libraries(nebulafs_integration_tests PRIVATE nebulafs_core GTest::gtest GTest::gtest_main)
    target_compile_definitions(
        nebulafs_integration_tests
        PRIVATE
            NEBULAFS_SERVER_PATH="$<TARGET_FILE:nebulafs>"
    )
    add_test(NAME nebulafs_integration_tests COMMAND nebulafs_integration_tests)
endif()
