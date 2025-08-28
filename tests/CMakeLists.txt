# Test configuration
enable_testing()

# Test directories
set(TEST_DATA_DIR "${CMAKE_CURRENT_SOURCE_DIR}/data")
set(TEST_OUTPUT_DIR "${CMAKE_CURRENT_BINARY_DIR}")

# Macro to create a test
macro(create_test TEST_NAME TEST_SOURCE)
    add_executable(${TEST_NAME} ${TEST_SOURCE})
    target_link_libraries(${TEST_NAME} 
        file_server_lib
        GTest::gtest
        GTest::gtest_main
        GTest::gmock
        GTest::gmock_main
        Poco::Foundation
        Poco::Util
        Poco::Net
        Poco::NetSSL
        Poco::Data
        Poco::DataSQLite
        Poco::JSON
        Poco::Crypto
        OpenSSL::SSL
        OpenSSL::Crypto
    )
    
    target_compile_definitions(${TEST_NAME} PRIVATE
        TEST_DATA_DIR="${TEST_DATA_DIR}"
        TEST_OUTPUT_DIR="${TEST_OUTPUT_DIR}"
    )
    
    add_test(NAME ${TEST_NAME} COMMAND ${TEST_NAME})
    set_target_properties(${TEST_NAME} PROPERTIES
        RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/tests"
    )
endmacro()

# Unit tests
create_test(test_config_manager unit/test_ConfigManager.cpp)
create_test(test_file_utils unit/test_FileUtils.cpp)
create_test(test_crypto_utils unit/test_CryptoUtils.cpp)
create_test(test_string_utils unit/test_StringUtils.cpp)
create_test(test_jwt_manager unit/test_JWTManager.cpp)
create_test(test_password_manager unit/test_PasswordManager.cpp)
create_test(test_local_storage unit/test_LocalStorage.cpp)
create_test(test_database_manager unit/test_DatabaseManager.cpp)

# Integration tests
create_test(test_http_server integration/test_HTTPServer.cpp)
create_test(test_file_service integration/test_FileService.cpp)
create_test(test_auth_service integration/test_AuthService.cpp)
create_test(test_search_service integration/test_SearchService.cpp)
create_test(test_api_endpoints integration/test_APIEndpoints.cpp)

# Performance tests
if(BUILD_BENCHMARKS)
    find_package(benchmark REQUIRED)
    
    add_executable(perf_file_operations performance/perf_FileOperations.cpp)
    target_link_libraries(perf_file_operations
        file_server_lib
        benchmark::benchmark
        benchmark::benchmark_main
    )
    
    add_executable(perf_database performance/perf_Database.cpp)
    target_link_libraries(perf_database
        file_server_lib
        benchmark::benchmark
        benchmark::benchmark_main
    )
endif()

# Create test data directory
file(MAKE_DIRECTORY ${TEST_DATA_DIR})

# Copy test configuration files
configure_file(
    "${CMAKE_SOURCE_DIR}/config/server.json"
    "${TEST_OUTPUT_DIR}/test_server.json"
    COPYONLY
)

# Custom target to run all tests
add_custom_target(run_tests
    COMMAND ${CMAKE_CTEST_COMMAND} --verbose
    DEPENDS 
        test_config_manager
        test_file_utils
        test_crypto_utils
        test_string_utils
        test_jwt_manager
        test_password_manager
        test_local_storage
        test_database_manager
        test_http_server
        test_file_service
        test_auth_service
        test_search_service
        test_api_endpoints
    COMMENT "Running all tests"
)