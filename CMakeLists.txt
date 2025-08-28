cmake_minimum_required(VERSION 3.20)
project(FileServer
    VERSION 1.0.0
    DESCRIPTION "A high-performance personal file server built with C++ and Poco"
    LANGUAGES CXX)

# Set C++ standard
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# Build options
option(BUILD_TESTS "Build test suite" ON)
option(BUILD_EXAMPLES "Build example applications" ON)
option(ENABLE_COVERAGE "Enable code coverage" OFF)
option(ENABLE_LTO "Enable Link Time Optimization" OFF)
option(ENABLE_OPTIMIZATIONS "Enable additional optimizations" OFF)

# Set default build type
if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE "Release" CACHE STRING "Build type" FORCE)
endif()

# Compiler-specific options
if(CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -Wextra -Wpedantic")
    if(CMAKE_BUILD_TYPE STREQUAL "Debug")
        set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -g -O0 -DDEBUG")
    else()
        set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O3 -DNDEBUG")
    endif()
elseif(CMAKE_CXX_COMPILER_ID MATCHES "MSVC")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} /W4")
    if(CMAKE_BUILD_TYPE STREQUAL "Debug")
        set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} /Od /DDEBUG")
    else()
        set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} /O2 /DNDEBUG")
    endif()
endif()

# Enable LTO if requested
if(ENABLE_LTO)
    include(CheckIPOSupported)
    check_ipo_supported(RESULT supported OUTPUT error)
    if(supported)
        set(CMAKE_INTERPROCEDURAL_OPTIMIZATION TRUE)
        message(STATUS "Link Time Optimization enabled")
    else()
        message(WARNING "LTO not supported: ${error}")
    endif()
endif()

# Additional optimizations
if(ENABLE_OPTIMIZATIONS)
    if(CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
        set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -march=native -mtune=native")
    endif()
endif()

# Find packages
find_package(Poco REQUIRED COMPONENTS Foundation Util Net NetSSL Data DataSQLite JSON Crypto)
find_package(OpenSSL REQUIRED)

# Optional packages
if(BUILD_TESTS)
    find_package(GTest REQUIRED)
    enable_testing()
endif()

# Include directories
include_directories(
    ${CMAKE_SOURCE_DIR}/include
    ${CMAKE_SOURCE_DIR}/src
)

# Source files
set(CORE_SOURCES
    src/core/Application.cpp
    src/core/HTTPServer.cpp
    src/core/ConfigManager.cpp
    src/core/Logger.cpp
    src/core/DatabaseManager.cpp
)

set(SERVICE_SOURCES
    src/services/FileService.cpp
    src/services/UserService.cpp
    src/services/AuthManager.cpp
    src/services/SearchService.cpp
    src/services/SyncService.cpp
    src/services/MetricsService.cpp
)

set(HANDLER_SOURCES
    src/handlers/FileHandler.cpp
    src/handlers/AuthHandler.cpp
    src/handlers/UserHandler.cpp
    src/handlers/SearchHandler.cpp
    src/handlers/MetricsHandler.cpp
)

set(STORAGE_SOURCES
    src/storage/StorageBackend.cpp
    src/storage/LocalStorage.cpp
    src/storage/CloudStorage.cpp
    src/storage/CachedStorage.cpp
)

set(UTIL_SOURCES
    src/utils/FileUtils.cpp
    src/utils/StringUtils.cpp
    src/utils/CryptoUtils.cpp
    src/utils/TimeUtils.cpp
)

set(AUTH_SOURCES
    src/auth/JWTManager.cpp
    src/auth/PasswordManager.cpp
    src/auth/RoleManager.cpp
)

# All source files
set(ALL_SOURCES
    ${CORE_SOURCES}
    ${SERVICE_SOURCES}
    ${HANDLER_SOURCES}
    ${STORAGE_SOURCES}
    ${UTIL_SOURCES}
    ${AUTH_SOURCES}
    src/main.cpp
)

# Create main executable
add_executable(file_server ${ALL_SOURCES})

# Link libraries
target_link_libraries(file_server
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

# Set target properties
set_target_properties(file_server PROPERTIES
    OUTPUT_NAME "file_server"
    RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/bin"
)

# Create static library for testing
if(BUILD_TESTS)
    set(LIB_SOURCES ${ALL_SOURCES})
    list(REMOVE_ITEM LIB_SOURCES src/main.cpp)
    
    add_library(file_server_lib STATIC ${LIB_SOURCES})
    target_link_libraries(file_server_lib
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
    
    # Add test subdirectory
    add_subdirectory(tests)
endif()

# Add examples
if(BUILD_EXAMPLES)
    add_subdirectory(examples)
endif()

# Installation
install(TARGETS file_server
    RUNTIME DESTINATION bin
    LIBRARY DESTINATION lib
    ARCHIVE DESTINATION lib
)

install(DIRECTORY include/
    DESTINATION include
    FILES_MATCHING PATTERN "*.h" PATTERN "*.hpp"
)

install(DIRECTORY config/
    DESTINATION share/file_server/config
    FILES_MATCHING PATTERN "*.json"
)

install(DIRECTORY scripts/
    DESTINATION share/file_server/scripts
    FILES_MATCHING PATTERN "*.sh" PATTERN "*.bat"
    PERMISSIONS OWNER_EXECUTE OWNER_WRITE OWNER_READ
                GROUP_EXECUTE GROUP_READ
                WORLD_EXECUTE WORLD_READ
)

# CPack configuration
set(CPACK_PACKAGE_NAME "FileServer")
set(CPACK_PACKAGE_VERSION "${PROJECT_VERSION}")
set(CPACK_PACKAGE_DESCRIPTION_SUMMARY "Personal File Server")
set(CPACK_PACKAGE_VENDOR "Your Name")
set(CPACK_RESOURCE_FILE_LICENSE "${CMAKE_SOURCE_DIR}/LICENSE")
set(CPACK_RESOURCE_FILE_README "${CMAKE_SOURCE_DIR}/README.md")

include(CPack)

# Custom targets
add_custom_target(format
    COMMAND find ${CMAKE_SOURCE_DIR}/src ${CMAKE_SOURCE_DIR}/include ${CMAKE_SOURCE_DIR}/tests -name "*.cpp" -o -name "*.h" -o -name "*.hpp" | xargs clang-format -i
    COMMENT "Formatting source code"
)

add_custom_target(static-analysis
    COMMAND clang-tidy ${ALL_SOURCES} -p ${CMAKE_BINARY_DIR}
    COMMENT "Running static analysis"
)

# Code coverage
if(ENABLE_COVERAGE AND CMAKE_BUILD_TYPE STREQUAL "Debug")
    if(CMAKE_CXX_COMPILER_ID MATCHES "GNU")
        target_compile_options(file_server PRIVATE --coverage)
        target_link_libraries(file_server --coverage)
        
        if(TARGET file_server_lib)
            target_compile_options(file_server_lib PRIVATE --coverage)
            target_link_libraries(file_server_lib --coverage)
        endif()
        
        add_custom_target(coverage
            COMMAND lcov --capture --directory . --output-file coverage.info
            COMMAND genhtml coverage.info --output-directory coverage_report
            COMMENT "Generating code coverage report"
        )
    endif()
endif()

# Print configuration summary
message(STATUS "")
message(STATUS "FileServer Configuration Summary")
message(STATUS "================================")
message(STATUS "Version:              ${PROJECT_VERSION}")
message(STATUS "Build type:           ${CMAKE_BUILD_TYPE}")
message(STATUS "C++ standard:         C++${CMAKE_CXX_STANDARD}")
message(STATUS "Compiler:             ${CMAKE_CXX_COMPILER_ID}")
message(STATUS "Build tests:          ${BUILD_TESTS}")
message(STATUS "Build examples:       ${BUILD_EXAMPLES}")
message(STATUS "Enable coverage:      ${ENABLE_COVERAGE}")
message(STATUS "Enable LTO:           ${ENABLE_LTO}")
message(STATUS "Enable optimizations: ${ENABLE_OPTIMIZATIONS}")
message(STATUS "Install prefix:       ${CMAKE_INSTALL_PREFIX}")
message(STATUS "")