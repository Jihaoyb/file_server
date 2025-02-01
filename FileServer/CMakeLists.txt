cmake_minimum_required(VERSION 3.14)
cmake_policy(SET CMP0167 NEW)
project(FileServer VERSION 1.0 LANGUAGES CXX)

# Set C++ standard
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF) # Ensure only standard-compliant features are used

# Integrate vcpkg toolchain
set(CMAKE_TOOLCHAIN_FILE "C:/Users/yejih/vcpkg/scripts/buildsystems/vcpkg.cmake" CACHE STRING "Vcpkg toolchain file")

# Include Boost and Poco
find_package(Boost REQUIRED COMPONENTS filesystem)
find_package(Poco REQUIRED COMPONENTS Foundation Net Util NetSSL)

# Add source files
add_executable(FileServer src/main.cpp)

# Include directories
target_include_directories(FileServer PRIVATE
    "C:/Users/yejih/vcpkg/installed/x64-windows/include"
)

# Add OpenSSL include directories
target_include_directories(FileServer PRIVATE 
    "C:/Users/yejih/vcpkg/installed/x64-windows/include/openssl"
)

# Link OpenSSL libraries
target_link_libraries(FileServer PRIVATE 
    OpenSSL::SSL 
    OpenSSL::Crypto
)

# Link libraries
target_link_libraries(FileServer PRIVATE
    Poco::Foundation
    Poco::Net
    Poco::Util
    Poco::NetSSL
    Boost::filesystem
)

# Debugging: Output library paths and configurations
message(STATUS "Boost include directories: ${Boost_INCLUDE_DIRS}")
message(STATUS "Boost libraries: ${Boost_LIBRARIES}")
message(STATUS "Poco libraries: ${Poco_LIBRARIES}")
