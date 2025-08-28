# Examples configuration

# Simple client example
add_executable(simple_client clients/simple_client.cpp)
target_link_libraries(simple_client
    Poco::Foundation
    Poco::Net
    Poco::NetSSL
    Poco::JSON
    OpenSSL::SSL
    OpenSSL::Crypto
)
set_target_properties(simple_client PROPERTIES
    RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/examples"
)

# Upload client example
add_executable(upload_client clients/upload_client.cpp)
target_link_libraries(upload_client
    Poco::Foundation
    Poco::Net
    Poco::NetSSL
    Poco::JSON
    OpenSSL::SSL
    OpenSSL::Crypto
)
set_target_properties(upload_client PROPERTIES
    RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/examples"
)

# Sync client example
add_executable(sync_client clients/sync_client.cpp)
target_link_libraries(sync_client
    Poco::Foundation
    Poco::Net
    Poco::NetSSL
    Poco::JSON
    OpenSSL::SSL
    OpenSSL::Crypto
)
set_target_properties(sync_client PROPERTIES
    RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/examples"
)

# Custom storage backend example
add_executable(custom_storage_example integrations/custom_storage.cpp)
target_link_libraries(custom_storage_example
    file_server_lib
)
set_target_properties(custom_storage_example PROPERTIES
    RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/examples"
)

# Plugin example
add_library(example_plugin SHARED integrations/example_plugin.cpp)
target_link_libraries(example_plugin
    file_server_lib
)
set_target_properties(example_plugin PROPERTIES
    LIBRARY_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/examples/plugins"
)