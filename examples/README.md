# Examples Directory

This directory contains practical examples showing how to use and integrate with the File Server.

## 📁 Directory Structure

### `clients/` - Client Applications
Example applications that connect to and use the file server:

- **`simple_client.cpp`** - Basic C++ client demonstrating core operations
- **`upload_client.cpp`** - Specialized client for bulk file uploads
- **`sync_client.cpp`** - File synchronization client example

### `integrations/` - Integration Examples
Examples showing how to integrate the file server with other systems:

- **`custom_storage.cpp`** - How to create custom storage backends
- **`example_plugin.cpp`** - Plugin system example

## 🚀 What Examples Demonstrate

### **1. Basic File Operations**
- User authentication (login/logout)
- File upload with metadata
- File download
- File listing and search
- File deletion and management

### **2. Advanced Features**
- File versioning
- Batch operations
- Real-time sync
- Custom storage backends
- Plugin development

### **3. Integration Patterns**
- REST API usage
- Error handling
- Authentication management
- File streaming
- Progress tracking

## 🛠️ Building Examples

### Prerequisites
- Built file server project
- Poco C++ Libraries
- OpenSSL

### Build Commands
```bash
# Build all examples
cd build
make examples

# Or build individually
g++ -std=c++17 examples/clients/simple_client.cpp \
    -lPocoFoundation -lPocoNet -lPocoNetSSL -lPocoJSON \
    -o simple_client
```

## 📖 Usage Examples

### Simple Client
```bash
# Run with default server (https://localhost:8443)
./simple_client

# Run with custom server
./simple_client https://your-server.com:8443
```

### Upload Client (Bulk Upload)
```bash
# Upload all files in a directory
./upload_client --directory /path/to/files --server https://localhost:8443

# Upload with specific patterns
./upload_client --pattern "*.pdf" --directory /documents
```

### Sync Client
```bash
# Sync local folder with server
./sync_client --local /home/user/documents --remote /documents --sync
```

## 🔧 Configuration

Most examples use configuration files or command-line arguments:

### Client Configuration (`client_config.json`)
```json
{
    "server": {
        "url": "https://localhost:8443",
        "verify_ssl": false,
        "timeout": 30000
    },
    "auth": {
        "username": "your_username",
        "password": "your_password",
        "token_file": ".auth_token"
    },
    "upload": {
        "chunk_size": 8388608,
        "max_retries": 3,
        "parallel_uploads": 4
    }
}
```

## 📚 Learning Objectives

These examples teach:

### **C++ Network Programming**
- HTTP/HTTPS client implementation
- JSON parsing and generation
- SSL/TLS certificate handling
- Asynchronous operations

### **API Integration**
- REST API consumption
- Authentication flows (JWT)
- Error handling and retry logic
- File streaming protocols

### **Modern C++ Practices**
- RAII resource management
- Exception handling
- Smart pointers usage
- Lambda functions
- Standard library containers

### **File Operations**
- Binary file handling
- Progress tracking
- Checksums and validation
- Metadata management

## 🔍 Code Walkthrough

### Simple Client Structure
```cpp
class FileServerClient {
private:
    string server_url;          // Server endpoint
    string auth_token;          // JWT authentication token
    HTTPSClientSession session; // Poco HTTPS client
    
public:
    // Authentication
    bool login(const string& username, const string& password);
    
    // File operations
    bool uploadFile(const string& local_path, const string& description = "");
    bool downloadFile(const string& file_id, const string& output_path);
    void listFiles();
    
    // Advanced operations
    bool searchFiles(const string& query);
    bool deleteFile(const string& file_id);
    bool shareFile(const string& file_id, const string& permissions);
};
```

### Key Concepts Demonstrated

1. **Authentication Flow**
   ```cpp
   // Login request
   Object::Ptr login_data = new Object;
   login_data->set("username", username);
   login_data->set("password", password);
   
   // Store JWT token for subsequent requests
   auth_token = response_obj->getValue<string>("token");
   ```

2. **File Upload (Multipart Form)**
   ```cpp
   // Multipart form data for file uploads
   form_data << "------boundary123\r\n";
   form_data << "Content-Disposition: form-data; name=\"file\"; filename=\"" 
             << filename << "\"\r\n";
   form_data << "Content-Type: application/octet-stream\r\n\r\n";
   form_data << file_content << "\r\n";
   ```

3. **Error Handling**
   ```cpp
   try {
       // Network operation
   } catch (const Poco::Exception& e) {
       cout << "Network error: " << e.displayText() << endl;
   } catch (const std::exception& e) {
       cout << "General error: " << e.what() << endl;
   }
   ```

## 🧪 Testing Examples

Run examples against your development server:

```bash
# Start your file server
./file_server --config config/server.json

# In another terminal, run examples
./simple_client https://localhost:8443
```

## 📝 Customization

Modify examples for your specific needs:

1. **Add Custom Headers**
   ```cpp
   request.add("X-Custom-Header", "value");
   ```

2. **Handle Different Response Formats**
   ```cpp
   if (response.getContentType().find("application/json") != string::npos) {
       // Parse JSON response
   } else {
       // Handle binary/text response
   }
   ```

3. **Implement Retry Logic**
   ```cpp
   int max_retries = 3;
   for (int attempt = 0; attempt < max_retries; ++attempt) {
       if (performOperation()) break;
       this_thread::sleep_for(chrono::seconds(1 << attempt)); // Exponential backoff
   }
   ```

These examples serve as both learning tools and starting points for building your own file server clients and integrations.