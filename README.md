# Personal File Server

A high-performance, secure, and feature-rich personal file server built with modern C++ and the Poco framework. This project serves as both a practical self-hosted file storage solution and an educational resource for learning advanced C++ development techniques.

## 🎯 Project Overview

This personal file server is designed to replace commercial cloud storage solutions with a self-hosted alternative that gives you complete control over your data. Built with industrial-grade practices, it demonstrates modern C++ development, network programming, database design, and security implementation.

### Key Features

- **🌐 RESTful API** - Complete HTTP/HTTPS API for file operations
- **🔐 Security First** - JWT authentication, TLS encryption, role-based access control
- **📁 Advanced File Management** - Upload, download, versioning, deduplication, metadata
- **🔍 Search & Discovery** - Full-text search, filtering, and content indexing
- **📱 Multi-Platform** - Web interface, mobile apps, desktop clients
- **⚡ High Performance** - Asynchronous I/O, connection pooling, caching
- **🔄 Real-time Sync** - WebSocket notifications, live file synchronization
- **📊 Analytics** - Usage metrics, monitoring, and reporting
- **🐳 Production Ready** - Docker support, clustering, load balancing

## 🏗️ Architecture Overview

### System Architecture

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Web Client    │    │   Mobile App    │    │  Desktop Client │
└─────────┬───────┘    └─────────┬───────┘    └─────────┬───────┘
          │                      │                      │
          └──────────────────────┼──────────────────────┘
                                 │
                    ┌────────────┴─────────────┐
                    │     Load Balancer        │
                    └────────────┬─────────────┘
                                 │
          ┌──────────────────────┼──────────────────────┐
          │                      │                      │
    ┌─────┴─────┐          ┌─────┴─────┐          ┌─────┴─────┐
    │   Node 1  │          │   Node 2  │          │   Node N  │
    └───────────┘          └───────────┘          └───────────┘
          │                      │                      │
          └──────────────────────┼──────────────────────┘
                                 │
                    ┌────────────┴─────────────┐
                    │    Database Cluster      │
                    │   (Metadata & Users)     │
                    └──────────────────────────┘
                                  │
                    ┌─────────────┴─────────────┐
                    │    File Storage Layer     │
                    │  (Local/Cloud Storage)    │
                    └───────────────────────────┘
```

### Component Architecture

```cpp
namespace FileServer {
    // Core Components
    class Application;          // Main application orchestrator
    class HTTPServer;          // Poco-based HTTP/HTTPS server
    class AuthManager;         // JWT and session management
    class FileManager;         // File operations and metadata
    class DatabaseManager;     // Data persistence layer
    class ConfigManager;       // Configuration management
    class Logger;              // Structured logging system
    
    // Services
    namespace Services {
        class FileService;     // File CRUD operations
        class UserService;     // User management
        class SearchService;   // File search and indexing
        class SyncService;     // Real-time synchronization
        class MetricsService;  // Analytics and monitoring
    }
    
    // Storage Backends
    namespace Storage {
        class StorageBackend;  // Abstract storage interface
        class LocalStorage;    // Local filesystem storage
        class CloudStorage;    // Cloud storage (S3, Azure, etc.)
        class CachedStorage;   // Caching layer
    }
}
```

## 🚀 Quick Start

### Prerequisites

- **C++17 or later** (C++20 recommended)
- **CMake 3.20+**
- **vcpkg package manager**
- **Git**

### Installation

1. **Clone the repository**
   ```bash
   git clone https://github.com/Jihaoyb/file_server.git
   cd file_server
   ```

2. **Install dependencies with vcpkg**
   ```bash
   # Install vcpkg if not already installed
   git clone https://github.com/Microsoft/vcpkg.git
   cd vcpkg
   ./bootstrap-vcpkg.sh  # or .\bootstrap-vcpkg.bat on Windows
   
   # Install dependencies
   ./vcpkg install poco[netssl,data-sqlite,json] gtest openssl
   ```

3. **Build the project**
   ```bash
   mkdir build && cd build
   cmake -DCMAKE_TOOLCHAIN_FILE=[vcpkg root]/scripts/buildsystems/vcpkg.cmake ..
   cmake --build . --config Release
   ```

4. **Run the server**
   ```bash
   ./file_server --config ../config/server.json
   ```

### Docker Quick Start

```bash
# Build Docker image
docker build -t personal-file-server .

# Run with Docker Compose
docker-compose up -d

# Access at https://localhost:8443
```

## 📚 Usage Guide

### Basic Operations

#### Upload Files
```bash
# Upload a single file
curl -X POST -F "file=@document.pdf" \
  -H "Authorization: Bearer YOUR_JWT_TOKEN" \
  https://localhost:8443/api/v1/files

# Upload with metadata
curl -X POST -F "file=@photo.jpg" \
  -F "description=Vacation photo" \
  -F "tags=vacation,beach,2024" \
  -H "Authorization: Bearer YOUR_JWT_TOKEN" \
  https://localhost:8443/api/v1/files
```

#### Download Files
```bash
# Download by file ID
curl -H "Authorization: Bearer YOUR_JWT_TOKEN" \
  https://localhost:8443/api/v1/files/12345/download -o downloaded_file.pdf

# Download with specific version
curl -H "Authorization: Bearer YOUR_JWT_TOKEN" \
  https://localhost:8443/api/v1/files/12345/download?version=2 -o file_v2.pdf
```

#### Search Files
```bash
# Search by filename
curl -H "Authorization: Bearer YOUR_JWT_TOKEN" \
  "https://localhost:8443/api/v1/files/search?query=vacation&type=image"

# Advanced search with filters
curl -H "Authorization: Bearer YOUR_JWT_TOKEN" \
  "https://localhost:8443/api/v1/files/search?query=report&date_from=2024-01-01&size_max=10MB"
```

### Web Interface

Access the web interface at `https://localhost:8443` after starting the server:

- **Dashboard**: Overview of storage usage and recent activity
- **File Browser**: Navigate, upload, download, and organize files
- **Search**: Advanced file search with filters and previews
- **Settings**: User preferences, security settings, and account management
- **Admin Panel**: User management, system monitoring, and configuration

### API Documentation

The complete API documentation is available at:
- **Interactive Docs**: `https://localhost:8443/api/docs`
- **OpenAPI Spec**: `https://localhost:8443/api/v1/openapi.json`

Key API endpoints:

| Method | Endpoint | Description |
|--------|----------|-------------|
| `POST` | `/api/v1/auth/login` | User authentication |
| `GET` | `/api/v1/files` | List user files |
| `POST` | `/api/v1/files` | Upload new file |
| `GET` | `/api/v1/files/{id}` | Get file metadata |
| `GET` | `/api/v1/files/{id}/download` | Download file |
| `PUT` | `/api/v1/files/{id}` | Update file metadata |
| `DELETE` | `/api/v1/files/{id}` | Delete file |
| `GET` | `/api/v1/files/search` | Search files |
| `POST` | `/api/v1/files/{id}/share` | Create file share link |

## 🛠️ Development Guide

### Project Structure

```
file_server/
├── src/                    # Source code
│   ├── core/              # Core application classes
│   ├── services/          # Business logic services
│   ├── handlers/          # HTTP request handlers
│   ├── storage/           # Storage backend implementations
│   ├── auth/              # Authentication and authorization
│   └── utils/             # Utility functions and classes
├── include/               # Public header files
│   └── file_server/       # Main include directory
├── tests/                 # Test suite
│   ├── unit/             # Unit tests
│   ├── integration/      # Integration tests
│   └── performance/      # Performance benchmarks
├── docs/                  # Additional documentation
│   ├── api/              # API documentation
│   ├── deployment/       # Deployment guides
│   └── architecture/     # Architecture documentation
├── config/               # Configuration files
│   ├── server.json       # Server configuration
│   ├── database.json     # Database settings
│   └── ssl/             # SSL certificates
├── scripts/              # Build and deployment scripts
│   ├── build.sh         # Build script
│   ├── deploy.sh        # Deployment script
│   └── backup.sh        # Backup script
├── docker/               # Docker configuration
│   ├── Dockerfile       # Main Docker image
│   ├── docker-compose.yml # Development environment
│   └── production/      # Production Docker setup
└── examples/             # Usage examples
    ├── clients/         # Client implementations
    └── integrations/    # Integration examples
```

### Building from Source

#### Debug Build
```bash
mkdir build-debug && cd build-debug
cmake -DCMAKE_BUILD_TYPE=Debug \
      -DCMAKE_TOOLCHAIN_FILE=[vcpkg root]/scripts/buildsystems/vcpkg.cmake \
      ..
make -j$(nproc)
```

#### Release Build with Optimizations
```bash
mkdir build-release && cd build-release
cmake -DCMAKE_BUILD_TYPE=Release \
      -DCMAKE_TOOLCHAIN_FILE=[vcpkg root]/scripts/buildsystems/vcpkg.cmake \
      -DENABLE_LTO=ON \
      -DENABLE_OPTIMIZATIONS=ON \
      ..
make -j$(nproc)
```

### Testing

```bash
# Run all tests
make test

# Run specific test categories
./tests/unit_tests
./tests/integration_tests
./tests/performance_tests

# Run with coverage report
cmake -DENABLE_COVERAGE=ON ..
make coverage
```

### Code Quality

```bash
# Format code
make format

# Static analysis
make static-analysis

# Memory leak detection
make memcheck

# Security analysis
make security-check
```

## 🏛️ C++ Learning Objectives

This project demonstrates advanced C++ concepts and best practices:

### Modern C++ Features (C++17/20)
- **Smart Pointers**: RAII and automatic memory management
- **Move Semantics**: Efficient resource transfer
- **Lambda Functions**: Functional programming patterns
- **Template Metaprogramming**: Generic and type-safe code
- **Concepts** (C++20): Template constraints and requirements
- **Coroutines** (C++20): Asynchronous programming
- **Modules** (C++20): Improved compilation and encapsulation

### Design Patterns Implementation
- **Singleton Pattern**: Configuration and logging
- **Factory Pattern**: Storage backend creation
- **Observer Pattern**: Event notification system
- **Strategy Pattern**: Pluggable algorithms
- **RAII Pattern**: Resource management throughout
- **Template Method**: Request processing pipeline

### Network Programming
- **Asynchronous I/O**: Non-blocking operations with Poco::Net
- **HTTP Protocol**: Request/response handling
- **WebSocket Protocol**: Real-time communication
- **SSL/TLS**: Secure communication
- **Connection Pooling**: Resource optimization

### Database Programming
- **SQL Generation**: Type-safe query building
- **Connection Management**: Pool and transaction handling
- **ORM Concepts**: Object-relational mapping
- **Migration System**: Database version control
- **Performance Optimization**: Indexing and query optimization

### Security Implementation
- **Cryptographic Hashing**: Password and file integrity
- **JWT Tokens**: Stateless authentication
- **TLS Configuration**: Secure communication setup
- **Input Validation**: SQL injection and XSS prevention
- **Rate Limiting**: DoS protection

### Performance Optimization
- **Memory Management**: Custom allocators and pool allocation
- **Caching Strategies**: LRU and time-based caching
- **Concurrency**: Thread pools and synchronization
- **I/O Optimization**: Memory-mapped files and buffering
- **Profiling Integration**: Performance measurement

## 📊 Performance Benchmarks

### Expected Performance Metrics
- **Concurrent Connections**: 10,000+ simultaneous users
- **File Upload Speed**: 100MB/s+ on gigabit connection
- **Database Queries**: <5ms average response time
- **Memory Usage**: <500MB base memory footprint
- **CPU Usage**: <20% under normal load

### Scalability Targets
- **File Storage**: Petabyte-scale storage support
- **User Base**: 100,000+ registered users
- **Throughput**: 1,000+ requests per second
- **High Availability**: 99.9% uptime SLA

## 🔧 Configuration

### Server Configuration (`config/server.json`)
```json
{
  "server": {
    "host": "0.0.0.0",
    "port": 8443,
    "ssl": {
      "enabled": true,
      "certificate": "config/ssl/server.crt",
      "private_key": "config/ssl/server.key"
    },
    "max_connections": 10000,
    "timeout": 30000
  },
  "database": {
    "type": "sqlite",
    "connection_string": "data/fileserver.db",
    "pool_size": 20
  },
  "storage": {
    "backend": "local",
    "base_path": "data/files",
    "max_file_size": "1GB",
    "allowed_extensions": ["*"]
  },
  "security": {
    "jwt_secret": "your-secret-key-here",
    "jwt_expiry": 86400,
    "bcrypt_rounds": 12,
    "rate_limit": {
      "requests_per_minute": 1000,
      "burst_size": 100
    }
  }
}
```

## 🚀 Deployment

### Production Deployment with Docker

1. **Build production image**
   ```bash
   docker build -t personal-file-server:latest -f docker/Dockerfile.prod .
   ```

2. **Deploy with Docker Compose**
   ```bash
   cd docker/production
   docker-compose up -d
   ```

3. **Configure reverse proxy** (Nginx example)
   ```nginx
   server {
       listen 443 ssl http2;
       server_name your-domain.com;
       
       ssl_certificate /etc/ssl/certs/your-cert.pem;
       ssl_certificate_key /etc/ssl/private/your-key.pem;
       
       location / {
           proxy_pass https://localhost:8443;
           proxy_set_header Host $host;
           proxy_set_header X-Real-IP $remote_addr;
           proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
           proxy_set_header X-Forwarded-Proto $scheme;
       }
   }
   ```

### Kubernetes Deployment

```yaml
# k8s/deployment.yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: file-server
spec:
  replicas: 3
  selector:
    matchLabels:
      app: file-server
  template:
    metadata:
      labels:
        app: file-server
    spec:
      containers:
      - name: file-server
        image: personal-file-server:latest
        ports:
        - containerPort: 8443
        env:
        - name: CONFIG_PATH
          value: "/app/config/server.json"
        volumeMounts:
        - name: config
          mountPath: /app/config
        - name: data
          mountPath: /app/data
      volumes:
      - name: config
        configMap:
          name: file-server-config
      - name: data
        persistentVolumeClaim:
          claimName: file-server-storage
```

## 🤝 Contributing

### Development Workflow

1. **Fork the repository**
2. **Create a feature branch**
   ```bash
   git checkout -b feature/new-feature
   ```
3. **Make your changes**
4. **Run tests and linting**
   ```bash
   make test
   make lint
   ```
5. **Commit with conventional commits**
   ```bash
   git commit -m "feat: add file versioning support"
   ```
6. **Push and create a pull request**

### Code Standards
- Follow [Google C++ Style Guide](https://google.github.io/styleguide/cppguide.html)
- Use `clang-format` for code formatting
- Maintain >90% test coverage
- Document all public APIs with Doxygen
- Write meaningful commit messages

### Issue Reporting
- Use issue templates for bugs and feature requests
- Include minimal reproducible examples
- Specify environment details (OS, compiler, versions)

## 📈 Roadmap

### Phase 1: Foundation ✅
- [x] Project setup and build system
- [x] Basic HTTP server with Poco
- [x] Database integration
- [x] Authentication system

### Phase 2: Core Features (In Progress)
- [ ] File upload/download operations
- [ ] RESTful API implementation
- [ ] Web interface development
- [ ] File metadata management

### Phase 3: Advanced Features
- [ ] File versioning and deduplication
- [ ] Search and indexing system
- [ ] Real-time synchronization
- [ ] Mobile applications

### Phase 4: Enterprise Features
- [ ] Clustering and load balancing
- [ ] Advanced security features
- [ ] Analytics and monitoring
- [ ] Third-party integrations

### Future Versions
- **v2.0**: Microservices architecture with gRPC
- **v3.0**: Machine learning features (content analysis, recommendations)
- **v4.0**: Blockchain integration for file integrity

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🙏 Acknowledgments

- **Poco C++ Libraries** - Comprehensive C++ framework
- **vcpkg** - C++ package manager
- **Google Test** - Testing framework
- **OpenSSL** - Cryptography and SSL/TLS
- **SQLite** - Embedded database engine
