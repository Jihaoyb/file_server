# NebulaFS

NebulaFS is a production-grade, cloud-storage style file server written in C++20 using Boost.Asio/Beast for async HTTP and Poco for configuration, logging, and utilities. It is built as a learning-by-building project that scales from a single-node file server to a distributed storage cluster.

## Highlights

- **Async HTTP** server with Boost.Asio/Beast and multi-threaded IO.
- **Local filesystem storage** with atomic writes and checksum-based ETags.
- **SQLite metadata** for buckets and objects.
- **Structured logging** via Poco with request correlation.
- **Security-first** design (TLS, OIDC/JWT planned next milestone).

## Architecture (Milestone 0–2)

```mermaid
flowchart LR
  client((Client)) -->|HTTPS| gateway[HTTP Server]
  gateway --> auth[Auth (next milestone)]
  gateway --> storage[Local Storage Engine]
  gateway --> metadata[SQLite Metadata]
  gateway --> observability[Metrics/Health]
```

## Quickstart

### Prerequisites
- CMake 3.20+
- C++20 compiler
- vcpkg

### Build
```bash
cmake --preset debug
cmake --build --preset debug
```

### Run
```bash
./build/debug/nebulafs --config config/server.json
```

### Example API calls
```bash
# Health
curl http://localhost:8080/healthz

# Create bucket
curl -X POST http://localhost:8080/v1/buckets -d '{"name":"demo"}'

# Upload object
curl -X PUT \
  --data-binary @README.md \
  http://localhost:8080/v1/buckets/demo/objects/readme.txt

# Upload object (query-style)
curl -X POST \
  --data-binary @README.md \
  "http://localhost:8080/v1/buckets/demo/objects?name=readme.txt"

# Download object
curl http://localhost:8080/v1/buckets/demo/objects/readme.txt -o readme.txt

# List objects
curl "http://localhost:8080/v1/buckets/demo/objects?prefix=read"
```

## Security Model (Current)
- TLS supported via config; disabled by default for local dev.
- No auth yet (Milestone 3). Health is public; all other endpoints are open in dev.
- Path traversal protection enforced in storage.
- Size limits enforced by config.

## Performance Notes (Current)
- Async IO with per-connection strands.
- Streaming request bodies to disk with size limits.
- Download supports HTTP range requests.

## Roadmap
- **Milestone 3**: OIDC/JWT validation with JWKS caching (Keycloak dev compose).
- **Milestone 4**: Multipart uploads, background cleanup jobs.
- **Milestone 5**: Metrics (Prometheus), rate limiting, timeouts.
- **Milestone 6**: Distributed mode with metadata service and storage nodes.

## Docs
- Architecture: `docs/architecture.md`
- Threat model: `docs/threat-model.md`
- ADRs: `docs/adr/`
- Code style: `docs/code-style.md`

## License
MIT. See `LICENSE`.
