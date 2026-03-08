# NebulaFS Architecture

## Modules

- `core/`: configuration, logging, IDs, error handling.
- `http/`: Beast-based HTTP server, routing, request context.
- `storage/`: storage backends (`LocalStorage`, `RemoteStorageBackend`) and storage-node service.
- `metadata/`: metadata backends (`SqliteMetadataStore`, `RemoteMetadataStore`) and metadata service.
- `auth/`: JWT parsing/verification with JWKS cache.
- `observability/`: health, readiness, metrics.
- `distributed/`: internal HTTP client + placement token signing/validation.

## Runtime Modes

- `single_node` (default):
  - gateway serves public API and uses local filesystem + local SQLite metadata.
- `distributed`:
  - gateway remains public API surface and orchestrates writes/reads via internal services.
  - metadata service owns placement + object visibility state.
  - storage nodes own blob bytes and internal blob CRUD endpoints.

## Concurrency Model

- One `boost::asio::io_context` shared across a thread pool.
- Each connection is managed by a session object using a strand.
- HTTP parsing uses Beast; request bodies are streamed to storage.

## Storage Layout (single_node and storage-node local disk)

```
<base_path>/
  buckets/                # single_node object layout
    <bucket>/
      objects/
        <object>
  blobs/                  # storage-node blob layout
    <blob_id>
```

Uploads are written to `<base_path>/tmp/<uuid>` then `fsync` + `rename` to final path.

## Metadata Schema (SQLite baseline)

- `buckets(id, name, created_at)`
- `objects(id, bucket_id, name, size_bytes, etag, created_at, updated_at)`
- `storage_nodes(id, endpoint, status, updated_at)`
- `object_replicas(object_id, node_id, blob_id, replica_index, state, checksum, updated_at)`

## Error Handling

- Internal code uses a `Result<T>` type; HTTP errors return a JSON envelope with a request ID.

## Milestone 6 Status

- Implemented baseline:
  - distributed object CRUD flow (`allocate-write -> storage PUTs -> commit -> resolve-read`)
  - distributed multipart baseline (create/upload-parts/list/complete/abort)
  - storage-node server-side compose for distributed multipart complete
  - replica fallback on read
  - write quorum enforcement
  - distributed integration lane in CI
  - gateway/metadata/storage-node metrics
- Deferred:
  - distributed cleanup leadership/coordination for multi-gateway deployments
  - cross-cluster multipart/orphan reconciliation beyond best-effort sweeps
