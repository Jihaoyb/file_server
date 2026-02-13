# NebulaFS Architecture

## Modules

- `core/`: configuration, logging, IDs, error handling.
- `http/`: Beast-based HTTP server, routing, request context.
- `storage/`: filesystem storage engine with atomic writes.
- `metadata/`: SQLite-backed metadata store.
- `auth/`: JWT parsing/verification with JWKS cache.
- `observability/`: health, readiness, metrics.

## Planned Milestone 4 extensions

- Multipart upload coordinator (initiate, part upload, complete, abort).
- Multipart metadata tables for in-progress uploads and parts.
- Background cleanup sweep for expired/incomplete uploads.
- Additional multipart metrics (throughput, failures, cleanup activity).

## Concurrency Model

- One `boost::asio::io_context` shared across a thread pool.
- Each connection is managed by a session object using a strand.
- HTTP parsing uses Beast; request bodies are streamed to storage.

## Storage Layout

```
<base_path>/
  buckets/
    <bucket>/
      objects/
        <object>
```

Uploads are written to `<base_path>/tmp/<uuid>` then `fsync` + `rename` to final path.

## Metadata Schema (SQLite)

- `buckets(id, name, created_at)`
- `objects(id, bucket_id, name, size_bytes, etag, created_at, updated_at)`

## Error Handling

- Internal code uses a `Result<T>` type; HTTP errors return a JSON envelope with a request ID.
