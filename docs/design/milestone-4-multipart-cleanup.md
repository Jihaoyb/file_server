# Milestone 4 Design: Multipart Uploads and Cleanup Jobs

## Objective

Add reliable large-file upload support without loading full payloads in memory, while ensuring stale partial uploads are cleaned automatically.

## Scope

- Multipart upload lifecycle APIs (initiate, upload part, list parts, complete, abort).
- Metadata schema and storage layout for in-progress uploads.
- Background cleanup for expired/incomplete uploads.
- Test strategy for functional and failure paths.

## Non-goals (Milestone 4)

- Cross-node/distributed uploads.
- Versioned objects.
- Per-user quotas or billing.

## Current baseline constraints

- HTTP routing is path-pattern based, with minimal query parsing.
- Storage is local filesystem with temp-write then rename.
- Metadata is SQLite (`buckets`, `objects`).
- Auth layer validates JWT; authorization policy is not yet implemented.

## Proposed API

To fit the existing router and keep request matching explicit, use path-based endpoints:

1. Initiate upload
   - `POST /v1/buckets/{bucket}/multipart-uploads`
   - Request body: `{"object":"path/to/file.bin","content_type":"..."}`
   - Response: `201 Created` with `upload_id`, `expires_at`, limits.

2. Upload part
   - `PUT /v1/buckets/{bucket}/multipart-uploads/{upload_id}/parts/{part_number}`
   - Body: raw bytes of one part.
   - Response: `200 OK` with `etag`, `part_number`, `size_bytes`.

3. List uploaded parts
   - `GET /v1/buckets/{bucket}/multipart-uploads/{upload_id}/parts`
   - Response: ordered parts with checksum/etag and size.

4. Complete upload
   - `POST /v1/buckets/{bucket}/multipart-uploads/{upload_id}/complete`
   - Request body: ordered part list with expected etags.
   - Response: `200 OK` with final object metadata.

5. Abort upload
   - `DELETE /v1/buckets/{bucket}/multipart-uploads/{upload_id}`
   - Response: `204 No Content`.

## Multipart state machine

- `initiated` -> `uploading` -> `completed`
- `initiated|uploading` -> `aborted`
- `initiated|uploading` -> `expired` (cleanup sweep)

Invalid transitions must return `409 Conflict`.

## Storage layout

Keep parts in temp area until completion:

```text
<base_path>/tmp/multipart/<upload_id>/part-00001
<base_path>/tmp/multipart/<upload_id>/part-00002
...
```

On complete:
- Verify all requested parts exist and etags match.
- Concatenate to a new temp object file.
- `fsync` + atomic rename into final object path.
- Upsert object metadata in `objects`.
- Remove multipart temp directory and multipart metadata rows.

## Metadata/schema changes

Add two new tables:

- `multipart_uploads`
  - `id INTEGER PRIMARY KEY AUTOINCREMENT`
  - `upload_id TEXT NOT NULL UNIQUE`
  - `bucket_id INTEGER NOT NULL`
  - `object_name TEXT NOT NULL`
  - `state TEXT NOT NULL` (`initiated|uploading|completed|aborted|expired`)
  - `expires_at TEXT NOT NULL`
  - `created_at TEXT NOT NULL`
  - `updated_at TEXT NOT NULL`
  - `FOREIGN KEY(bucket_id) REFERENCES buckets(id) ON DELETE CASCADE`

- `multipart_parts`
  - `id INTEGER PRIMARY KEY AUTOINCREMENT`
  - `upload_id TEXT NOT NULL`
  - `part_number INTEGER NOT NULL`
  - `size_bytes INTEGER NOT NULL`
  - `etag TEXT NOT NULL`
  - `temp_path TEXT NOT NULL`
  - `created_at TEXT NOT NULL`
  - `UNIQUE(upload_id, part_number)`
  - `FOREIGN KEY(upload_id) REFERENCES multipart_uploads(upload_id) ON DELETE CASCADE`

Recommended indexes:
- `idx_multipart_uploads_expires_at` on `multipart_uploads(expires_at)`
- `idx_multipart_parts_upload_id` on `multipart_parts(upload_id)`

## Config additions (proposal)

Add under `storage.multipart`:

- `enabled` (bool, default `true`)
- `max_parts` (int, default `10000`)
- `max_part_bytes` (int, default `134217728` = 128 MiB)
- `max_upload_ttl_seconds` (int, default `86400`)

Add under `jobs.cleanup`:

- `enabled` (bool, default `true`)
- `sweep_interval_seconds` (int, default `300`)
- `grace_period_seconds` (int, default `60`)
- `max_uploads_per_sweep` (int, default `200`)

## Cleanup job design

- Run a periodic in-process task on the server IO context.
- Select uploads where `state in (initiated, uploading)` and `expires_at < now - grace`.
- For each upload:
  - mark state `expired`
  - delete temp files/dir best-effort
  - delete `multipart_parts` + `multipart_uploads` rows
- Job must be idempotent and safe after process restart.

## Error model

Use existing JSON error envelope + request ID.

- `400 Bad Request`: invalid part number, malformed body.
- `404 Not Found`: bucket/upload/part not found.
- `409 Conflict`: invalid state transition or etag mismatch.
- `413 Payload Too Large`: part too large.
- `500 Internal Server Error`: unexpected file/DB errors.

## Observability additions

- Counters:
  - `multipart_upload_initiated_total`
  - `multipart_part_uploaded_total`
  - `multipart_upload_completed_total`
  - `multipart_upload_aborted_total`
  - `multipart_upload_expired_total`
- Histograms:
  - part upload latency
  - completion latency
  - cleanup sweep duration
- Gauges:
  - in-progress multipart uploads
  - temp multipart bytes

## Security considerations

- Keep all multipart endpoints behind auth (same as object APIs).
- Enforce path safety for object names and temp paths.
- Validate part numbers and total limits to prevent resource abuse.
- Log request ID + upload ID for auditability.

## Test strategy

Unit tests:
- multipart state transition validation.
- part-number and size limit checks.
- cleanup eligibility logic (expiry/grace).
- metadata store CRUD for uploads/parts.

Integration tests:
- happy path: initiate -> upload parts -> complete -> download object.
- abort path: initiate -> upload parts -> abort -> verify removal.
- expiry path: initiate -> wait/force expiry -> cleanup -> verify removal.
- idempotency: repeat part upload for same part number (replace semantics decision).
- failure injection: missing part on complete, etag mismatch, invalid upload_id.

CI strategy:
- Keep default integration tests fast.
- Add one focused multipart integration suite first.
- Defer large-payload stress tests to nightly workflow once baseline is stable.

## Implementation sequence

1. Extend metadata schema and store interface.
2. Add multipart storage primitives (part write/read/concat/delete).
3. Add HTTP handlers and routing for multipart endpoints.
4. Add completion/abort logic.
5. Add cleanup scheduler and sweep logic.
6. Add metrics and logs.
7. Add tests and CI wiring.
