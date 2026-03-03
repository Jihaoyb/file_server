# Milestone 6 Design: Distributed Metadata and Storage Nodes

## Objective

Move NebulaFS from single-node local storage to a distributed architecture where:
- a metadata service owns namespace and object-part placement state,
- storage nodes own object bytes,
- the gateway remains the public HTTP API surface.

## Scope

- Define service boundaries (Gateway, Metadata Service, Storage Node).
- Define internal APIs for object writes/reads/listing.
- Define initial consistency and failure model.
- Define rollout plan from single-node mode.

## Non-goals (Milestone 6)

- Multi-region replication.
- Erasure coding.
- Per-tenant quota or billing enforcement.
- Automatic rebalancing across clusters.

## Current baseline constraints

- Gateway directly reads/writes local filesystem.
- SQLite is embedded and local to process.
- Auth is validated in gateway using JWT/JWKS.
- Metrics are process-local counters.

## Target architecture

```text
Client -> Gateway (HTTP API, auth, validation)
Gateway -> Metadata Service (object metadata + placement)
Gateway -> Storage Node(s) (byte upload/download/delete)
```

Key rule: Gateway no longer owns object bytes directly in distributed mode.

## Service responsibilities

### Gateway
- Keep existing public REST API contract.
- Perform auth, request validation, and routing orchestration.
- Request placement decisions from Metadata Service.
- Stream object bytes to/from Storage Nodes.

### Metadata Service
- Source of truth for buckets, objects, versions, and placement.
- Allocate write targets for new objects/parts.
- Track object state transitions (`pending`, `committed`, `deleted`).
- Return read location list for object retrieval.

### Storage Node
- Store and serve object blobs and multipart parts.
- Expose internal APIs for `PUT/GET/DELETE` by object key + placement token.
- Report health/capacity for placement decisions.

## Data model changes (proposal)

Extend metadata schema with:
- `storage_nodes(id, endpoint, status, capacity_bytes, free_bytes, updated_at)`
- `object_replicas(object_id, node_id, replica_index, state, checksum, updated_at)`
- `object_states` upgrade to include `pending_commit` for two-phase write flow.

## Internal API contract (initial)

Use internal HTTP+JSON for first milestone to reduce integration cost.

### Metadata Service endpoints
- `POST /internal/v1/objects/allocate-write`
  - input: bucket/object/content hints
  - output: placement targets + write token
- `POST /internal/v1/objects/commit`
  - input: object identity + uploaded replica receipts
  - output: committed object metadata
- `GET /internal/v1/objects/resolve-read`
  - input: bucket/object
  - output: ordered readable replicas

### Storage Node endpoints
- `PUT /internal/v1/blobs/{blob_id}`
  - requires signed placement token
- `GET /internal/v1/blobs/{blob_id}`
- `DELETE /internal/v1/blobs/{blob_id}`

## Write flow (initial consistency model)

1. Gateway requests allocation from Metadata Service.
2. Metadata Service returns target node list + write token.
3. Gateway streams payload to each target storage node.
4. Gateway sends commit request with receipts/checksums.
5. Metadata Service marks object committed and visible.

Visibility rule: object is readable only after commit succeeds.

## Read flow

1. Gateway asks Metadata Service to resolve readable replicas.
2. Gateway attempts primary replica, then fallback replicas on failure.
3. Gateway streams bytes to client while preserving current HTTP semantics.

## Failure handling

- Partial write before commit: Metadata Service keeps object non-visible; cleanup job removes orphan blobs.
- Storage node down during write: Gateway returns failure unless minimum replica policy satisfied.
- Metadata unavailable: Gateway rejects mutating requests; may optionally serve cached read resolutions in later milestone.

## Security

- Public auth remains at gateway.
- Internal service calls require service-to-service auth token (static shared secret for initial milestone).
- Placement tokens are short-lived and scoped to blob/action.

## Observability additions

- Gateway:
  - `nebulafs_gateway_storage_put_failures_total`
  - `nebulafs_gateway_metadata_rpc_failures_total`
- Metadata Service:
  - allocation latency and commit latency counters/timers
- Storage Node:
  - blob write/read/delete counters and latency

## Rollout plan

1. Add abstraction layer (`StorageBackend`, `MetadataBackend`) behind current gateway handlers.
2. Keep local adapters as default implementation.
3. Add remote adapters guarded by config flag (`mode: single_node|distributed`).
4. Introduce metadata service binary and storage node binary.
5. Add distributed integration test lane in CI (single-host multi-process).

## Test strategy

Unit tests:
- placement selection, write token validation, state transitions.

Integration tests:
- gateway + metadata + 2 storage nodes happy path upload/download/delete.
- primary node failure with replica fallback on read.
- failed commit keeps object invisible.

Compatibility tests:
- existing single-node behavior unchanged when `mode=single_node`.

## Implementation sequence

1. Add backend interfaces and keep local implementations.
2. Add metadata service process and schema migrations.
3. Add storage node process and blob APIs.
4. Add gateway remote adapters and distributed mode wiring.
5. Add distributed integration tests and CI job.
