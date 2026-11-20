# Retrieve a verified file through an expiring link

Type: AFK

Recommended subagent: `gpt-6.2-astra` / `high`

## Parent PRD

[Approved PRD](prd.md)

## What to build

Let the sender share the verified object with the recipient after upload.

## Acceptance criteria

- [ ] Only verified objects receive unguessable bearer links. A forwarded valid link grants access and works after the sender goes offline.
- [ ] Check expiry on every download and range request. Enforce a 24-hour link lifetime bounded by file retention.
- [ ] Verify source, finalized file, and downloaded copy against the same SHA-256. Test interrupted download and resume with a supported client.

## Blocked by

- [003](003-verified-finalization.md)

## User stories addressed

9, 10, 11, 12
