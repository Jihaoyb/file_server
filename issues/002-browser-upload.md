# Send a file from the browser

Type: AFK

Recommended subagent: `gpt-6.2-sol` / `high`

## Parent PRD

[Approved PRD](prd.md)

## What to build

Deliver a same-origin, invite-restricted browser upload through the existing multipart API.

## Acceptance criteria

- [ ] An invited sender can select a file and see upload progress and errors. Unauthorized requests cannot create, inspect, or modify uploads.
- [ ] Start with 8 MiB parts. Stream file reads and enforce the 12 GB maximum server-side. Verify boundary behavior and single-node multipart compatibility.
- [ ] Show completion only when the server reports a verified result. Integrate with issue 003 before public deployment.

## Blocked by

None. Can start immediately.

## User stories addressed

1, 2, 3, 4, 5
