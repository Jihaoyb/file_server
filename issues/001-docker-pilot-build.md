# Build and run the pilot in Docker

Type: AFK

Recommended subagent: `gpt-6.2-sol` / `high`

## Parent PRD

[Approved PRD](prd.md)

## What to build

Deliver a clean-checkout Linux container build and Compose startup for the single-node pilot.

## Acceptance criteria

- [ ] Build the image and start the stack from a clean checkout using documented commands.
- [ ] Keep files and SQLite metadata in persistent storage. Prove persistence across container replacement.
- [ ] Bind the published port to localhost, configure restart, and load deployment secrets outside Git. Verify health and readiness.

## Blocked by

None. Can start immediately.

## User stories addressed

Deployment prerequisite for the browser workflow.
