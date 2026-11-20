# Verify clean deployment and Windows reboot recovery

Type: HITL

Recommended subagent: `gpt-6.2-sol` / `high`

## Parent PRD

[Approved PRD](prd.md)

## What to build

Rehearse the full workflow inside Compose and document Windows deployment for the owner.

## Acceptance criteria

- [ ] From a clean checkout, build and verify upload, finalization, resume, and link expiry before the coordinator commits and pushes the tested revision.
- [ ] The Windows laptop owner checks out the tested revision and verifies unattended Docker and tunnel startup, plugged-in sleep and lid settings, and persistent storage after reboot.
- [ ] Document recovery steps, verify the host can be operated without committing secrets, and record the tested revision and Windows setup.

## Blocked by

- [001](001-docker-pilot-build.md)
- [006](006-storage-lifecycle.md)

## User stories addressed

1, 3, 6, 7, 9, 13
