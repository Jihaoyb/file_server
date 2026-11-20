# Pass the 250 MB final-hostname smoke test

Type: HITL

Recommended subagent: `gpt-6.2-sol` / `medium`

## Parent PRD

[Approved PRD](prd.md)

## What to build

Validate send.nebulafs.app through the named tunnel to the actual Windows laptop.

## Acceptance criteria

- [ ] Timebox the initial setup attempt to one hour and record any blocker that leaves the gate pending.
- [ ] On the normal mainland connection with VPN off, upload 250 MB using multipart and retrieve it over the route. Verify SHA-256 and record timing and throughput.
- [ ] Agree on mainland daytime and evening windows, China-to-US pickup method, direction-specific ceilings, and two required clean passes per direction before scoring.

## Blocked by

- [007](007-windows-rehearsal.md)

## User stories addressed

1, 4, 8, 9, 11
