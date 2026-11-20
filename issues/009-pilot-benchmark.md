# Score verified cross-border delivery

Type: HITL

Recommended subagent: `gpt-6.2-astra` / `high`

## Parent PRD

[Approved PRD](prd.md)

## What to build

Run the scored benchmark and record which routes are approved.

## Acceptance criteria

- [ ] Run two clean 10 GB attempts per direction in the agreed windows. For China-to-US, verify the intended recipient’s agreed pickup. For US-to-China, verify mainland browser pickup with VPN off. Run one 12 GB China-to-US transfer with laptop pickup.
- [ ] Exercise disconnect, browser restart, Windows service restart, lost finalization response, remote pickup and expiry, and range recovery. Record the tested client. Use controlled local fault tests for disk exhaustion.
- [ ] Record upload start to verified receipt, upload, finalization and remote download durations, recovery time, payload bytes resent, all three SHA-256 checks, and incremental cost per completion.
- [ ] Compare a working fallback at a common file size and record WeTransfer availability separately. Keep 5 GB in 15 minutes as an upload benchmark. Score against pre-agreed ceilings and restrict pilot use to passing routes.

## Blocked by

- [008](008-route-smoke-test.md)

## User stories addressed

1, 2, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14
