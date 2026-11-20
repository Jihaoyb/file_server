# Finalize once and recover the verified result

Type: AFK

Recommended subagent: `gpt-6.2-astra` / `xhigh`

## Parent PRD

[Approved PRD](prd.md)

## What to build

Make the single-node completion flow recoverable by upload ID and gate publication on the source SHA-256.

## Acceptance criteria

- [ ] Check writes, flush and close, then read back the assembled output and compare a streamed SHA-256 before publishing.
- [ ] Persist completion state and object identity through file retention. Repeated or concurrent completion and restart recovery return the same result.
- [ ] Inject response loss after commit and disk exhaustion during assembly. Verify no duplicate, no published partial file, and recovery after restoring space.
- [ ] Provide authenticated status polling and observable finalization timing. Verify completion that outlasts a proxy request.

## Blocked by

None. Can start immediately.

## User stories addressed

8, 11, 13, 14
