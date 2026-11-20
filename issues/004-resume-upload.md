# Resume browser upload after interruption

Type: AFK

Recommended subagent: `gpt-6.2-astra` / `high`

## Parent PRD

[Approved PRD](prd.md)

## What to build

Resume the original upload using durable server acknowledgments and browser recovery state.

## Acceptance criteria

- [ ] Persist part bytes and matching metadata before acknowledging. Verify survival across a Windows service restart during the pilot and a process restart locally.
- [ ] After network drop or browser restart, reconcile with the server and resend only missing parts. Reject a reselected file with the same name and size but a different full-file SHA-256.
- [ ] Handle a lost completion response through the status flow. Surface retry status and measure payload bytes resent.

## Blocked by

- [002](002-browser-upload.md)
- [003](003-verified-finalization.md)

## User stories addressed

6, 7, 8, 13, 14
