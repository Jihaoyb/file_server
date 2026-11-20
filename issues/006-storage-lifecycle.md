# Enforce pilot storage limits and cleanup

Type: AFK

Recommended subagent: `gpt-6.2-astra` / `high`

## Parent PRD

[Approved PRD](prd.md)

## What to build

Enforce the pilot admission and cleanup rules on durable uploads and shared files.

## Acceptance criteria

- [ ] Allow one active upload even with concurrent admission requests. Enforce the 12 GB maximum and a 10 GB reserve beyond projected staging and assembly demand and existing disk use.
- [ ] Expire sessions after 24 hours of inactivity and delete completed files after seven days. Protect live uploads and finalization from cleanup. Keep completion results recoverable while files are retained.
- [ ] Inject disk exhaustion during part upload and assembly. Return an actionable error, leave no downloadable partial data, and resume after restoring space.

## Blocked by

- [004](004-resume-upload.md)
- [005](005-bearer-download.md)

## User stories addressed

3, 4, 12, 13, 14
