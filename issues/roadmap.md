# NebulaFS implementation roadmap

[Approved PRD](prd.md). Each issue has its own model, effort, dependencies, and acceptance criteria. AFK issues can proceed independently once their blockers clear. HITL issues need the Windows owner or a mainland tester.

| Phase | Suggested lead model and effort | Issues and parallel work | Exit gate |
|---|---|---|---|
| 1. Local transfer foundation | `gpt-6.2-astra` / `high` | Start [001 Docker](001-docker-pilot-build.md), [002 browser upload](002-browser-upload.md), and [003 verified finalization](003-verified-finalization.md) in parallel. Agree on the multipart and status response contract first. | Clean Compose boot and a local browser upload that returns one verified result. |
| 2. Recovery and sharing | `gpt-6.2-astra` / `xhigh` | Start [004 upload resume](004-resume-upload.md) after 002 and 003. [005 bearer downloads](005-bearer-download.md) can start after 003, even while 002 is underway. Merge both before [006 storage lifecycle](006-storage-lifecycle.md). | Recovery survives restart and lost responses, verified files are shareable, and expiry and disk limits pass. |
| 3. Windows deployment and route smoke | `gpt-6.2-sol` / `high` | Run [007 clean build and Windows reboot](007-windows-rehearsal.md), then [008 final-hostname smoke test](008-route-smoke-test.md). The owner can prep Docker, power settings, and the tunnel while Phase 2 runs. | A tested revision is pushed and running on Windows. Gate 1 passes and scored-run rules are agreed. |
| 4. Scored pilot | `gpt-6.2-astra` / `high` | Run [009 cross-border benchmark](009-pilot-benchmark.md). An agent can prepare the recording sheet and fallback comparison while Phase 3 runs. Run scored transfers serially to avoid competing for the test connection. | Gate 2 records approved routes, unvalidated routes, and measured results. |

## Implementation workflow

1. Assign one issue per subagent in an isolated checkout. Use the issue's model and effort at dispatch. The coordinator owns integration, the shared contracts, and the push. Suggested ownership is packaging for 001, browser upload for 002, and server finalization for 003. For 004 and 005, assign separate browser surfaces and agree on server route ownership. Serialize edits to shared route registration if overlap cannot be isolated.
2. Ask each agent to report the revision or diff, verification commands and results, and any remaining caveats. Review those against the issue acceptance criteria. Integrate in dependency order and rerun the combined workflow after each phase. Run a clean Docker build and a local Compose smoke test before pushing.
3. At the end of **each task**, have the agent write a short handoff. If that agent's active context is **above 20,000 tokens**, compact after the handoff and before assigning more work. Preserve the completed issue, revision, decisions, test evidence, and blockers. If the runtime exposes neither context size nor manual compaction, use a fresh agent with the handoff whenever the estimate is above 20,000 tokens or the size cannot be determined. Record that a fresh context was used.
4. After push, the owner deploys the exact tested revision on Windows. Keep the smoke and scored pilot gates separate. Record any missed direction as unvalidated.
