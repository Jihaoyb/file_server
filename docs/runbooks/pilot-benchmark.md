# NebulaFS pilot benchmark

Run on the deployed Windows host at `https://send.nebulafs.app`. Use the sender’s and recipient’s normal networks and turn off VPNs. Keep one transfer active at a time. Use files cleared for testing and keep bearer URLs out of the results.

## Route smoke test

1. Record the deployed commit, Windows and browser versions, time zone, pickup method, and network type on both sides.
2. Open the final hostname from mainland China. Upload a 250 MB file from the browser and retrieve it across the route.
3. Compare the source, finalized host file, and recipient SHA-256. Record elapsed time and payload throughput. Stop and record the blocker if the first-hour setup or smoke test cannot finish.

## Scored pilot

Before scoring, record the agreed mainland daytime and evening windows, pickup method, delivery ceiling, and required pass count for each direction.

| Direction | Size | Pickup | Required runs | Delivery ceiling |
|---|---:|---|---|---|
| Mainland China to US | 10 GB | Agree before scoring | Two clean passes | Confirm. Proposed 60 min for laptop pickup |
| US to mainland China | 10 GB | Mainland browser with VPN off | Two clean passes | Confirm. Proposed 90 min |
| Mainland China to US | 12 GB | US Windows laptop | One verified pass | Record elapsed time |

Run each clean test from upload start through verified receipt. Record upload and finalization separately. Include local pickup or remote download and checksum verification in the elapsed delivery time. Keep 5 GB in 15 minutes as a separate upload benchmark.

Run recovery checks separately. Disconnect during upload, reopen the browser and reselect the source file, and restart the Windows service with acknowledged parts on disk. Test a different file with the same name and size. Verify that losing the finalization response returns the original completed result. Check remote pickup after the sender goes offline, expiry, and download resume. Exercise disk exhaustion on an isolated local test volume.

For each run, record:

- Run ID, date, time window, direction, source bytes, and pickup method
- Upload start, last part acknowledgment, server finalization completion, remote download start and finish if applicable, and receipt verification time
- Recovery time, payload bytes resent, and the three SHA-256 values
- Incremental spend attributable to the run, including failed attempts in the same scenario, and cost per completed transfer
- Verified pass, failure, or unavailable, with the reason

On Windows, use `Get-FileHash -Algorithm SHA256 -LiteralPath <path>` on each copy available there. On macOS, use `shasum -a 256 <path>`. The host copy means the finalized bytes on persistent storage. Record the host readback digest for that copy.

Run a working fallback on the same connection and at a file size that both workflows accept. Record WeTransfer availability separately. Mark any direction that misses its gate as unvalidated.
