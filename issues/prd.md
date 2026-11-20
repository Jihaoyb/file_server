# NebulaFS browser transfer pilot

Status: Draft for review

## Problem Statement

A small group needs to send large files between mainland China and the US without a VPN, primarily from China to the US. Most files are 1–5 GB. The workflow must handle files larger than 10 GB and recover from interruptions without restarting the whole transfer. WeTransfer is the current reference where usable. The pilot budget is $0 in additional spend beyond the domain already purchased.

China-to-US delivery ends when the intended recipient retrieves and verifies the file on the US laptop or remotely, using the pickup method agreed for that run. US-to-China delivery ends when the mainland recipient downloads and verifies the file with VPN off. Measure from upload start to verified receipt. Record upload, finalization, and any remote download separately.

## Solution

Host NebulaFS on an existing US Windows laptop. Use a stable HTTPS subdomain for browser uploads and expiring bearer download links. The recipient can collect a finished file on the host or download it on another device. The sender can go offline after upload completion. The laptop and its connection must stay available for remote pickup.

## User Stories

1. As a sender in China, I want to open the upload page in my normal browser, so that I can send without installing software or using a VPN.
2. As a sender in the US, I want the same workflow in reverse, so that I can send to a recipient in China.
3. As a sender, I want private upload access, so that the host owner controls who can use their storage.
4. As a sender, I want to select a file larger than 10 GB, so that I can send it in one workflow.
5. As a sender, I want visible progress and retry status, so that I know whether my upload is moving.
6. As a sender, I want interrupted requests to retry, so that a brief connection drop does not stop the transfer.
7. As a sender, I want to reopen the page and reselect the original file, so that I can recover after closing or restarting my browser.
8. As a sender, I want confirmation after the host finalizes and verifies the file, so that I know when I can go offline.
9. As a recipient, I want to collect the file on the laptop or open a private link from another device, so that I can pick it up where needed.
10. As a recipient, I want interrupted downloads to support range requests, so that a compatible client can resume from saved bytes.
11. As a recipient, I want the source, finalized host file, and received copy to match the same SHA-256 during pilot runs, so that I can verify delivery.
12. As a host owner, I want expired links and unused upload parts cleaned up according to policy, so that access and disk usage stay bounded.
13. As a sender, I want to recover a completed upload after losing the finalization response, so that I can share the same result without uploading again.
14. As a host owner, I want clear errors when storage is insufficient or the upload cannot continue, so that I can resolve the problem without mistaking a partial file for a finished one.

## Implementation Decisions

- Prefer a named Cloudflare Tunnel to the Windows laptop. It uses an outbound connection and avoids inbound port forwarding. Test the final hostname and transfer path from both networks before relying on them.
- Reuse NebulaFS multipart initiation, part upload, part listing, and finalization. Reuse HTTP byte-range downloads. Existing coverage includes small localhost cases. Cross-border throughput and 10 GB recovery remain unverified.
- Persist acknowledged part data and metadata before confirming receipt. Both must survive a Windows service restart. On browser reopen, reconcile with the server and verify the reselected file against the original full-file SHA-256 before resuming. Reject a different file even if name and size match.
- Make finalization idempotent by upload ID. Persist the completed result through the file retention period so an authorized sender can recover the same object and completion details after a lost response or restart. The single-node handler currently deletes the upload record before returning success. Provide an authenticated status endpoint if assembly can outlast a proxy request.
- Publish the file and issue a link only after successful writes, flush, close, and a streamed SHA-256 readback of the finalized host file against the source digest. The current assembly hash covers input bytes. An assembly failure must leave no downloadable partial object.
- Start with 8 MiB parts. Cloudflare Free caps proxied uploads at 100 MB per request. Tune part size and concurrency using sustained throughput, retry rate, and bytes resent. Measure finalization separately and validate proxy timeouts. [Upload limit](https://developers.cloudflare.com/support/troubleshooting/http-status-codes/4xx-client-error/error-413/) · [Proxy timeouts](https://developers.cloudflare.com/fundamentals/reference/connection-limits/)
- Restrict uploads to invited users. Anyone holding a valid expiring bearer link can download the file, including someone who receives it by forwarding. Files are readable on the host.
- Proposed pilot limits are 12 GB per file, one active upload, 24 hours of upload inactivity before session expiry, a 24-hour download link lifetime, and seven days of file retention from verified completion. A link cannot outlive file retention.
- Allow roughly 24 GB for staging and assembly of a 12 GB file, plus retained files and a proposed 10 GB free-space reserve. Admit an upload only if its staging and assembly fit while preserving the reserve. Keep staged parts and metadata on persistent Docker storage.
- Assign the US Windows laptop owner as pilot operator. Document unattended startup for Docker and the named tunnel, plugged-in sleep and lid settings, and recovery steps after a Windows reboot. Verify the stack starts and acknowledged parts remain available after reboot.
- Direct P2P requires overlapping online time and can need a relay if NAT traversal fails. Managed storage allows pickup while the laptop is off but can add storage and bandwidth fees. Revisit a managed fallback if the laptop route misses reliability or speed needs.

## Testing Decisions

### Gate 1 · Route smoke test

Timebox initial setup to one hour. Use the actual Windows host and final HTTPS hostname, `send.nebulafs.app`. With VPN off on the mainland connection, upload 250 MB using multipart requests and retrieve it across the route. Compare checksums and record elapsed time and throughput. If setup takes the hour, leave the gate pending.

### Gate 2 · Pilot approval

- Use the same real 10 GB file in both directions. Before scoring, agree on two clean-run windows per direction and the pickup method for China-to-US. Proposed windows are one mainland daytime and one mainland evening window. Proposed pass count is two verified clean runs out of two per direction.
- Proposed 10 GB delivery ceilings are 60 minutes for China-to-US with laptop pickup and 90 minutes for US-to-China with mainland browser pickup. If China-to-US uses remote pickup, agree on its ceiling separately before scoring. Confirm ceilings after the smoke test and before any scored run. Keep 5 GB in 15 minutes as an upload-only benchmark, which requires about 44.4 Mbps of sustained payload throughput.
- Require one verified 12 GB run from China to the US with pickup on the Windows laptop.
- Log time from upload start to verified receipt, upload time, finalization time, remote download time where applicable, recovery time, payload bytes resent, SHA-256, and incremental cost per completed transfer. Compare against a working fallback at a file size both workflows support. Record WeTransfer availability separately.

### Acceptance Criteria

- China-to-US passes with verified local or remote pickup by the intended US recipient, using the agreed method. US-to-China passes when the mainland recipient completes the download and verifies it with VPN off. Each verified pilot run requires the same SHA-256 for source, finalized host file, and recipient copy.
- Interrupt the connection and restart the browser during upload. Reconcile saved parts with the server and resume without resending acknowledged parts. Reject a different file with identical name and size. Restart the Windows service during upload and verify acknowledged parts survive.
- Drop the finalization response after the host commits completion. Retry or query status and retrieve the same completed result without reuploading parts or creating a second file.
- Exhaust disk space during part upload and separately during assembly. Verify that incomplete or unverified data stays unavailable and no link is issued. After restoring space, verify recovery with acknowledged parts intact.
- Verify range-based download recovery with the supported browser or client and record what was used. Open a bearer link on another device after the sender goes offline. Verify expiry and file retention behavior.
- Score each direction against its agreed pass count and delivery-time ceiling. Mark a direction that misses its gate as unvalidated. Limit pilot use to routes that passed.

## Out of Scope

Desktop clients, end-to-end encryption, public signup, direct P2P, paid relay, and managed storage fallback.

## Further Notes

Open decisions include weekly transfer count, supported browsers, time windows, scoring ceilings, and China-to-US pickup method for the 10 GB runs. The proposed limits and host owner assignment above are for review. The temporary tunnel page was reachable from the mainland test connection. Final-domain access, Windows deployment, sustained speed, and restart behavior still need verification.
