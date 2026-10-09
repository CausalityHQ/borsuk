What this change does: it binds preparation and the native chain to authenticated support assets and fresh scratch storage, then preserves opaque evidence for collection after termination. I reviewed the exact `92bbe8fc582b903dd003dd24ae2d98e469dd6a57` blobs against base `890933b6…` and the full v3 contract.

**Verdict: fix finding 1 before GO to the bounded disposable composition canary.** This review grants no science or launch authorization.

All source lines below refer to `runtime-support.pending/` at that commit.

1. **[P1 — blocker] Watcher errors can bypass termination.**
   `watch-original.sh:9` only translates unexpected exits to 19. If `remote-results` already exists (`:17`), or writing `watcher-reason.txt` fails (`:35`), execution exits before the termination loop at `:44–54`. The original instance then loses this external deadline guard. The guest shutdown timer is not confirmation of EC2 termination.
   **Minimal fix:** initialize and validate the deadlines before filesystem setup, then arm bounded failure cleanup for the exact original instance. Cleanup must remain possible when result-file writes fail, preserve existing receipts, and report unconfirmed termination explicitly.

2. **[P2 — contract mismatch] The binding wait can accept a result after its 900-second deadline.**
   `user-data.sh:110–114` checks time only before two independently timed GETs. An iteration starting at second 899 can complete both requests after second 900, break, and proceed toward formatting.
   **Minimal fix:** clamp each GET and sleep to the remaining window and reject success received after the deadline. The later preparation admission still protects its separate remaining-time budget; this finding concerns the claimed setup deadline.

3. **[P2 — contract mismatch] Manager-proof waiting can take roughly six minutes.**
   `watch-original.sh:38–40` permits twelve calls, each potentially taking 25 seconds plus kill grace, with intervening sleeps. That is substantially longer than the contract’s stated ≤60 seconds.
   **Minimal fix:** use an absolute proof deadline of `min(hard_stop, now + 60)` and clamp calls and sleeps to it. The overall external deadline remains bounded on this path.

The highest-priority **runtime canary question** is exit propagation. `user-data.sh:103` attaches attestation to **cloud-final**, while `watch-original.sh:75–77` requires its manager exit to equal the nested bootstrap’s 0/2/3. These are distinct process exits. Exercise all three outcomes through the actual target cloud-init/systemd stack. If that stack maps a nested failure to a different status, the watcher rejects an otherwise closed baseline disposition. Preserve separate actual exit receipts; do not simply relax the equality check.

The main source safeguards otherwise align with the contract:

- Exact checksum-companion bytes precede binding parsing; unique NVMe serial, size, signature, partition and mount checks precede formatting (`user-data.sh:114–137`).
- Root admission includes transport bytes plus 1.5 GiB (`:139–145`).
- Eight support files and seventeen transported assets are authenticated; preparation closure and five seals precede two-field config finalization (`:92–101`, `:149–188`).
- Glue exits remain separate from admitted baseline dispositions; opaque evidence uses create-only uploads.
- Collection follows confirmed termination. At $0.59/hour, 15,120 seconds models **$2.478**; enforcing the bid and authenticating the launch timestamp remain caller responsibilities.
- The Git blob is 15,933 bytes; recheck the actual frozen bytes against 16,384.

The unexecuted 91-line wrapper canary does not establish composition coverage. Actual storage binding, service cleanup, manager exits, immutable publication and termination failures remain unverified.

No files were edited; no tests, network calls, native execution or paid jobs were run. Historical jobs and receipts were untouched.
