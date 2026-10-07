# Critique: retained S3 publication admission a0001 (read-only)

**Verdict:** I found no correctness blocker in the publisher code. The binary can only create new objects under the destination, it puts the head last and only if absent, and it fails closed if anything changed. Six small fixes are needed before freeze, all in user-data, the preregistration, or the freeze procedure. None of them touches Rust, so no requalification is needed.

## What I checked and found sound
- **Hashes and syntax:** config `b2688d80…`, stage `8acd3607…` and user-data `e322c165…` match the preregistration, and both scripts pass `bash -n`. The binary in the qualification evidence hashes to `85ca006a…`. There are no differences in `crates/`, `Cargo.toml` or `Cargo.lock` between `4c2e535f` and `910b8b09`.
- **Create-only on real S3:** object_store 0.14.1 sends `If-None-Match: *` for `PutMode::Create`, because `S3ConditionalPut::ETagMatch` is the default. Create-mode copy is a multipart upload whose completion also carries `If-None-Match: *`.
- **Credentials and TLS:** `metadata_options()` sets `allow_http=true`, and `NativeConnector` honours it, so the HTTPS-only guard does not block the instance-metadata endpoint. TLS is rustls using the system certificates in `/etc/ssl/certs`. The binary links only libc, libm and libgcc_s.
- **No path to modify the source:**
  - Destination and source are checked to be disjoint, and the canonical and SQ8 keys must lie outside the destination.
  - Cleanup deletes only objects whose creation was confirmed, and stops once the head write has been attempted (`two_bit_store.rs:1127`).
  - Copies are not conditioned on the source version, but every copied body is re-hashed in full. All source and destination metadata is checked again before the head write.
- **Reads and timing:** bodies stream in chunks of at most 1 MiB. Total reads are about 690 MB, because SQ8 is read twice (same key, only the ETag changes). The 410 MB canonical object needs at least 1.4 MB/s to finish inside the 300 s request timeout, which in-region traffic easily exceeds.
- **Separate publication canary:** not needed. Every failure either happens before any write, before the head write (with cleanup), or burns only this attempt's destination prefix.

## Required smallest fixes
1. **Worst-case run time exceeds the shutdown timer.**
   - Worst case adds up to about 1890 s: apt (210 + 330), curl 180, unzip/install (150 + 150), the native unit (600 + systemd's default 90 s stop timeout), and the finish uploads (about 180).
   - The timer at `user-data.sh:3` shuts the instance down at 1800 s, so it can cut off the `terminal.json` upload.
   - Fix: add `(( SECONDS <= 900 )) || exit 95` just before `phase=native`. A slow bootstrap then fails before any S3 write. Optionally add `-p TimeoutStopSec=10`.
2. **The OS check was removed and the launch request is not pinned.**
   - `prepare.py` cut everything from `phase=source` to `phase=native`, which removed the `uname -m` and `/etc/os-release` (Ubuntu 24.04) checks.
   - The binary needs GLIBC 2.38, so an Ubuntu 22.04 image would fail.
   - Nothing pins the AMI, instance type, Spot settings, shutdown behaviour or metadata-token setting for this attempt.
   - Fix: put the launch JSON in the preregistration (qualification used `ami-0b8a830d6339a9758`, tokens required, shutdown terminates, one-time Spot that terminates on interruption). Restore the three check lines.
3. **Write the outcome table into the preregistration before launch,** so outcomes can't be judged after the fact:
   - **PASS:** native exit 0, final exit 0, terminal exit 0, and the receipt is present. The root then checks independently that:
     - the destination `head.json` points to the same root SHA as the receipt;
     - the destination contains exactly the 10 generation objects plus `head.json`;
     - there are no open multipart uploads;
     - the source head and SQ8 ETags are unchanged;
     - `oom_kill` is 0.
   - **INVALID (prefix burned):** native exit non-zero and no head in the destination.
   - **INVALID-COMMITTED (not adopted, prefix burned):** a head exists but anything else failed, including a final exit of 96 or a failed evidence upload.
   - **Execution or environment INVALID, not a rejection:** a Spot interruption, the 600 s timeout, or the library's limits rejecting the real data size. The next attempt uses a new destination prefix.
4. **After the run, "check" multipart uploads means list, record, abort, then list again.** object_store aborts only on a handled error; its own comment says it "cannot protect against an ill-timed process crash". The timeout's SIGTERM kills the publisher without cleanup, and the bucket has no lifecycle rule.
5. **Check every source key's metadata before freeze, not just SQ8.** The supplied evidence observes only SQ8, `head.json` and the manifest. Nothing shows that the 410,400,000-byte canonical object or the 9 sidecars exist with the expected sizes. This check only avoids wasting the single attempt; correctness doesn't depend on it, because the binary re-checks every key at run time (for example the SQ8 ETag at `two_bit_store.rs:922`).
6. **Commit the freeze through a separate index file.** This worktree's default git index has about 6.7k lines of staged deletions under `crates/`, including `publish_two_bit_generation.rs`. A plain `git commit` would delete the qualified source. Use a separate index, as `protocol.index` and `integration.index` did, and assert `git diff --quiet 910b8b09 <freeze> -- crates Cargo.toml Cargo.lock` before pushing.

## Recommended, not blocking
- **Record the native timeout explicitly.** In `stage.sh`, wrap the publisher in `timeout --kill-after=10 570`. Today the 600 s kill also kills the wrapping bash, so `native-exit` and the after-run cgroup metrics are never written. `systemd-after.txt` doesn't fill the gap: on success the unit is already gone, and the qualification run shows `LoadState=not-found`, so its `Result=success` is a default rather than an observation.
- **Make the evidence uploads create-only.** `aws s3 cp` overwrites existing objects. Either use `s3api put-object --if-none-match '*'` or check before launch that the run prefix's three keys don't exist yet.
- **Label the 64 MiB scratch cap honestly.** Only the library enforces it, in a plain directory on the root disk. Say so in the preregistration, or mount a 64M tmpfs. The leftover check `find -type f` misses empty directories; use `-mindepth 1`.
- **Fix two naming issues:**
  - The destination is called `…-smoke-a0001` but will hold the actually admitted generation.
  - The preregistration says `source_commit: 910b8b09` while `terminal.json` says `4c2e535f`. Say "binary built from 4c2e535f, crates identical to 910b8b09".

## Future limitations (no action now)
- The instance role has `AmazonS3FullAccess` across the whole account.
- The receipt contains no transport counters, so request and byte counts for the publication are not observed.
- The library's 512 MiB memory limit equals the cgroup limit, leaving nothing reserved for runtime, TLS or allocator overhead. This run gives the first real `memory.peak`.
- The new prefix lives inside the historical 20261006 a0002 namespace. Integrity audits of a0002 should exclude it.
- The shared canonical and SQ8 objects have no versioning or object lock. If they were overwritten, the generation would become unusable (reads fail on ETag), not silently wrong.

I saved this review to memory as `retained-s3-publication-admission-review-a0001`. I made no repository edits, launched no jobs, made no network calls and read no corpus data.
