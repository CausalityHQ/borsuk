## Verdict on candidate `5ed52756`: not releasable as "retained publication through native S3"

Both files match the contract hashes (`6530d010…`, `30ab8113…`). I read the candidate blobs with `git show`. Nothing was run.

The safety design is correct: the S3 path cannot overwrite an object, and keys, the namespace and the quoted ETag are bound correctly. But one transport-level problem means the S3 path cannot publish the 10M target, and the planned 100k canary would pass anyway and hide it.

## Defects, most serious first

**1. Blocker at scale: the 30-second total request limit cuts off full-body S3 reads and large copies.**
- `one_attempt_builder` sets `reqwest .timeout(30s)` (`sq8_s3_range.rs` around line 225). In reqwest 0.13.4 that limit runs until the response body has finished (`async_impl/client.rs:1438-1441`, and `Response::new(…, total_timeout, …)` at 3116-3121).
- The retained publisher downloads and hashes every body through this transport in one request each (`authenticate_retained_body`):
  - the canonical object, `rows×(4D+8)`
  - the SQ8 object twice (`sq8_meta` and `current_sq8_meta`), `rows×(D+12)` each
  - `plane/records.bin` twice (source and copy), `rows×264`
- It also issues one UploadPartCopy per copied object, which falls under the same limit.
- At D1024:

| Rows | Canonical size | Required single-stream speed (30 s) | Outcome |
|---|---|---|---|
| 100k | 410 MB | ≈14 MB/s | passes |
| 1M | 4.1 GB | ≈137 MB/s | likely fails |
| 10M | 41 GB canonical, 10.4 GB SQ8 | far beyond one stream | certain to fail |

- So a 100k canary would "qualify" a publisher that cannot do the matched-10M run.
- **Minimum fix:** add a publisher-only constructor that uses `.read_timeout(30s)` (a per-read stall limit) and no total limit. Leave the query reader's `.timeout(30s)` unchanged so the qualified read behaviour is untouched.
- **Alternative:** before any I/O, refuse S3 objects above a declared byte ceiling, and record that ceiling in the receipt and docs.
- Either way, the canary needs at least one object large enough to take more than 30 seconds.

**2. The new transport test proves almost nothing.**
- `native_s3_create_copy_uses_conditional_multipart` only checks that the config string reads back as `"multipart"`.
- It does not exercise any of the project-specific risks:
  - the namespace appearing in `x-amz-copy-source`
  - a 412 still being recognised after `NativeHttp` throws away error bodies
  - the abort request being sent
  - no plain PUT ever reaching the destination
- **Smallest meaningful regression:** one scripted test on the existing HTTP fixture (around lines 870-932) that calls `copy_opts(Create)` through `PrefixStore("ns")`:
  - Scripted responses, in order: `200 InitiateMultipartUploadResult(U)`, `200 CopyPartResult`, `412 (empty)`, `204`.
  - Assert the result is `Err(AlreadyExists)`.
  - Assert exactly these four requests:
    1. `POST /fixture/ns/dst?uploads`
    2. `PUT …?partNumber=1&uploadId=U` with `x-amz-copy-source: fixture/ns/src`
    3. `POST …?uploadId=U` with `if-none-match: *`
    4. `DELETE …?uploadId=U`
  - Assert no request to the destination without an `uploadId`.
  - The fixture needs two small changes: one scripted response per request, and reading the request body by `Content-Length` (the Complete request has an XML body; closing the socket with unread data sends a reset).

**3. The S3 descriptor is less strict than claimed (low).**
- Only non-empty bucket and region are checked.
- `region` is inserted into the host name (`s3.<region>.amazonaws.com`), and `bucket` into the request path and the copy-source header.
- The config is SHA-pinned by the operator, so this is not exploitable, but the "strict descriptor" test name overstates it.
- **Fix:** a one-line character check, for example region `[a-z0-9-]+` and bucket `[a-z0-9.-]{3,63}`.

**4. The 5 GiB single-part copy limit is not admitted up front (low, beyond the 10M target).**
- One UploadPartCopy can copy at most 5 GiB. At D1024, `records.bin` passes that at about 20.3M rows.
- The failure would happen partway through, after earlier copies, leaving only best-effort cleanup.
- **Fix:** refuse any copied object over 5 GiB on the S3 backend before the first write, or document the limit.

**5. The receipt and scratch accounting are accurate but incomplete (low).**
- For S3, `local_file_only:false` and the `backend` echo are correct.
- **Optional addition:** record `remote.transport_stats()` taken after publication. As the runner plan already notes, those counts include the instance-credential (IMDS) requests.
- **Wording to fix:** on S3, `max_scratch_bytes` counts the remote destination copies as if they were local scratch. That over-charges, which is safe, but the docs should say so.

## Confirmed sound in the installed object_store 0.14.1 source

- **Namespace and keys:**
  - `PrefixStore::copy_opts` adds the namespace to both source and destination (`prefix.rs:193`).
  - The copy source is sent as `{bucket}/{encode_path(ns/from)}`.
  - `head`/`get_opts` strip the namespace, so the `meta.location == location` checks still hold.
- **No overwrite:**
  - A Create-mode copy is create-upload → part copy → complete with `If-None-Match:*`.
  - Status 412 becomes `Precondition`, then `AlreadyExists`. Status 409 becomes `AlreadyExists`. Both are decided by status code alone, so dropping the error body is harmless.
  - A 200 with an error in the body becomes an error (no retries), and an abort is attempted.
  - The head's `PutMode::Create` uses the default ETag-match conditional put, i.e. `If-None-Match:*`.
- **ETags:**
  - S3 returns the ETag with its quotes and the comparison against `approved.sq8_etag` is exact; a config value without quotes fails safely before any write.
  - Copied objects get new multipart-style `…-1` ETags. These are re-pinned by the full SHA check and the later head comparison; nothing assumes the ETag survives a copy.
- **A lost response cannot cause a wrong overwrite:**
  - If a copy or the manifest PUT commits but its response is lost, the object stays behind untracked, and the destination then counts as occupied, so the run refuses to continue.
  - The head is marked "attempted" before it is sent, so a lost head response never triggers cleanup.
  - Two concurrent publishers try the same keys in the same order, so the loser can only delete objects it created itself.
- **Reads are unaffected:** the copy-if-not-exists setting is only consulted for Create-mode copies (`mod.rs:352`).
- **Compilation:** I found no definite error. The deferred `let local; let remote;` borrows work; `json!` only borrows; the `Option<String>` type inference holds. Whether `deny_unknown_fields` on the tagged enum rejects the mixed configs can only be confirmed by running the new test.

## Missing evidence before an S3 run

- **Bucket lifecycle:** an AbortIncompleteMultipartUpload rule, since the abort is best-effort with no retries.
- **IAM:** `PutObject`, `GetObject`, `AbortMultipartUpload`, `ListBucket` and `DeleteObject`.
- **Credentials:** only the EC2 instance role via IMDS works, so the publisher's S3 path can only run on EC2.
- **Efficiency (optional):** when the manifest and approved SQ8 keys are the same object, its full body is hashed twice (+10.4 GB at 10M). The fix belongs in `two_bit_store.rs`, which is outside the two candidate files.
- **Source pins:** integrating this changes `sq8_s3_range.rs`, so the a0002 qualification's source pins describe the parent, not the integrated tree.
- **Index hygiene:** this worktree's git index has deletions staged, including `publish_two_bit_generation.rs`. Do not commit from that index.

**Required before calling this complete:** fix 1 (or an explicit ceiling admitted before any I/O) and the test from fix 2. The remote affected tests, Clippy and the workspace test build listed in the contract still have not run.
