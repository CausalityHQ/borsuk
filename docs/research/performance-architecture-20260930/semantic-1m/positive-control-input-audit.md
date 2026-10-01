The raw ReLAION source is not on this machine, and canonical.bin cannot stand in for it. It can be regenerated from an authenticated S3 parquet. That is one root-authorized GET of 145 MB, and it is the only blocker. I did no hashing, no builds, no cloud calls and no vector, query or truth reads. Sizes come from `stat`, and hashes come from committed receipts.

## Why canonical.bin is not a substitute
- **Plane is bound to raw.** The plane builder encodes the raw rows (`two_bit_source.rs:174-186` and `:275-283`). It takes the mean over raw rows and checks the raw SHA at build time.
- **canonical.bin is normalized.** It holds `cosine_vector(raw row)` per physical position (`canonical_source.rs write_canonical_source`). The row norms are gone, so raw cannot be recovered. Reconstruction would never produce `0d55a097…`, and the plane `mean.bin` and `records.bin` would differ.
- **No ids.i64 exists in this chain.** The `source.f32`+`ids.i64` pair is only the output of `prepare_two_bit_compaction`. Here the IDs are the SQ8 IDs, which equal `order[p]`, which are raw ordinals. Nothing needs fabricating.

## Authority chain (ReLAION FIRST100k)

| Item | Identity | Producer / location |
|---|---|---|
| **raw** (307,200,000 B) | `0d55a09756f4…758c2` | **Missing locally.** Regenerate from S3 `research/v85-pq16-page-nomination/24383d853474a19702d18d2de700bee3618167f5/100k-a0023/attempt/inputs/source-100k.parquet` (145,121,661 B, `a199e151…`). Use the `item['name']=='relaion'` block in `scripts/run_native_source_precision_http.py`: column `embedding`, first 100,000 rows, `<f4`. Accept only if the SHA is `0d55a097…`. |
| raw, corroboration | same SHA | `source-fit-cost-20260928/verification.json` (a199e151 → 0d55a097, normalized `4b9605bd…`). The Spark `vectors.raw` is in `native-relaion-terminal-receipt.json`, but its host path is not local. |
| **order.u64** (800,000 B) | `22abeb08…ab18` | Local in `/tmp/borsuk-semantic-router-inputs/relaion/` and in paired a0002. Made by `hier-fit` on the normalized source, recipe `chacha8-v3`, in the locality-layout a0001 run. Meaning: LEu64 physical → logical (`semantic-router-row-identity.json`). **Pin it; do not refit.** |
| **sq8.bin** (78,000,000 B) | `be4b19a8…e3b` | Local. Records are 780 B: i64 id, f32 norm², 768 u8. |
| low/step | `binding.json`, 17,287 B, `75d5e2d8…` | Local. |
| requests | parent `b2485629…` (1000 lines), first64 `1f94514c…`, 1,132,707 B | `/tmp/borsuk-semantic-native-cold-inputs-a0001/relaion/requests` |
| truth | parent `4bd3ac79…`, first64 `3ad233f3…`, 25,600 B, 64×100 LEu32 | same directory |
| canonical.bin | `c260b7fe…` | A build output. It is local, and it is the parity target below. |

**Cosine contract:** keep requests raw. `diagnostic_search_with_store` normalizes queries itself (`normalize_two_bit_diagnostic_query`, which calls `cosine_vector`). Pre-normalizing would apply it twice, and bitwise idempotence is not guaranteed.

## Minimal preparation plan
1. **Raw.** Regenerate with the code above and check the SHA. If it fails, stop.
2. **Truth.** Widen the first64 `truth` to LEi64 (zero-extend each u32). That gives 51,200 B, and the scorer's required size is 64×100×8. Check every id is below 100,000 and unique per row.
3. **Requests.** Write JSONL with keys `ordinal` and `query` only. The v2 `Request` uses `deny_unknown_fields`, and the historical `query_ordinal` key would be rejected (as would any extra keys). Check each f32 equals the first64 file bitwise. These are new files with new hashes; don't overwrite the cold-harness inputs.
4. **SQ8 and ETag.** Copy `sq8.bin` into `store_root` at the key the manifest names. The builder config's `sq8_etag` must be the local file store's ETag, not the historical S3 value `"232d675e…"`. So stage the SQ8 first, read its ETag, then freeze the builder config.
5. **Build.** Run `build_two_bit_generation` from 5075366d with a Native100k semantic profile and `order={order.u64, 22abeb08…}`. Confirm the profile name against the 5075366d source before freezing.
6. **Stage.** No existing CLI publishes to a local file store. `two_bit_plan_demo` is S3-only. Call `publish_two_bit_generation(LocalFileSystem, prefix, local, root_sha, limits, None)`, as the existing tests do. Copy `canonical.bin` to the `canonical.object_key` in the built manifest; read that key from the manifest rather than guessing it.
7. **Config.** Write the scorer v2 config: `rows=100000`, `dimensions=768`, `first=0`, `count=64`, `max_memory_bytes=536870912`, and `generation_root_sha256` set to the new root.

## Smallest parity checks (no scorer needed for the first two)
- **A, byte identity.** The rebuilt `plane/mean.bin` must be `197a8ead…`, `plane/records.bin` `624e63de…`, canonical `c260b7fe…`, `page_digests.bin` `2195b3fa…`, and `plane/page_digests.bin` `afa60c15…`. A mismatch means the input chain is broken, not a recall result.
- **B, truth binding.** Score the committed historical reference records against the widened truth: `/tmp/borsuk-semantic-native-cold-inputs-a0001/relaion/{candidate,control}-reference-records.jsonl`, SHAs `738aba5b…` and `87ac5731…`. Expect candidate 628 and control 638 of 640 R10 hits, matching the historical scorer verification. This is a check on the harness inputs only. I did not open those files, so confirm their field names first.
- **C, v8 scorer.** Report v8 hits10 as a new measurement. Cite 628/640 only as an informational v7 reference, with the router-format difference disclosed (JSON router vs v8 binary root). It is not a floor.

## Falsifier for the harness wiring
Rerun the scorer with a deranged truth, for example ids shifted by +1 mod 100,000. R10 must fall far below 608. If it doesn't, truth is not actually bound to the order. This is a cheap offline run over the same 64 requests.

## Risks
- Step 6 is the least defined part. Confirm the local-store ETag and `canonical.object_key` handling against the 5075366d staging tests before the root freezes the protocol.
- The S3 GET touches cloud storage, so the root has to authorize it. It is a plain download on the devbox, not benchmark compute. AWS credentials are present locally.
- The only ~307 MB local file is `.borsuk-scratch/v283/cohere-vectors.raw`. That is CoHere, not ReLAION; do not use it.
