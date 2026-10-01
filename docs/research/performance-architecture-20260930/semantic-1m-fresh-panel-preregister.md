# Fresh ReLAION 1M semantic development panel

Frozen before selecting or decoding new queries. This extends the existing quality-blind ranking; it does not reopen the old confirmation panel. No new query bodies, truth, quality or latency have been measured.

## Selection

Use the authenticated registry/population/physical-ID overlap authorities from `scripts/select_v36_rank16_fresh_ids.py`. Preserve object ranks16–31, exclusion of every original ranks0–15 physical ID, first occurrence by rank/offset, seed `borsuk-v36-rank16-fresh-v1`, ascending SHA256(seed + little-endian u64 ID), and ID tie order.

Select the first1064 reservoir entries using IDs only. Require entries0–999 to reproduce every ID, source rank, offset and selector hash in the pinned `fresh-rank16-provisional-ids.json`. Those entries were consumed by development0–63 and confirmation64–999 and cannot qualify the new arm.

Reservoir1000–1063 become new query ordinals0–63, retaining `reservoir_ordinal` and source locators. Do not decode a successor or confirmation reservoir. Fail the whole panel on selection mismatch, repeated IDs, malformed geometry, or raw/unit-vector duplicates within the panel, against indexed FIRST1M source, or against the old consumed rank16 query bodies. Do not replace individual queries. Report the exact audited populations; complete historical vector-audit coverage remains unproven.

## Seal and truth

Authenticate the indexed source parquet/raw identities from `fresh-rank16-seal-config.json` and the consumed query-object identity from its closed verification. Re-read selected IDs at authenticated locators. Use original finite nonzero f32 D768 queries and the existing exhaustive f64-normalized cosine block-sort/top100 merge oracle in `run_native_source_frontier_1m.py`, with ascending source-ordinal ties. Run its existing self-check. Preserve source-ordinal truth and explicitly authenticate its mapping to production application IDs/source order.

Outputs: 196608-byte `queries.raw`, 25600-byte little-endian `truth.u32`, 64-row requests JSONL, panel/config/code identities, duplicate-audit receipt, oracle receipt and terminal/resource evidence. Seal to a new prefix with conditional PUT and authenticate object size/hash by readback. No ANN result is inspected during preparation. Historical artifacts remain immutable. Never let the old helper unlink shared cached shards; use owned scratch copies or links.

## Execution and next gate

Preparation is a separately bounded remote Spot campaign using the existing lifecycle, with exact ACK ownership, terminal upload, terminate-and-wait before collection, and no duplicate job. Initial ceiling:8GiB cgroup/no swap,2CPU,3600s service wall, compute cap$0.60 plus$0.15 EBS/S3 allowance. These are ceilings, not measured costs or predictions. Freeze config/source/code and actual quote before launch; no preparation starts during the local compiler gate.

The new Fresh1m library first needs affected tests, release binaries, workspace Clippy/test compilation and one remote changed-native full execution. Freeze exact generation, original centroid, router, source/SQ8, query, truth and source-order identities before the ReLAION-first falsifier. Require64 complete valid results and at least608/640 recall@10 hits; report recall@100 separately. Preserve the declared nomination/scoring/read caps and decompose nomination, page/source coverage and quantization loss. Record build wall/CPU, actual RSS and payload bounds separately. A scientific failure ends this arm without retuning against this panel. CoHere follows only after survival, with separately preregistered new queries.

This preparation establishes fresh development evidence, not vendor equivalence, sustainable throughput,1M cold latency or100M feasibility. Published1M D768 coldp90444ms is later comparison context under disclosed differences; unknown vendorp95/QPS remain unknown.
