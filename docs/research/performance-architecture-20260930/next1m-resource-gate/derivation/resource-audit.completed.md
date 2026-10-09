## NEXT1M generation/build/publication resource plan (read-only, HEAD 04bccf4e)

I sent nothing to the operator and launched no children. Every source read ran under the 1-CPU / CPU0 / 256 MiB / no-swap / pids 128 / 120 s scope.

**Recommendation:** make the next run one Spot instance that runs five sequential phases and stops at the first failing gate: regenerate prep → derive → stage SQ8 → build → publish to LocalFileSystem → local open plus Q32.

- Splitting it into a derive-only run and a later build run buys no extra independence. The a0001 corpus was deleted when that instance terminated, so a second run would have to repeat prep and derive.
- Each phase still gets its own receipt and its own success or failure verdict.
- Q1000 stays out of this run.
- Run only one native process at a time. Keep the unchanged 8 GiB, no-swap cgroup for every phase.
- Use a dedicated 40 GiB gp3 scratch volume (42,949,672,960 B). The filesystem size is then the hard disk limit.

### Exact sizes (D1024, 1,000,000 rows)
From the source formulas:
- units = 31,250; pages = 3,907; router centres = 489; router leaves L between 489 and 977.
- **Derive outputs** (`build_sq8_source.rs:642-645`):
  - `normalized.f32` 4,096,000,000
  - `order.u64` 8,000,000
  - `sq8.bin` 1,036,000,000
  - `derivation.json` ≤ 65,536
- **Generation directory G** (`two_bit_build.rs:163-172,598-717`, `two_bit_source.rs:265-272`):

| File | Bytes |
|---|---|
| `plane/records.bin` (264 B per row) | 264,000,000 |
| `plane/page_digests.bin` | 1,000,000 |
| `plane/mean.bin` | 4,096 |
| `canonical.bin` | 4,104,000,000 |
| `centroids.bin` | 64,000,032 |
| `router/root.bin` (512 + 4,160·L) | 2,034,752 to 4,064,832 |
| `router/membership.bin` | 125,000 |
| `router/leaves.bin` | 64,125,000 |
| `page_digests.bin` | 125,024 |
| 3 manifests (≤ 64 KiB each) | ≤ 196,608 |
| **G total** | **≤ 4,501,640,592** |

- **What publication copies** (`two_bit_store.rs:1124-1362`):
  - `canonical.bin` (4,104,000,000) plus 10 roster files (333,640,560) plus head/control objects (≤ 131,072): **4,437,771,632**.
  - `centroids.bin` is read for validation but **not uploaded**.
- **Router memory model** (`semantic_unit_router.rs:278-350`): I recomputed the training term as 506,729,928. It dominates and matches the pending ledger.

### Phase table
Disk is cumulative, keeps every file, assumes no hard-link savings, and charges every failure residue.

| Phase | Disk added | Peak disk | Settled disk | Memory (modelled vs measured) | Proposed deadline |
|---|---|---|---|---|---|
| P0 regenerate prep (frozen a0001 config) | shards 2,382,253,857 + outputs 4,297,013,278 | 13,653,621,025 (shards + already-admitted output cap 11,271,367,168) | 6,679,267,135 | modelled 2,893,086,880; **measured** RSS 113.5 MB in 72.44 s | keep the frozen deadline |
| P1 derive (`:395-475` model) | 2×source + 2×order + sq8 + 2×64 KiB + reserve = 9,311,239,936 | 15,990,507,071 | 11,819,332,671 | modelled payload 1,213,619,392 (wrapper 71,418,688 + fitting 1,142,200,704); RSS not measured | 3,600 s (model says 10–20 min; not measured) |
| P2 stage SQ8 into store | full copy 1,036,000,000 | 12,855,332,671 | same | about 0 | 300 s |
| P3 build | G ≤ 4,501,640,592; a failed partial stays ≤ G because files use create_new and a retry refuses an existing output | 17,356,973,263 | same | modelled 8,783,456 + 506,729,928 = 515,513,384, plus 16,000,000 while the order file is read (`bin:43`); RSS not measured | 2,700 s (random 4 KiB reads at 3,000 IOPS could take ~333 s worst case) |
| P4 publish | 4,437,771,632, plus ≤ 4,104,000,000 if a crash leaves a staging file | 21,794,744,895, or 25,898,744,895 with that residue | | router admission 506,729,928 inside the 512 MiB budget (~29.9 MB margin); router buffers 132,314,864; upload buffers 2×8 MiB | 1,800 s |
| P5 open + Q32 (`open_remote`) | scratch 5,515,560 (records and leaves are paged, not staged) + output cap 67,108,864 | 25,971,369,319 | | library cap fixed at 512 MiB (`check_cohere_native_baseline.rs:39,734`) | 900 s |
| Binaries, logs, receipts | reserve 1,073,741,824 (not measured) | **27,045,111,143 for the Q32 run** | | | |
| Q1000 (excluded from this run) | second full prep output set 4,301,243,196, or 4,369,750 if it reuses the authenticated corpus and adds only queries and truth | 31,346,354,339 | | | |

- **Old/current generation coexistence = 0.** The prefix is fresh, so `expected = None` and `retained_root_bytes = 0` (`two_bit_store.rs:1135-1142`).
- **Optional deletions at verified seals** (not counted above): shards after the prep seal; `normalized.f32`, `order.u64` and the derive copy of `sq8.bin` after the build seal; G after the publish receipt. With those, the peak falls to about 14.97 GB, during P3.

### Proposed native caps
**Derive config** (`borsuk-native-scale-derivation-config-v1`):

| Field | Value |
|---|---|
| `max_payload_bytes` | 2,147,483,648 (8 GiB is the code ceiling) |
| `caller_payload_bytes` | 67,108,864 |
| `caller_scratch_bytes` | 2,650,375,999 (shards + ids/queries/truth + 64 MiB of evidence) |
| `temporary_reserve_bytes` | 67,108,864 |
| `max_aggregate_scratch_bytes` | 16,057,615,935 (the derive's own 13,340,131,072 + caller scratch + reserve) |

**Build:** pass `MAX_MEMORY_BYTES` = 1,073,741,824. It is a modelled argument only, not an RSS cap. The router bytes do not change with it, because the router is built at the estimate (`two_bit_build.rs:684-687`).

**Publish:** `max_memory_bytes` = 536,870,912 and `already_pinned_bytes` = 0. Copy the checker's other limits.

### CLI seams (in order)
1. `build_sq8_source --derive CONFIG CONFIG_SHA NEW_OUTPUT_DIR` (`examples/build_sq8_source.rs:13-17`)
2. **SQ8 staging into the store** — no native command exists for this (see B1).
3. `build_two_bit_generation CONFIG CONFIG_SHA MAX_MEMORY_BYTES NEW_OUTPUT`, with `discovery: semantic`, the Scale1m profile, the order path and SHA, `raw = normalized.f32`, `sq8_object_key`, `sq8_etag`, and `base_epoch: 0` (bin:74-115).
4. `publish_two_bit_generation CONFIG CONFIG_SHA NEW_RECEIPT` (non-retained, LocalFileSystem; bin:206-250)
5. `check_cohere_native_baseline CONFIG CONFIG_SHA NEW_OUTPUT` with `Backend::Local`, rows 1,000,000, count 32, Scale1m. `population()` overrides the 100k production shape (`:312-336`).

### Unresolved blockers
- **B1 — no way to stage SQ8 into the store.** Nothing at HEAD puts `sq8.bin` into a LocalFileSystem store and reports its ETag; only the test fixture does it, with an in-memory `store.put` (publish bin:405-408). The build needs the key and ETag before it starts. The key must end in `/objects/<sq8_sha256>` and sit outside `{prefix}/maintenance/` (`two_bit_store.rs:360-376`).
  - Minimal workaround: copy or hard-link the file into place, then compute the ETag with object_store's `inode-mtime_us-size` formula.
  - That formula is **unverified**. If it is wrong, the publisher's HEAD check rejects the run before anything is uploaded (`two_bit_store.rs:1124-1362`, SQ8 HEAD identity).
- **B2 — the memory gate will misfire.** File I/O above 8 GiB in derive, build and publish will raise the `memory.events` `max` counter through page-cache reclaim. Gate on `oom` and `oom_kill` only, as the earlier 1M admission review found.
- **B3 — derive and build costs are not measured.** Derive RSS and wall time at 1M are unknown. The build model leaves out the buffers of `write_canonical_source` and `UnitCentroidPages::build_from_sq8_reader`. The 8 GiB cgroup is the only bound on those.
- **B4 — publisher memory at 1M is unverified.** The 512 MiB `validate_local_publication` model at 1M has only been exercised on tiny fixtures. It runs before any upload, so a failure is cheap.
- **B5 — deletions may break the checker.** The checker's source binding may read the corpus or normalized paths. Check its config fields before taking any optional deletion; the conservative ledger keeps everything.
- **B6 — prep must reproduce exactly.** Regenerated prep must match the sealed SHAs `1a491c06…` (corpus), `664f5b26…` (queries) and `36e83267…` (truth). That check costs nothing.

### Falsifying test (inside the run, no new Rust)
After each phase, mark the attempt INVALID (an execution failure, not a KILL) if any of these fail:
- Every file size under G and the store equals the formulas above.
- `root.bin` size satisfies `(size − 512) % 4160 = 0` and 489 ≤ L ≤ 977.
- Store roster sizes equal the G file sizes.
- The 1 s df-sampled disk high-water mark stays at or below that phase's cumulative ledger value.
- Per-phase VmHWM and cgroup `memory.peak` stay at or below 8 GiB, with `oom_kill` = 0.
- Each phase ends before its deadline.

Recall thresholds for the Q32 step are not part of this plan; preregistering them, freezing, costing and launching are for you to decide.
