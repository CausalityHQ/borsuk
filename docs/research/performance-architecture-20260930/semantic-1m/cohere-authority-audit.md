**Next gate: freeze a CoHere-specific representative64 population and preparation configuration. The existing next64 machinery cannot prepare CoHere through a configuration swap.** Audit is pinned to `6caab4a8`; no files were edited or campaign files inspected.

The corpus is **CoHere-large-10M’s original FIRST1M source ordinals 0–999,999, D768, little-endian f32, cosine**. This is distinct from the historical FIRST100k corpus.

All keys below use bucket `borsuk-bench-453182569524-euc1`:

- `P` = `publication/v3/20260812/datasets/cohere-large-10m-768/attempts/0001`
- `V` = `research/v261-cohere-dual-graph-1m/2dee58896e42f84d74e2653dc0960e19d6defc63/runs/a0001/artifacts`
- `S` = `research/native-union/20260929/cohere-source-1m-v4`
- `Q` = `research/native-union/20260929/fresh-cohere-seal-a0001/sealed`

| Artifact | Status | Exact key; bytes | SHA256 |
|---|---|---|---|
| Original corpus receipt | Available | `P/STAGING_COMPLETE.json`; 135,298 | `0965aa0241199822dfac3410bba4edad5536ac0eb0aaa8ab83c216e8c5749a87` |
| FIRST1M boundary parquet | Available | `P/materialized/train-00000045.parquet`; 67,122,282 | `1fae833dd9cbdb775b177176a2d301f0ee887988575b1152c13d7d0517cd25f4` |
| Original FIRST1M raw | Available | `V/vectors.raw`; 3,072,000,000 | `6c82a340e3e1b4226640e593efa9c4000c6a5962d4b13063093a1dab689a9005` |
| SQ8 | Available | `S/objects/` followed by its SHA256; 780,000,000 | `b2f2f7dbec79c8646495b4a1500c363c5ccbf51690cf1e936d1264e7ee3839e1` |
| Source order, LEu64 permutation | Available | `S/artifacts/order.u64`; 8,000,000 | `25672572b8e36eafea6ba856068f33aca5064e3ce02544340f682cd39881ea02` |
| Consumed 1,000-query panel | Available | `Q/queries.raw`; 3,072,000 | `28af674aaa41964d065c0cab8390488961aa4fa7b3d07bef4e95bb2bad7d769c` |
| That panel’s exhaustive GT100 | Available | `Q/truth.u32`; 400,000 | `b9dd2d8d6b12560108b55a70f339f2a097ee550df4b6e562aa91b76cbd9e4f41` |
| Fresh representative64 authority and GT | Missing | No CoHere population/seed/selected64 construction authority found in the inspected evidence | — |
| Complete historical freshness | Unknown | Committed proofs explicitly retain incomplete prior-query coverage | — |

“Available” combines committed provenance with successful `causality` HEAD checks. HEAD does not authenticate body contents; notably, `vectors.raw` has **no SHA metadata**.

The ordered train roster has 458 shards and 10M rows. FIRST1M comprises shards 0–44 plus the first **16,975 rows** of shard45, whose source start is 983,025. The complete constituent parquet roster still needs a committed preparation authority; no authenticated combined CoHere `source.parquet` identity was found. Evidence: [source identity proof](/home/rb/worktrees/borsuk-prod-ready-v9/docs/research/native-union-20260928/fresh-cohere-stress-query-families.json), [source construction identities](/home/rb/worktrees/borsuk-prod-ready-v9/docs/research/native-union-20260928/cohere-source-1m/a0001/source-build.json), [published order/SQ8 identities](/home/rb/worktrees/borsuk-prod-ready-v9/docs/research/native-union-20260928/cohere-source-1m/a0001/terminal.json).

The old panel selects source rows **1,005,000–1,005,999** from shard46: 67,122,091 bytes, SHA256 `13195bd1a643b659679156745e45dfdf3f2be7d8e340da1dd97bf84352cbf0c4`. Its development ordinals **0–63** and confirmation ordinals **64–999** were both consumed. Earlier exclusions include train100,000–104,999, train1,000,000–1,001,999, and all 1,000 registered test queries. The existing exhaustive f64 cosine GT100 is authority for the **old panel only**. These ranges come from pinned metadata and verified configurations, not filenames: [candidate/proof bindings](/home/rb/worktrees/borsuk-prod-ready-v9/docs/research/native-union-20260928/fresh-cohere-source-candidate.json), [seal verification](/home/rb/worktrees/borsuk-prod-ready-v9/docs/research/native-union-20260928/fresh-cohere-seal/a0001/verification.json), [confirmation configuration](/home/rb/worktrees/borsuk-prod-ready-v9/docs/research/native-union-20260928/fresh-cohere-confirm936-config.json).

The fixed next64 selector uses ReLAION’s physical IDs, ranked objects16–31, consumed reservoir0–999, and pinned ReLAION registry/population/overlap hashes. Its extractor requires `feature_row_id`/`embedding`; CoHere’s publication extractor uses `emb` and source ordinals. The `quality-execution` configuration also explicitly binds ReLAION. Exact seams: [selector](/home/rb/worktrees/borsuk-prod-ready-v9/scripts/select_v36_rank16_fresh_ids.py:58), [construction authority checks](/home/rb/worktrees/borsuk-prod-ready-v9/scripts/prepare_semantic_1m_fresh_panel.py:180), [source extraction](/home/rb/worktrees/borsuk-prod-ready-v9/scripts/seal_v36_rank16_fresh_1m.py:101).

One actionable plan:

1. **Commit the metadata authority first:** ordered CoHere shard identities, FIRST1M raw/SQ8/order pins, and a consumed-query ledger including the entire old 1,000-query panel. Keep historical coverage explicitly incomplete.
2. **Preregister a new representative64 selection:** freeze the eligible population, deterministic sampling rule, seed, ties and no-replacement policy before decoding. A hash-ranked sample across the declared eligible population is a concrete option requiring this new preregistration; neither adjacent rows nor ReLAION reservoir ordinals are already authorized CoHere selections.
3. **Add one CoHere preparation adapter and its frozen configuration.** Authenticate the existing raw corpus directly, extract fixed `emb` locators, and reuse `audit_duplicates`, `write_requests_truth`, the existing exhaustive oracle, and `seal_readback`. Generate new GT100 only after identity checks. No Rust change is needed for this preparation gate.

Risks are incomplete historical exclusions, assuming a contiguous block is representative, mixing FIRST100k and FIRST1M authorities, and comparing old f32-normalized duplicate hashes with the newer f64 normalization.

**Bounded falsifier:** one metadata-only adapter check, ≤60 seconds, one core, 128 MiB, zero S3 GETs. It must reproduce exactly64 locators and reject a consumed locator, changed shard identity, or missing population/seed pin before decoding. Any rejection stops the fixed panel without replacement selection.

The read-only metadata assertions passed for corpus/order/SQ8/seal bindings and both consumption configurations. No fresh CoHere quality or latency was established.
