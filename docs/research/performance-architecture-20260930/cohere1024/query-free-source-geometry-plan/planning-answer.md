**Select no new ANN arm yet. Obtain one query-free, exact source-neighborhood measurement before investing in graph-cut partitioning.** This recommendation uses only committed documents and source at `e359f0a33661107f55861af9a17d5fe58f56d432`.

The established cause is leaf nomination: original returned recall was **313/320**, SOURCE-scored closure covered **314/320**, and the six uncovered targets belonged to unselected leaves. Cosine48 and sampled-boundary nomination both validly killed at **312/320**. The sampled arm’s gain1/loss3 does not identify midpoint bias, approximate graph discovery, adjacency truncation, or SQ8 distortion as the cause. q537’s covered ranking loss remains separate.

The missing evidence concerns **partition geometry**, not another attribution report: do exact source-row neighborhoods cross the current leaf boundaries, and does that fragmentation offer a plausible advantage over the existing mean-based partition?

Rank the alternatives as follows:

| Rank | Alternative | Decision |
|---|---|---|
| 1 | **Graph-cut repartition of existing 32-row units into leaves of at most64 units**, preserving physical row order, L2 mean ranking, fixed48 selection and downstream scoring | Best distinct hypothesis, conditional on source geometry. Changes semantic partitioning alone; adds neither row copies nor adjacency nomination. |
| 2 | Query-dependent leaf ranking using source-derived residual bounds | Smaller construction change, but loose bounds can favor broad/outlier leaves. Current evidence does not justify another summary ranker. |
| 3 | Paged centroid hierarchy with router-entry copies | Defer. It combines discovery, representation, hydration and potentially layout changes; boundary quality and lifecycle costs remain unresolved. |

Fable’s physical graph-cut layout **plus** adjacency nomination changes two layers. Its stored48B/row estimate was also mistaken for residency: the existing D768 decoder needs96.125B/row for centroid arrays; at D1024 that becomes **128.125B/row**. V139 demonstrated threshold/centroid mismatch, V146 rejected linear scan cost, V149 lost discovery through broad summaries, and V150 lost both discovery and unit-to-page diversity. These historical failures warn against the proposals; they are not fresh Cohere measurements. The old panels are consumed.

The actionable next task is one bounded Rust source-neighborhood diagnostic:

1. **Authenticate the unchanged1M generation.** Use its normalized FP32 `canonical.bin`, router root, membership, physical-order authority and exact FP16 unit centroids. Canonical records already contain physical-order logical IDs and normalized FP32 rows. Do not reconstruct the reference neighborhoods from SQ8.
2. **Choose32 source anchors before measuring geometry.** Deterministically select32 physical units by a domain-separated hash of the generation identity and unit ID; choose one row within each unit by a separately specified hash. Seal the anchor list, source pins and method before computing neighborhoods. Exclude the anchor itself.
3. **Stream all1M canonical rows and retain exact top32 cosine neighbors per anchor**, using f64 accumulation and logical-ID ties. Requests, truth and original ANN producer bodies remain unopened. These are source neighborhoods, not held-out queries or ANN recall.
4. Emit one compact terminal summary with:
   - Neighbor containment within the anchor’s unit, physical256-row page and semantic leaf.
   - Distinct target leaves and their frozen L2 prototype ranks, including counts outside top48.
   - Row-to-unit-mean and unit-to-leaf-mean residual distributions for those neighborhoods.
   - If the existing sealed adjacency is admitted, how much exact cross-leaf neighbor mass its retained destinations represent. Read it once; do not rebuild or rerun its failed policy.

This tests the premise that graph-cut grouping has useful locality to preserve. **If these anchors show no neighborhood targets outside L2top48, they supply no support for changing the partition to address the observed misses.** If apparent locality exists chiefly below the indivisible32-row-unit boundary, a unit-only partition lacks the necessary resolution. Positive evidence remains exploratory: it must show where locality exists and where compression loses it, rather than merely report a high cross-leaf percentage. Neither outcome changes the historical KILLs or authorizes a production arm.

Bound the diagnostic to **oneCPU,1GiB charged cgroup memory, swap0,8GiB data scratch and1200seconds**, on causality Spot. Admit the allocation/coexistence model before opening bodies; charge file cache separately from process RSS. Use streaming buffers and32 bounded heaps, never an all-pairs matrix or resident row graph. The work is **32.768billion coordinate terms** plus validation scans; the deadline is a limit, not a runtime prediction. A verified resource exhaustion rejects this implementation envelope; setup/authentication defects are INVALID. Preserve interruptions and restart the whole diagnostic attempt without changing its method.

The smallest implementation seam is **`crates/borsuk/examples/compare_native_replay.rs`**: one source-only CLI branch, a strict versioned config without request/truth/producer fields, and reuse of `header_file`, `HeaderSnapshot`, root admission and durable seal publication. Keep the bounded computation private there. No controller, new library abstraction or serving-format change is needed for this diagnostic. `canonical_source.rs` establishes the input format; `semantic_unit_router.rs` provides the authenticated membership and frozen ranking.

Require full SHA/length/EOF, generation and physical-order agreement, ID bijection under this fixture’s ordinal contract, finite nonzero rows, checked geometry/allocation arithmetic, and file/path identity rechecks through publication. Small native tests must cover nonidentity IDs/order, self exclusion, equal-distance ties, partial units, wrong membership/generation, duplicate IDs, corrupt/truncated/grown files, invalid norms, overflow, and seal/cleanup failures. On the frozen remote revision, compile the affected target and pass the required workspace Clippy and complete test-build checks. Then pass separate real-input admission and disposable smoke before scientific execution.

The resource accounting must remain explicit:

| Quantity, D1024 | 1M | 100M arithmetic only |
|---|---:|---:|
| Canonical FP32 plus IDs | 4.104GB | 410.4GB |
| SOURCE records | 264MB | 26.4GB |
| SQ8 records | 1.036GB | 103.6GB |
| Decoded full unit-centroid arrays | 128.125MB | 12.8125GB |
| Maximum encoded semantic root under current leaf arithmetic | 4.065MB | 406.254MB |
| Retained prototype/membership/directory arrays, before other overhead | ≈4.385MB | ≈438.284MB |

The current semantic query path does **not** retain the full centroid arrays merely because construction uses them. Conversely, its metadata hydration still scales with population. The diagnostic adds no serving residency. **100M is unsupported by the current profile**, and none of these numbers is measured RSS or build qualification.

An exact full row-kNN builder would require roughly `N²D` coordinate work;32 u32 neighbor IDs alone cost128MB at1M and12.8GB at100M. The diagnostic avoids that build and does not qualify a scalable replacement. Any later partition arm must account for builder scratch, retained metadata, simultaneous pinned generations, concurrent query scratch and full canonical compaction rebuilds. Local touched-page maintenance is still a proposal. Retained generations multiply storage; publication and GC must preserve existing pins.

For a later **partition-only** candidate, preregister exactly one consumed32 screen. Because it retains fixed48 nomination and the same SOURCE-scored page-closure definition, **coverage≥315/320 and gains>losses** can remain its cheap survival rule; reproduce control313 returned/314 covered first. Fetched bridges alone do not count. Cap refusals are INVALID, not valid omissions.

Survival must then be checked against **actual returned IDs**, with a separately frozen returned-recall criterion. It earns no cold claim. Fresh cold qualification needs an audited unused query identity, exact input bytes, real admission/smoke, caches disabled as declared, end-to-end tails, offered-load QPS, physical requests/bytes, pins and lifecycle cost. Historical GET/byte limits apply to that unchanged screen; they are not universal100M architecture gates.

Skipped: edits, Cargo, native execution, tests, mocks, raw data, network and children.  
Risk:32 source anchors can falsify or motivate a locality hypothesis; they cannot establish held-out recall, scalable construction, cold competitiveness or readiness.
