# V194 fresh 1M optional utility source and return gate: preregistration

## Question and split

Test the V193 cross-scale policy on **new** ReLAION-1M D768 source
pseudoqueries, then measure both exact-source physical GT100 coverage and
actual SQ8 returned top-100 GT100 hits. The dataset's 1M rows and V164
physical generation are historical development assets, but the query
identities and labels in this panel have not been opened by V189, V190,
V192 or V193. Select the next **512** stable-ID SHA-ranked source rows at
zero-based ranks **2944–3455** (display ranks **2945–3456**) with the
existing `select_pseudoqueries` rule. Treat all 512 as one validation
split. Exact float64 cosine GT100 excludes each source row. No label,
candidate ceiling, plan result or partial CSV from this panel may be used
to fit or change a model, price, candidate radius, cap or exception.

V189 model-fit ranks 2433–2496 and V190 validation ranks 2689–2944
are disjoint from this panel. The V192 model and prices, V193 arm set,
V164 layout and V115 router are frozen. The 100k V193 comparison is a
reused developmental screen, not the quality target for this gate.

## Frozen method and paired controls

Use V190's authenticated 1M source, old layout/SQ8, V164 order and
V115 router. Repeat the 513-PQ nominee call and direct-512 prefix
check, remove the query's own old-layout row, and take the next 512
nominees. SQ8 squared-L2 chooses the 100 primary nominee rows; their
V164 32-row physical units are mandatory. Form V190's radius-32 PQ
reconstructed-cosine candidate field and rank units by minimum row
score, masking the source row before the unit minimum. Keep the V192
optional-risk model trained on V189 fit64 ranks 2433–2496 and its
`(unit=1000, GET=50000)` price. Use the V192 full-rank refit on the
same 64 with its own `(2000, 50000)` price, plus V193's constant-risk
ablation. Use the same V190 V187-style 446-base elastic greedy plan as
a paired historical-method control. None is refit on V194 data.

For the three priced arms, run `hard_priced_cover` with page count
31,250, at most 672 full 32-row units and 32 GETs per query, and an
explicit 512 MiB trace allowance. Preserve mandatory units. A trace
budget or mandatory-cover failure is an infeasible query, never a
zero-recall result. For all arms, count bridge units, physical byte
ranges, GETs and per-query caps. Construct actual V164-ordered SQ8
fetched rows from the authenticated old SQ8 artifact, score them with
the same frozen SQ8 quantizer and deterministic top-100 scorer as V155,
exclude the query's own stable ID, and return 100 distinct stable IDs.
Record whether any scored row is missing or a query lacks 100 returned
IDs. The paired greedy return arm has the same SQ8 and truth scorer.

Run `prepare` and `plan` on one Spot worker. Before calculating any
exact-source GT100, copy all 512 GT-blind features, all physical plans,
model/price identity, and both seals to distinct immutable S3 keys.
The evaluation process independently authenticates those copies and
the external seal digests before it opens source truth. Record complete
raw per-query candidate ceilings, mandatory floors, fetched exact
coverage, returned IDs/hits, resource charges, and offline phase
CPU/wall/RSS. An interrupted cell is discarded and restarted under a
new attempt ID. Do not inspect an incomplete measurement artifact.

## Frozen gates and interpretation

The strongest prior BORSUK actual-returned 1M comparator is **V155 on
the used ReLAION-1M D768 validation-1000**: 99,567/100,000 returned
exact-source GT100 hits, p05 98, 11,134,007,040 planned bytes and
22,126 GETs. It is **unpaired**, so meeting its scaled envelope does
not establish a paired performance win. For 512 V194 queries, the
predeclared screening envelope is:

- at least **50,979/51,200 actual returned GT100 hits** and p05 at
  least **98**;
- zero infeasible plans, every query at most **672 units/32 GETs**,
  aggregate at most **5,700,611,604 planned bytes/11,328 GETs**;
- at least 50,979 physically fetched exact-source GT100 rows, since
  returned hits cannot exceed physical coverage;
- 512 complete returned top-100 sets, with the query's own ID absent.

Evaluate optional-risk and full-rank against these gates separately.
If neither passes, stop promotion and separate candidate omission,
mandatory-cover, optional allocation, quantization and returned-rank
losses before changing the responsible layer. If both pass, select
optional-risk only if it uses no more bytes and GETs and loses at most
**26 returned hits** (0.05 percentage point) versus full-rank; otherwise
select the passing full-rank arm. If only one passes, select it. This is
a predeclared policy-frontier choice, not holdout fitting. Report the
constant-risk and greedy controls and paired per-query wins/ties/losses
regardless of the decision. A passing V194 policy advances to **live
S3** returned quality, p50/p95/p99 latency, throughput, charged serving
RAM and cost; it does not freeze a production default by itself.

For the tail, report minimum, nearest-rank p05, count of queries below
98, bottom-decile mean and a 95% Wilson interval for the proportion
below 98. The interval is descriptive; it does not change the frozen
gate. Source pseudoqueries are not independent real user queries, so
neither the interval nor a pass proves real-query quality. A distinct
dataset and 10M/100M scale gate remain required. Lean can certify
conditional planner caps and recall implications, but data-dependent
recall, storage latency and charged RAM require measurements.

Use one Causality Spot cell with an immutable source archive, terminal
marker, artifact hashes and instance identity. Terminate compute
immediately after the terminal. Keep the devbox free of full local
builds/tests while swap remains allocated.
