# V41 Learned Residual Page Router Falsifier Design

## Status and purpose

V41 is a claim-ineligible, artifact-only, one-million-row falsifier. It asks
whether a small supervised residual model can predict useful V38 pages from a
query and its already selected pages. It does not change the V38 relation,
download corpus vectors, read page bodies, simulate S3 after a quality failure,
or claim production readiness.

The frozen population is ReLAION2B cohort A: 1,000,000 non-null `f32[768]`
rows. V41 uses the already burned 1,000 development queries and exact GT@100
for model development, the disjoint 1,000 validation queries for the first
generalization decision, and the 1,000 sealed-holdout queries at most once.
All query and truth identities come from the immutable V36 freeze receipt.
Every GT row maps through the immutable V38 primary/accepted-alternate
relation. The page budget is exactly 21 for the primary hypothesis.

V40 established the need for a different predictor. Its direct tree router
reached 918,010 ppm aggregate and 90,000 ppm minimum recall. Its
overlap-aware spill challenger exposed about 121 of 123 pages per query but
fell to 913,070/90,000 ppm. Candidate discovery was already broad; the
query-blind population-overlap score selected the wrong pages. V41 therefore
removes the tree frontier and scores all 123 pages at one million rows.

## Alternatives and decision

Three alternatives are materially distinct:

1. **Learned residual page scoring.** Train from the burned development
   queries and their GT-derived remaining page gains, then evaluate disjoint
   validation and holdout. This needs no corpus stream and tests the precise
   V40 failure: query-conditioned discrimination among known pages.
2. **L2 multi-probe locality-sensitive hashing.** Build fixed-width hashes and
   bucket-to-page postings from one 1,458,450,077-byte corpus stream. It is
   query-independent, but bucket skew, replication, and hundreds of logical
   probes make it a larger and less targeted next experiment.
3. **Representative or graph page discovery.** Stream the corpus to collect
   representatives, then search them exactly or through a graph. It risks
   repeating failed prototype summaries, storing a second ANN index, or adding
   dependent S3 reads.

V41 chooses the first alternative because it is the cheapest causal test. A
pass would justify a later query-independent pseudoquery-training experiment;
it would not by itself make workload-supervised training a production default.
A failure rejects this exact learned model, not all learned routing.

## Frozen artifacts and split discipline

V41 binds the exact V36 freeze receipt and these roles: development,
validation, and sealed-holdout query/GT Parquet; V38 construction result,
relation, and posting summary; the V39 K21 feasible-witness result; the V40
direct result; source archive; binary; and source commit. The V36 query roles
are disjoint by construction. The V38 relation and every query/GT pair must
authenticate before semantic use.

Before training, a query-only split audit opens the development and validation
query Parquet roles together without either GT role. It rejects any
bit-identical `f32[768]` query vector that occurs in both roles and records both
row counts and vector-bitset digests. Holdout remains unopened. Only after a
validation pass does a second query-only audit open the holdout query role and
reject bit-identical overlap with either already authenticated vector-bitset.
This prevents workload leakage without exposing validation or holdout truth.

Development is already burned. Exact duplicate `f32[768]` query vectors must
remain in one partition. Groups are ordered by
`(sha256("borsuk-v41-development-split-v1" || vector_f32_bits), minimum
query_ordinal)`. Whole groups are assigned to training while doing so does not
exceed 800 rows; the remaining groups are diagnostic. The expected no-duplicate
shape is 800/200. Any other counts are recorded and the diagnostic aggregate
gate uses the observed fixed diagnostic count. Validation never selects a
checkpoint. Holdout remains unopened unless validation passes.

A trusted, model-free partition phase materializes query and GT Parquet for
the development training and diagnostic partitions. Each child binds the
authenticated parent byte identity, exact sorted parent ordinals, split-rule
identity, and sibling identity. Diagnostic training receives only the training
children; it has no descriptor, path, URI, or role for diagnostic GT.

Training is explicitly supervised and may open only development query, GT,
and relation artifacts. Selection may open query vectors and the frozen model,
but never GT or relation membership. Evaluation may open the sealed selection,
GT, and relation, but never queries, model internals, or optimizer state.

## Residual model

For query `q`, selected set `S`, page embedding `e_p`, and page bias `c_p`,
the model is

```text
q_state = W_q q + b
s_state = sum(e_s for s in S)
h(q,S) = relu(q_state + W_s s_state)
score(p|q,S) = e_p dot h(q,S) + c_p
```

`W_q` is `64 x 768`, `W_s` is `64 x 64`, each page has one 64-element
embedding, and selected pages are masked. Each round chooses the maximum score
with page ordinal as the exact tie-break. Exactly 21 unique pages are emitted;
non-finite state, score, weight, gradient, or optimizer value is a stop.

For training query `q` and current selected set `S`, the literal target is

```text
y_p(q,S) = count(x in GT100(q): p in owners(x) and owners(x) intersects S is false) / 100
```

Each neighbor contributes at most once. The current model supplies the rollout
set `S`; labels are independently recomputed from GT and relation at every
step. At the start of each epoch, freeze the current weights and generate all
21-step rollouts. For a state with remaining gain, normalize nonnegative
`y_p` over unselected pages and use listwise softmax cross-entropy. A state
with zero remaining gain contributes no loss but still fills deterministically.

The frozen epoch model determines only the ordered rollout pages. Training
then evaluates the live batch model on that fixed ordered prefix: selected
embeddings are accumulated in selection-rank order, gradients flow through
both selected and candidate embeddings, and every page score participates in
the softmax except masked pages. Batch loss is the arithmetic mean over only
positive-gain states. A batch with no such state performs no optimizer step and
does not advance the Adam step counter. Gradient tensors are reduced in state
ordinal then parameter ordinal order.

Training uses the 32-byte SHA-256 digest
`sha256("borsuk-v41-residual-router-initialization-v1")` as its initialization
ChaCha20 key. Every matrix and page embedding uses Xavier-uniform samples from
that stream; `b` and every `c_p` start at positive zero. Epoch `e` uses the
separate 32-byte key `sha256(initialization_digest || e.to_le_bytes())` for
Fisher-Yates. Training runs 50 epochs in batches of 64 states. AdamW uses
learning rate `0.001`, beta1 `0.9`, beta2 `0.999`, epsilon `1e-8`, decoupled
weight decay `0.0001`, global gradient-norm clipping at `1.0`, bias-corrected
first and second moments, and no learning-rate schedule. Literal golden tests
freeze forward, backward, clip, and update bits. Training and inference use
checked f32 plus new 64- and 768-element registered kernels in `borsuk-fma`;
an unavailable fused backend is a stop, never a scalar scientific fallback.
No post-diagnostic checkpoint is selected. The final model repeats the same
procedure from the same initialization over all 1,000 development queries.

The fused order is part of the artifact authority. A 64-element dot uses eight
lanes; lane `l` consumes dimensions `8*l + s` for `s=0..7`, then lanes reduce
in ascending order. A 768-element dot uses eight lanes; lane `l` consumes
dimensions `96*l + s` for `s=0..95`, then lanes reduce in ascending order.
Every row of `W_q` uses the 768-element order; every row of `W_s` and every
page score uses the 64-element order. AArch64 implements two four-lane
`vfmaq_f32` accumulators; x86/x86_64 implements one runtime-detected AVX/FMA
eight-lane accumulator. Backward outer products traverse state, output,
input, then parameter ordinal, all increasing.

For optimizer step `t`, checked f32 operations apply `m=beta1*m +
(1-beta1)*g`, `v=beta2*v + (1-beta2)*g*g`, `m_hat=m/(1-beta1^t)`,
`v_hat=v/(1-beta2^t)`, then `theta=theta-lr*(m_hat/(sqrt(v_hat)+eps) +
weight_decay*theta)` in that written order. The global norm is the square root
of the ascending-parameter sum of `g*g`; when above 1.0, every gradient is
multiplied by the single checked `1.0/norm` scale before moment updates.

Serving parameters contain `53,312 + 65P` f32 values. That is 245,228 bytes at
`P=123` and 3,387,328 bytes at the projected `P=12,208` for 100 million rows.
The query transform is cached once. Inference performs
`49,152 + 21*(4,096 + 64P)` fused MACs: 300,480 at one million rows and
16,542,720 at 100 million. These are arithmetic projections, not measurements.

## Fail-fast scientific ladder

1. **Authority and arithmetic.** Validate all roles, split isolation, owner
   mapping, targets, rollout masks, tie rules, parameter counts, and canonical
   reductions on tiny exhaustive fixtures.
2. **Source-free preflight.** Benchmark complete hot inference after one-time
   model validation at `P=123` and `P=12,208`. Use 1,024 warmups and at least
   10,000 raw nanosecond samples. Require p99 at most 5 ms, at most 256 MiB
   cgroup memory, zero swap growth, and memory PSI full avg10 at most 0.75.
3. **Burned diagnostic screen.** Train only on the development training
   partition. Seal all diagnostic selections before opening diagnostic GT.
   Require at least 998,000 ppm aggregate recall and at least 800,000 ppm for
   every query. Stop immediately on one query below 80 hits or when cumulative
   misses exceed `floor(0.002 * 100 * diagnostic_query_count)`.
4. **Final development model.** Only a diagnostic pass permits one unchanged
   training run on all 1,000 development queries. Freeze model, binary, and
   authorities. An in-sample result is diagnostic only.
5. **Validation.** Seal all 1,000 selections before evaluation. Stop on one
   query below 80 hits or cumulative misses above 200. No early success and no
   retraining after any validation output.
6. **Sealed holdout.** Only validation pass opens the holdout once, with the
   identical fixed model, K, gates, and stops. A failure terminates V41.
7. **Transport.** Only holdout pass permits a separate coarse/fine request and
   byte model. Physical S3, performance queries, 10M, and 100M remain fenced.

The 998,000-ppm K21 target is an aggressive hypothesis. V39 demonstrated a
998,510-ppm feasible witness but certified only a 1,000,000-ppm upper bound.
The witness therefore leaves 51 hits of observed margin, not a proven optimum.
If V41 fails, preserve it and preregister a separate Pareto experiment over
`K={21,24,32,48,64}` and recall targets `{95%,98%,99%,99.8%}`. A larger-K
point is never relabeled as a rescued K21 pass.

## Cross-language artifacts

Cross-language tables use strict Parquet, dense model tensors use Arrow IPC,
and authorities, manifests, results, progress, and terminals use recursively
sorted compact JSON plus one LF. No Rust layout, pickle, framework checkpoint,
or native memory dump is an artifact.

- training-state Parquet records epoch, query ordinal, rollout step, selected
  page, exact target summary, loss, and stop counters;
- selection Parquet records query ordinal, rank, page ordinal, score bits,
  backend, and model identity;
- Arrow model tensors have fixed names, shapes, order, f32 type, and no nulls;
- completed canonical results recompute every query hit count, aggregate,
  minimum, disposition, and all input/output identities;
- rejected-prefix results contain fixed total count, evaluated prefix count,
  cumulative hits/misses, stopping ordinal and reason, but contain no
  fabricated full-split aggregate, minimum, or pass value.

Readers reject aliases, missing/extra fields, wrong physical types, nulls,
non-finite values, duplicate or unordered ordinals, shape drift, digest/length
drift, split overlap, stale model/layout generations, and trailing bytes. Each
input is opened once, bounded, authenticated through the retained descriptor,
and never reopened by pathname for scientific use.

## Scalability and release limits

Routing metadata is resident and adds zero per-query S3 routing GETs after
initialization. V41 still selects 21 coarse objects and does not hide fine
vector reads. Under the existing V40 worst-case bound, 21 coarse objects are
10.5 MiB/query and 21,000 GET/s at 1,000 QPS before retries. Full serving must
separately qualify fetch concurrency, fine candidates, reranking, cache state,
and total memory. The 256 MiB preflight gate is a router-only gate.

Supervised development training is not a write-through production design.
Before release, a passing V41 model must be replaced or validated by a generic
query-independent training source, support generation-bound atomic refresh,
and demonstrate incremental write/compaction throughput. Because BORSUK is
pre-release, failed experimental formats are removed rather than supported by
compatibility readers.

## Terminal dispositions

Every phase writes one create-only authenticated terminal. The only successful
predecessor chain is `split-audit-passed` -> `development-partitioned` ->
`preflight-passed` -> `diagnostic-trained` -> `diagnostic-selected` ->
`diagnostic-passed` -> `final-trained` -> `validation-selected` ->
`validation-passed` -> `holdout-audit-passed` -> `holdout-selected` ->
`learned-router-feasible`. Failures use `authority-rejected`,
`preflight-rejected`, `training-rejected`, `diagnostic-rejected`,
`validation-rejected`, `holdout-rejected`, or `holdout-indeterminate` and
close the arm.

Before any holdout input is opened, the controller conditionally creates one
campaign-wide `holdout-consumed` marker binding model, split, source, binary,
and attempt. Once that marker exists, an interruption may resume publication
only from an already complete authenticated holdout result; it may not reopen
or rerun holdout selection or truth evaluation.

Phase resource limits are registered, not inferred from model size. Split
audit/partition allow 1 GiB cgroup memory, 256 MiB scratch/output, 60 seconds
without progress, and 600 seconds science wall. Diagnostic/final training
allow 3 GiB memory, 1 GiB scratch, 256 MiB output, 120 seconds without
progress, and respectively 3,600/5,400 seconds science wall. Selection and
evaluation allow 512 MiB memory, 256 MiB scratch/output, 60 seconds without
progress, and 600 seconds science wall. Preflight retains its 256 MiB and
5 ms p99 gates and has a 300-second science wall. Every phase stops on swap
growth or memory PSI full avg10 above 0.75. Rollouts retain only one epoch's
`query_count*21` page prefixes/state ordinals; training-state Parquet streams
in row groups of at most 4,096 and no all-epoch state table is resident.

All results remain `claim_eligible=false`. Any failure closes the arm without
parameter repair or repetition. Interrupted non-holdout Spot cells discard
incomplete measurements and restart only from complete authenticated
predecessor artifacts. Holdout follows the stricter consumption rule above.
Benchmark instances terminate in a `finally` cleanup path even when output
publication or terminal verification fails.
