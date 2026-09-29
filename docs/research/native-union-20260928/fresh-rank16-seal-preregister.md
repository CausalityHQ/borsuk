# Rank16 ReLAION 1M query and exact-GT construction

Frozen before candidate embeddings or GT were decoded. Source and selected-ID
authority: `fresh-rank16-panel-preregister.md`, panel SHA256
`a8bd97d6e6468e715c4b26d9df8c400f649e5428b2a85f151307f20cb030230c`,
and scoped lineage decision `fresh-rank16-identity-decision.md`. This cell may
construct sealed inputs only; no ANN, recall, HTTP, vendor or scale measurement.
No old rejected seal or incomplete measurement body may be read.

The indexed corpus is the original V36 FIRST1M D768 cosine Parquet SHA256
`2796b579f37afe99ca4aff57e282335a6a79ad30596645957d26326a0560cf86`.
Reconstructed source.raw must equal existing candidate/control SHA256
`a3eac4dedae5006843ea2ad243ec590440fe5b983ca8cd969dbc92b3e3406c33`.
Decode only selected query rows from full-SHA-verified original-revision rank16–31
physical shards and check feature ID at each frozen rank/row locator. Query
ordinals0–63 are development;64–999 remain prospective. Any identity mismatch,
selected-ID reuse proof or incomplete lineage after construction rejects the
whole panel; no substitution or row salvage.

Ground truth: reuse the AWS-proven exact `run_native_source_frontier_1m.oracle`
on 1M raw source rows and1,000 external f32 queries. It normalizes the original
f32 values in f64, computes exhaustive cosine, merges exact block top100 and
breaks ties by signed **source ordinal** ascending, as the current native index
does. Its existing three-row scalar/matrix self-check must pass on AWS.
Output `queries.raw` (3,072,000B), `requests.jsonl` (1,000 lines), and
`truth.u32` (400,000B) to new conditional-create sealed S3 keys; terminal
and decision receipts must bind every SHA256. No quality output is produced.

One original AWS `causality` Spot c7g.4xlarge worker in eu-central-1,16 vCPU,
32GiB RAM,80GiB encrypted gp3 root,8 BLAS/CPU threads,24GiB cgroup memory,
zero swap,3,000s process cap,3,600s machine cap. Spot quote must be <=$0.60/h;
max compute at that quote $0.60; allow at most $0.50 additional EBS/S3/transfer
for this cell, report actual compute only as an estimate. Stop/terminate as
soon as the terminal marker exists. Interruption or failed identity discards
the whole measurement cell; no automatic retry or overlapping job. Immutable
source archive/config and unique attempt reservation precede launch. Sync
terminal and small resource/decision artifacts to S3 before shutdown; verify
S3 hashes and actual EC2 termination independently before any ANN quality gate.
