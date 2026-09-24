# V123 postmortem SQ8 candidate-capture and exact rerank diagnostic

Status: preregistered after the sealed V121 a0003 failure and before any V123
top-K or exact-source rerank result. This is a **development postmortem**, not
an untouched quality promotion. It uses V121 a0003's already evaluated
deep-image-96-angular publication test ordinals 0..999 and its immutable
candidate and paired-control physical ranges. The query and GT values may be
used for diagnosis, but no setting selected from this cohort qualifies the
generic method or may be reported as an independent validation result.

Inputs are V121 a0003 terminal SHA-256
`01cf70dfc77c285636943b334c21efb3bccbfe33dcfeffee153e4a7cdb484113`,
its `requests.jsonl` SHA-256
`31626934383e80ae683bbb21298d019d5b7b985dbc80f8542d4fc363db58818f`,
`rust-replay.jsonl` SHA-256
`ddc9af991bdc6d3ef77d34a156994daa43aeb78f67f18de2cd0dc5ebb93abe91`,
V120's 1,078,920,000-byte SQ8 body SHA-256
`7c78292cef359f1a3bc7f7406a0cedffe79a5bf06b7356f1a97de209ed6c5e7c`,
V119's 3,566,768,562-byte normalized source Parquet SHA-256
`8f88122f412554107d97c07f440352f9043b8cb4b58fe08434ac75f4b90776ee`,
and the publication GT Parquet SHA-256
`d305fcea7387988941defd2942cca1673693271329f977ba073da888cac3de8d`.
Authenticate every downloaded object and the V120 router low/step sections
against their sealed manifests before scoring. Do not refit a router, layout,
codebook or planner, change a physical range, or read a new quality cohort.

For both candidate and control ranges, use the same Rust returned SQ8 scorer
with an explicitly requested top 1,600, capped at the number of fetched rows
when a valid plan contains fewer. The fixed nested candidate widths
are **K = 0, 100, 256, 512, 1,024, 1,600**, where K=0 means only the 512
router nominees. Before computing any capture
statistic, require identical nominated IDs, primary, ranges, planned bytes
and ordered top-100 returned ID prefixes against V121's sealed Rust replay
for every query and both arms. A mismatch invalidates the diagnostic; do not
accept approximate parity to make a number look good. The control is a
**shared-nominee hybrid**: it unions the candidate's 512 router nominees with
the V121 control ranges' SQ8 top-K. It is not the V121 served control.
At K=0 both arms are identical by construction.

At each K, record GT100 capture by the SQ8 top-K, by the 512 nominees, and
by their union. Capture is an upper bound on an exact reranker over that set.
Seal capture evidence and its summary before downloading the source corpus, so
a source-transfer failure still leaves a verifiable capture result. Record the
realized SQ8 width for every query and K, including saturation below K=1,600.
Then load and verify the source corpus. For each union,
score source vectors using both original float32 coordinates and those same
coordinates rounded to IEEE FP16 then widened back to float32. Rank
`(descending cosine score, ascending source ID)` to return 100. Record
returned GT hits, p05, sub-90 count, paired wins/ties/losses for both source
precisions, FP16 versus
float32 top-100 disagreement, diagnostic process peak RSS and wall time. FP16
here is an offline quantization simulation, not a measured serving mirror or SSD
latency. The K ladder is a diagnostic frontier, not dataset-specific parameter
tuning. A positive candidate at K=512 requires ≥99,000 FP16 returned hits,
p05 ≥90, within 0.05 percentage points of float32 on the same union, no
worse than the shared-nominee hybrid control, and unchanged remote GET/byte caps. It only
authorizes a matched ReLAION development build and a fresh deep-image held-out
cohort, then live S3 and cgroup measurements. A K=0 pass will be reported as
evidence that the existing S3 wave adds little to returned quality on this
cohort, not as object-storage-native serving qualification.

Use one interruptible Causality Spot attempt with a 7,200-second science cap,
terminal marker, artifact SHA-256 receipt, interruption handling, and immediate
instance termination. Monitor incomplete work by terminal and infrastructure
only. An interrupted cell is discarded and restarted under a new attempt ID.
The diagnostic cannot establish unseen-query recall, live S3 latency, a
production RAM default, or 100M capacity. RAM/SSD placement must later be
qualified over `(N,D,R,C,G,L)` without a vector-count quality knee.
