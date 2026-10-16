# Bounded S3 read retry and SQ8 GET-budget curve

Date: 2026-10-16. Dataset: frozen Cohere Wikipedia multilingual v3, 1M rows,
D1024, cosine, k10, all1000 held-out queries. No competitor win is established.

## Shipped source

Canonical source commit: 12336e5ad08a171a0485d43a083881236ea3e81a.
Its six changed Rust blobs equal qualified EC2 snapshot
c3b0e81e93fdb06a13cc38a25868d75030a87e04. Snapshot history is not canonical.
The library adds an explicit bounded GET/HEAD retry policy with exact
range/length/ETag validation, ordered error reporting and complete draining.
Defaults and publication mutation policy are unchanged.

All preparation, compilation, tests and measurements ran on Causality EC2
i-04933959e7bb82dfc with persistent encrypted EBS. Devbox performed orchestration
and Git-object integration only.

Final qualifier64200041-cd10-4206-8052-cacbb9b9a2b8 passed affected compilation,
two exact fixtures, seventeen GET-curve regressions, two current-reader
regressions, both release binaries, workspace correctness/suspicious Clippy and
the actual environment-unset workspace test build. Separate SDK qualification
passed37 range tests, including status/body retry and concurrent ordered drain.
Qualification archive: b9ecd591de0251e12b5dbc4c1931366f4d47a8ee4d01197d3ea0a7527aa3bce7.

## Real-S3 reference

Original full SSM: ff645eb9-398d-434c-bd06-92fb15e5a906. Native0, strict reader0.
Admission and separate cold-credential canary passed first. Config v11 explicitly
uses one_retry_100ms, SOURCE128 logical GET, fetch64, SQ8 GET32/32MiB,
512MiB library envelope, 8GiB supervisor, cache/window off.
This is a new declared envelope; earlier16,773,120-byte failures remain INVALID.

Recall9785/10000 (97.85%); median154.374404ms, p90215.936970ms,
p95240.180405ms, serial5.998415queries/s. These are reference query intervals,
not a cold-service qualification or a matched vendor comparison.
SOURCE/SQ8 logical totals:107882/30033GET,27,428,966,400/10,625,249,152bytes.
Process counters are not wire or billed accounting.

## Frozen shortlist256 GET curve

Config SHA: db0ff9a0d85bdfa35b80dca52e5f87a5ded41254f82c09c8b7d82aa7f227ec5f.
Same current32-row blocks, SOURCE routing/scoring and shortlist256; four GET
limits, common32MiB candidate byte cap. Baseline is the exact completed
real-S3 reference. Historical G30 report comparison is descriptive.

| GET cap | Hits /10000 | Recall | Actual planned GET total | Planned SQ8 bytes total |
|---|---:|---:|---:|---:|
|30|9786|97.86%|28358|11497411968|
|38|9784|97.84%|34857|8562100736|
|46|9780|97.80%|40911|6821620736|
|64|9780|97.80%|52384|5067448960|

Actual-input admission25e44ca6-1b3e-412d-b81c-d3b0addd5dd9 passed whole SOURCE/SQ8,
all requests and truth validation. Separate one-query smoke
f3dd8e72-d0f8-4cef-bf65-efaf6a7f8ef4 passed with truth unopened.
Full a543e145-4e31-4a73-89e5-60a19f53e82d completed0 in217s.
All four arms admit1000 queries; exact baseline plan/trace/charges/IDs/scorebits
replay passed before measurement truth opened. Full report SHA:
980da403e974e73f31777b1ff9713beaf8273ccfb40568ff937954bdd80a7aba.

## Decision and next falsifier

Retain all four valid research trade-offs. Small recall differences do not
discard a candidate; G46/G64 trade0.05percentage points against this baseline
for substantially fewer bytes. G64 lowers planned SQ8 bytes52.31% but raises
GETs74.42%; neither implies latency or cost improvement. More GETs remove
large bridge reads, and can add network waves under bounded parallelism.
G46 and G64 have equal aggregate recall but different GET/byte trade-offs.

Next: expose the exact authenticated shortlist/block plan through a generic
production Rust serving API, then perform exact native parity/refusal checks
and a separate actual-input/canary/frozen real-S3 comparison. Keep download,
resident-byte and compute concurrency independently bounded; test ordered error
and drain under the chosen limits. Choose the first serving arm prospectively
from this curve and the source/API inspection, not retrospectively from timing.
No performance acceptance or production-default change is made by this report.

Closed evidence objects, bucket borsuk-bench-453182569524-euc1, prefix
research/semantic-router/20261016/persistent-rust-workspace-a0001/:
- final-source-freeze-a0001/: source archive and exact binaries.
- getcurve-prerequisites-export-a0001/evidence.tar.gz:
  b9ecd591de0251e12b5dbc4c1931366f4d47a8ee4d01197d3ea0a7527aa3bce7.
- getcurve-closed-export-a0001/evidence.tar.gz:
  b2ed5d069fb964ecce12926ec3a0071f225535793708692e96e24f71753276d0.
- getcurve-closed-export-a0001/decision-summary.json:
  f6be6236c6c4c5f1c321ba10f79c3aa6061a1d80474bffb80061a17bdf49ecec.
