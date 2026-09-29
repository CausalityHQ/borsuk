# CoHere FIRST1M source construction complete

2026-09-29. **GO for fixed-panel identity and exact-GT construction.** The
current Rust builder produced CoHere-large-10M's canonical first1M D768 cosine
v4 generation, root `a4eb4851c545e828f3d08181a0c3e9cca09ed9341032ee3e69c511b9b7caf67e`.
No query vector, GT or ANN quality body was opened during construction.

Original local build session20894 and publication17138 both exited0. The
source-only build took **1,940.77 s** and reported maximum process RSS
**407,004 KiB**, zero reported swaps, under a configured4 GiB address-space
limit. These are verified GNU-time construction measurements on a shared
x86_64 VPS; cache and competing system I/O were uncontrolled. They include
source processing and SQ8 publication. There is no matched construction
baseline, measured query latency/QPS, or total-dollar result. Canonical source
writing used bounded random source reads and spent most wall time waiting on
I/O; scale construction and maintenance cost remain open.

The source raw SHA is `6c82a340…`, normalized SHA `fd175b35…`, order SHA
`25672572…`, and SQ8 SHA `b2f2f7db…`. The order is a complete first1M permutation
from the existing source-only hierarchical v3 recipe. Publication independently
checked root-bound component hashes, plane source/order/SQ8 identities, coefficient
f32 bits and canonical geometry, then read back and hashed **all20 S3 artifacts**.
Terminal SHA is `905ed20152c29ded8f339fd030f9c02a0cabd67c7d54b143b9832ed56a643d92`,
prefix `research/native-union/20260929/cohere-source-1m-v4`. The source SQ8 is
780,000,000 B and canonical object3,080,000,000 B. Query publication can reuse
these verified objects without a second source build.

Rust source was unchanged; the2696-pass full assurance is reused. The local
release build passed, the five meaningful offered HTTP protocol checks passed,
and the existing exact-f64 oracle fixtures passed. The source controller ran
from native source commit `33fa7273` with separately captured controller SHA
`64ce6d7a…`; the actual controller bytes are an immutable S3 artifact. This
construction is tied to its recorded x86_64 builder binaries. Future serving
will use its separately qualified native/HTTP binary identities.

Next run the [fixed preregistered panel](fresh-cohere-1m-preregister.md):
authenticate source and known prior query families, reject the whole panel for
raw/unit duplicates, then seal exact f64 GT100. Complete all-history query
closure remains false and must be disclosed. No CoHere1M recall or vendor
claim is established. A scoped identity GO permits one native R@10/R@100 and
8 offered QPS HTTP development gate, followed by the unchanged prospective
panel if it survives. Preserve the verified ReLAION prospective GO. BOTH-vendor
matching, peer-client cold protocol, saturation/total cost,10M/100M and mutation/
pin/recovery/compaction/GC qualification remain required.

Receipts and checks: [verification](cohere-source-1m/a0001/verification.json),
[source report](cohere-source-1m/a0001/source-build.json),
[immutable terminal](cohere-source-1m/a0001/terminal.json).
