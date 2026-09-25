# V224 graph generation S3 round trip closeout

**Decision:** retain the content-addressed graph blobs, authenticated root,
and conditional head API. The first real S3 publication and cold/warm
hydration passed its frozen persistence gate. This does not qualify query
latency or a complete generation switch.

Source commit `fa81d37d98dab534104625de1347c52d35132d17`, source archive
SHA-256 `dbd0490eaaeee814a88235f5c59f945bf17b9b6d2a82ede30f57b2021978de54`,
trusted root SHA-256
`c59650ec920d031ff236f5ab47331db88b71fdac0462cd07a568d51c549b7caf`.
The original Spot attempt was `a0001` under
`s3://borsuk-bench-453182569524-euc1/research/v224-graph-store-s3/fa81d37d98dab534104625de1347c52d35132d17/runs/a0001/`.
Instance `i-011d40be6b483860b` was c7i.4xlarge Spot in eu-central-1c
and is confirmed terminated. Terminal SHA-256 is
`f7f58a6f231a4bfedaba0aa7d02708a85a59a317b460c0ed442e192d6c740de2`;
closeout SHA-256 is
`9b7f0216b2fcde85998c84ce1777318c7285c0c3c1885576e27b465986c566b3`.
The controller and an independent readback verified all five terminal
artifacts against their recorded lengths and SHA-256 hashes.

The frozen ReLAION-1M index has 1,000,000 rows, 768 dimensions and
generation 196. The library uploaded its five blobs, root and conditional
head to a fresh S3 prefix, read that head back, loaded the graph from five
cold blob GETs totaling **1,879,697,462 bytes**, then reused the local
authenticated cache with **zero** blob GETs and bytes on the warm load.
Both loads returned generation 196. `gate_pass=true`. The GET counts exclude
the separately fetched head and root; they are artifact hydration counts.

The round-trip program's `/usr/bin/time` wall time was **69.76 seconds** and
its peak RSS was **3,927,814,144 bytes** (3.66 GiB). The harness retained the
first loaded generation while opening the second, so this peak includes two
resident graphs. It is not V223 serving RSS or a single-generation memory
measurement. The Spot quote was $0.3631/hour; compute to the terminal marker
is estimated at **$0.034129**, excluding S3 requests, storage, network, EBS
and billing adjustments.

This gate exercised S3 conditional **creation** of a head. The versioned
update CAS, ambiguous-response recovery and reader pinning across a
generation switch still require a real S3 mutation/compaction gate. Cache
retention and incomplete multipart cleanup need deployment policy. The next
product increment should switch between two authenticated generations while
old readers finish, with memory provisioned for the overlap. The subsequent
scale gate is a frozen 10M build/serve run under an explicit recall and RAM
envelope. V223 remains the latest measured query-serving performance; V224
contains no query samples and makes no new competitor claim.
