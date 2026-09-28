# Decision: callable compaction functional gate passes

GO for the experimental Rust library increment; no quality/performance/vendor win.

`compact_two_bit_index` now connects authenticated current head/delta, irreversible
CAS seal, canonical/delta merge, native SQ8/build and conditional publication.
A durable ready point resumes failed publication without rewriting pinned SQ8;
empty pending puts become populated, and deleting all rows yields a true empty
root. One maintenance directory/file lock owns an in-process worker. Successful
publication cleans local staging, with recognized obsolete-job retry cleanup.
Remote reader-safe GC remains missing.

AWS causality, eu-central-1, c7i.4xlarge Spot only. a0001 compiled successfully but
failed an assertion counting canonical and SQ8 uploads together (2 versus1).
The correction selects the authenticated target's SQ8 key. a0002 passed both
integration tests and4 existing HTTP-layer tests. Red compilation exit101 from
a0001 proves the prior source lacks the new API. No repeated red build in a0002.

Original a0001 `i-0d244fa14d7e9d5ab` exit101, verified terminated,232s;
a0002 `i-071278e86ff40adfb` exit0/complete, verified terminated,237s.
Combined compute estimate $0.0471 from observed Spot quote/wall; excludes EBS/S3,
not an invoice. Receipts and SHA-authenticated logs are stored per attempt.
Green source SHA575cd350efed6c03061b4d1e0c74bc1c14f2ec724c5ba4ee8c98fe4c98e3202a;
all three changed Rust files match its immutable source archive.

Functional fixtures are512 two-dimensional source rows and tiny replacement
bases; HTTP tests exercise the existing reader. These are not independently
sampled recall, latency, throughput,100M scale or lifecycle-cost benchmarks.
Payload caps are not RSS guarantees. Local staging is trusted writer state;
separate directories/processes need coordination. Failure after sealing can
pause writes until adequate-cap retry. Dropping the awaiting future does not
cancel its owned worker. Remote orphan reclamation is unresolved.

Next single product gate: reader-safe callable remote GC with explicit pinned
root protection and failure/reload checks; do not delete old-generation objects
from the current head alone. Both-vendor matched quality/e2e/cost gates stay OPEN.
