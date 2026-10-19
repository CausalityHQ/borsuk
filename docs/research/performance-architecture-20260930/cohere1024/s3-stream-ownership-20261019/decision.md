# S3 SOURCE ownership falsifier

## Decision

Retain the qualified contiguous S3 serving path. Do not promote the segmented
prototype. The diagnostic is qualified and retained as research tooling; this
delivery does not change the production Rust tree or establish product readiness.

## Authenticated observation

One query-free range contained the first 496 complete SOURCE authentication units:
4,190,208 bytes. The v13 generation root pins the v3 plane and its complete digest
sidecar. The existing native transport used HTTP/1, no application cache,
concurrency one, an explicit pinned SOURCE HEAD and exactly one following
conditional range GET. No requests, truth, scoring or canonical vectors were used.

The valid response had 489 nonempty chunks. Its first 256 conversions covered
2,223,891 bytes: all were non-unique, all changed visible address, and all returned
exact-length Vec capacity. Zero observed bytes met the immediate zero-movement
handoff rule. F256 then recopied that entire prefix and appended the remaining
1,966,317 bytes. Total counted collector movement was 6,414,099 bytes. Capacity
normalization and crossing-record copies were both zero.

This identifies a concrete sharing-plus-fallback mechanism for this response.
It is not an upstream allocation-size/RSS proof, a population ownership
distribution, or a CPU/latency/recall/QPS result. The synthetic uniquely owned
chunk assumption did not describe this observation. The single fixed range
does not establish that every S3 response has the same ownership.

## Verification and preserved failure

Frozen diagnostic commit a4987a0e builds on the retained e578c535 prototype.
Four focused tests and 64 range regressions passed; locked workspace all-targets
Clippy correctness/suspicious checks and the actual shim-unset workspace
test-build passed. Root metadata admission, the separate seven-case mocked
local HTTP canary, and the actual witness each executed one native test and
exited zero. The debug ELF makes no timing claim.

The first actual attempt is preserved as execution INVALID. Its aggregate
transport baseline included IMDS token PUT, role GET and credential GET, yielding
four service attempts including the data request. The repair validates one
native SOURCE HEAD before the baseline and reports setup traffic separately.
The subsequent data interval still must contain exactly one GET and the exact
authenticated body length. No guessed credential count is subtracted.

## Next falsifier

Move to a quality-preserving cold concurrency test, Q1 versus Q2, using unchanged
qualified library source, corpus/query/truth bytes and the retained exact-answer
oracle. A contextless experiment packet must first verify the current runtime
configuration, source-bound working-set model and input/reader pins. Measure
cohort QPS and per-query tails together; retain valid quality/memory/latency
tradeoffs. Do not repeat broad qualification for unchanged source or claim a
vendor win from an unmatched protocol.

Cohere is only the current evaluation workload. This diagnostic adds no dataset
or dimension constants to library behavior. S3 Vectors protocol differences
remain disclosed; Turbopuffer is not measured. The cold, generic library remains
the product objective.

## Reproduction

See diagnostic-contract.md, qualification.json and result.json. The immutable
S3 prefix in decision.json contains closure.tar.gz, source.bundle, delivery.bundle
and manifest.json with SHA256/checksum verification. Source bundle requires
e578c535; its retained base bundle is included in the archive. All invalid
receipts and original qualified artifacts are preserved.
