# Native scale parity repair — prospective specification

This addresses the independently verified missing native cross-run gate. It is not implementation or qualification. Preserve all historical reducer modes and source/result artifacts.

## Exact gate

Compare three independently SHA-bound, fully sealed v7 native runs: historical1M local count32/full; fresh1M local count1000/diagnostic_panel ordinals0..31; fresh1M S3 count1000/the identical panel. The local panel is a correctness admission and must not be used to warm the subsequent fresh-process measured S3 pass.

Reuse the existing strict completed-scale reader for every arm: complete expected identity and bound-input rows, duplicate-key rejection, exact bytes/hash, descriptor stamps, query seals, recall rows and terminal reconciliation. Reject duplicate files/artifacts. Do not rewrite, strip or reseal existing JSONL.

Require ordered IDs AND f32 score bits, hits10, underfill and router/source/SQ8 logical charges to match at every one of32 query ordinals. Trace=false for both new panels; no trace digest equality across changed root/backend scope. Timing, physical transport and SDK credentials are deliberately not parity invariants. Report them separately without a speed or cold claim.

Require the same producer executable and all runner/library component source hashes, corpus geometry/intervals, cosine/tie rule/k, normalized corpus/order/SQ8 hashes, reserved-query digest and resource/fetch/scoring policy. Each differing backend, generation root, receipt and execution field remains independently pinned; do not broadly remove arbitrary fields before comparing.

The historical/full-query request and truth relationship needs native exact byte-prefix authentication: historical131072 request bytes equal the first131072 bytes of the4096000-byte full file; historical2560 truth bytes equal the first2560 bytes of the80000-byte full file. Authenticate complete lengths and whole-file hashes first, preserve descriptor stamps and refuse truncation/growth/mutation. Only after all three runs are sealed may the comparison open truth. Do not infer this relationship from aggregate recall or unrelated100k truth.

Generation semantic equivalence remains a separate source-bound gate: authenticate freshly derived immutable component hashes against accepted1M artifacts and all low/step coefficient bits; use the retained publication receipt to bind source root to actualS3 destination root/ETag. Comparison output must explicitly state external generation/provenance qualification required; matching returned hits is not proof that arbitrary roots are equivalent.

## Required falsifiers

Changed ordered ID; changed score bit; changed per-query logical GET/verified-byte/failure count; duplicate or reordered ordinal; wrong full-query prefix; wrong whole-file hash; short/grown/mutated inputs; duplicate evidence inode/hash; changed corpus or source component; changed fetch/scoring/resource policy; unsealed/partial/nonzero native run; unknown/missing configuration fields. Include one positive actual native tiny pipeline fixture and reject mismatch before success publication. Keep historical reducer tests intact.

## Qualification and execution

One Rust example source change is expected, with focused tests in that target. First compile and run the exact affected native tests, release example, workspace/all-target correctness+suspicious Clippy and actual shim-unset test-build on causality EC2 Spot with bounded jobs1. No local Cargo/native/mock/validator execution. New reducer binaries and source identity must be frozen before admission or measurement; unchanged ANN binaries retain their original qualification.

Review synthesis is still pending groupf2d0e09175384181; this specification must be reconciled with its full research answer before implementation. No new paid run is authorized by this document alone.
