# Native centroid reconstruction qualification

Exact source: `91107713f6f7bd700b5b6d92a83e438febac8069`, based on qualified spatial library `483b7f69`. This ships an experiment-private Rust `--partition-reconstruction CONFIG CONFIG_SHA APPROVAL APPROVAL_SHA` mode. It does not change production routing defaults or restore a historical serving reader.

| Actual native gate | Original attempt | Result |
| --- | --- | --- |
| Filtered reconstruction tests, debug | a0001 | 10/10 passed, exactly once |
| Filtered reconstruction tests, release | a0001 | 10/10 passed, exactly once |
| Complete affected example tests | a0001 | 137/137 passed; 0 ignored/failed |
| Release example build and retained ELF | a0002 | exit 0; ELF SHA authenticated |
| Locked workspace/all-target Clippy correctness+suspicious | a0002 | exit 0 |
| Actual environment-unset complete workspace test compilation | a0002 | exit 0; compilation only |

Every stage has full 464-file source before/after/root equality, the same 431-native-file identity, actual command, native/manager exit 0, enforced CPU0–3/400%, 16 GiB/swap0/jobs1 compiler envelope, no OOM/swap, and owned-cgroup drain. Rust1.98.0, target, manifest-default features, unset flags/wrappers and compiler configuration match across both attempts. The actual release ELF is retained at the immutable S3 key in `qualification.closed.final.json`; its 8,387,384 bytes hash to `739e703efe1c6ac1364c4c1173a8c8bcba4929d7466a9a088ceb16d268e73f52`.

## Original failure and correction

Original a0001 SSM `8ff109ef-4471-4846-b22f-664b5438383e` remains **INVALID/setup, exit 42**. The root's static scanner expected 124 tests and omitted 13 existing `#[tokio::test] async fn` tests. Original native output passed all 137. The source-derived corrected roster was independently compared against those original closed native results; no passing test was repeated and no failing test was waived. Original 124 roster, actual names, batch error and corrected 137 roster are retained. Continuation a0002 SSM `193ba1f1-76ba-422b-852f-0c6b32f38b8d` ran only the three genuinely unrun build/lint/compile commands and closed 0. Both instances are terminated and both delete-on-termination volumes are proven absent.

## Implemented authority and parity boundary

An independently SHA-approved document binds original generation/epoch/plane/Discovery/construction schema/root/centroid/profile/geometry. Stage 1 streams authenticated FP16 unit records, refuses nonfinite values, duplicate/out-of-range IDs, length/hash/EOF failures, scatters into original ID order and requires the exact original centroid SHA. Requests, truth, historical root and membership stay unopened during reconstruction.

Stage 2 authenticates the whole historical root/membership/leaves, extracts its original modeled construction limit, invokes the actual current radial builder, validates and opens the actual new publication, and compares every byte. Only BORSUSR2→BORSUSR3 magic and mode 0→1 at the declared header offsets may differ; membership/payload must match exactly. No patched historical image is used as serving validation proof. Create-only output, input/output descriptor stamps, checked file/containing-directory synchronization and receipt-last publication are required. Sync failure, directory replacement and identical-byte rewrite-after-sync falsifiers cover both output modes.

The original engineering and research reviews are preserved. Their findings were repaired in separate checkpoints: complete sync-position coverage, frozen arithmetic/export/build dependency pins, and pre/post synchronization identity checks. Native qualification applies to final source, not earlier unverified checkpoints.

## Next falsifiable gate

**Real retained Cohere 1M/D1024 centroid reconstruction and strict historical radial publication parity remain UNRUN.** The draft original approval expects centroid SHA `720b98899031e1aeeeabe0fb2aa0d938485b43141c2438c251b3383a6f0315d8`. Execute source-bound admission and a separate disposable CLI/asset/cleanup canary on causality EC2 before the frozen real parity gate. Keep queries/truth absent. Preserve any original error; environment/setup failures are INVALID rather than an algorithm quality KILL.

After real parity passes, the next separate Rust paired slice must authenticate original source authority and derived radial/spatial publication authority independently, reproduce original exact radial selections and evaluate the frozen consumed32 SOURCE screen including displaced losses and physical closure. That screen is not held-out recall or a cold-performance comparison. Historical S3 Vectors/Turbopuffer references remain unchanged; this compiler/synthetic qualification supplies no new recall, p90/p95, QPS, lifecycle cost, 100M admission or product-ready claim.

## Receipt access

Small original stage bodies and compiler fingerprints are copied here. Original S3 body manifests cover the complete immutable stage roster, including the retained ELF omitted from Git. Their namespaces are `s3://borsuk-bench-453182569524-euc1/research/semantic-router/20261011/reconstruction-native-qualification-a000{1,2}/`. Whole-body/source checks were performed on complete downloaded originals before this delivery snapshot; the source archive and ELF remain in those namespaces. Historical logs and review findings are evidence with their original status/date, not current instructions.
