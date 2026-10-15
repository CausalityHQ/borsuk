# Fixed-panel SQ8 reader qualification

Qualified Rust source: `4fe2c1349922b7f5862e9b2568ae2fb70fab2417`.
This qualifies the diagnostic reader, not ANN quality, latency, a production default, or a vendor win.

| Gate | Verified result |
| --- | --- |
| New exact tests | Nine names, each selected once in debug and release |
| Complete reader suite | 270 passed, zero failed or ignored |
| Workspace Clippy | correctness and suspicious groups exit 0 |
| Complete test compilation | actual shim unset, bounded compiler concurrency, exit 0 |
| Release | exact retained ELF and archive hashes verified |
| Source | 478 package files / 445 native files unchanged before and after, bound to Git |
| Infrastructure | original instance terminated, volume absent, external timer inactive |

Original native SSM: `74c2433f-7d3c-42e0-85d7-1f03f2166969`.
Original instance: `i-0d098044b92ed6ec8`; volume: `vol-060638d80fefece36`.
Evidence archive: `s3://borsuk-bench-453182569524-euc1/research/semantic-router/20261015/sq8-panel-reader-gates-a0001/terminal/qualification-evidence.tar.gz`.
Archive SHA256: `a72104483baa6b2af101ab19790df99afcabf05753b9f212b816419cbe995c12` (787401 bytes).
Release archive SHA256: `95a6e0035ae8183e6bfcb127c363c65120115e3bd765cd217f3b997032c0a2b0` (13066240 bytes).
ELF SHA256: `f456ea8a94f983a2266d3df661a7b4dfe120e607b79b13f968687d2af510880a`.

The engineering review's three contradictions were repaired: impossible body-gap timing, understated diagnostic reservation, and inconsistent query CPU anchors. The research reviewer returned only a quota banner; no second substantive review is claimed. Both original results are retained.

A separate bounded EC2 analysis authenticated the historical traced candidate output and confirmed its emitted reservation: 534097 pinned bytes + 90720 trace bytes = 624817 total. It issued zero new ANN queries. Its first wrapper attempt failed before reading data because SSM selected `/bin/sh`; the corrected Bash wrapper passed. This is accounting evidence from the candidate, not a new performance result or an observation of the original actor.

The root verifier initially rejected the valid, empty stdout log from successful tar retention. The original trace is retained; requiring a regular log file while keeping exit, command, timing and artifact checks corrected that metadata-verifier defect. No native rerun occurred.

`stage-summary.json` preserves exact command text and completed compiler/test resource observations. These are not serving latencies or query RSS. Raw native logs and timings remain unchanged in the S3 archive. `launch-inputs.sha256` is the original launch-directory manifest, not a manifest of this evidence folder.

Next: finish and validate the fixed32 runtime packet, pass actual-input admission and separate canary, then run the preregistered diagnostic without selecting replacement queries. The source-only runner handoff is not yet runtime-qualified. Its initial all-remaining-stage reservation exceeds the proposed host lifetime and must be corrected before launch. No new scientific run is admitted by this decision.

The product goal remains incomplete, including matched competitor results and lifecycle qualification.
