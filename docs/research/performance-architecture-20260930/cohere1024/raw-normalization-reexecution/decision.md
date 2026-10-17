# Raw-to-normalized positional mapping: source-bound reexecution

The raw-to-normalized positional mapping link is closed by a fresh source-bound reexecution of the original normalization API, followed by successful verification of the existing execution. This is one provenance link, not complete vendor admission.

## Exact evidence

On Causality EC2 `i-0f168289eda791b07` in `eu-central-1`, the qualified producer from commit `7acbbb82d48078f46c0a163614db39eeee15afc7` (parent prefix `a666`) has ELF SHA-256 `d1b001f2aadffb2e36b10f081e47ff03dce6f66f290df9aaa48c8d441ff5f074`. Release evidence is `evidence/source-block32-packed-release-a0001`.

The API source is unchanged: `crates/borsuk/examples/build_sq8_source.rs` SHA-256 `a7ba542c3ada061002b09e638a68423236d5499d11aae0e0083cd6e52d38adfd`; `crates/borsuk/src/sq8_source.rs` SHA-256 `16f03aa7187b4f18676aa511339b07b6e7b8776323710a60071e6ea0d62eb795`.

The native API authenticated the complete raw 1,000,000-row, D=1024 input while streaming: 4,096,000,000 bytes, SHA-256 `1a491c060ec8b668b4983042dc1ac3971456327f41f28fd6fd2449df24cf3e38`. Its normalized output SHA-256 is `31f28fa211f9237f0f7e41f569773cd207cefcdce22d4932ec96aee01ff2c40d`, matching native stdout and two independent SHA-256 readbacks. Raw stamps match before, after, and at follow-up; executable hashes match before and after.

## Preserve the failed wrapper and successful native result

Closed metadata in `evidence/raw-normalized-reexecution-a0001` records native exit **0** and wrapper (`runner.exit`) **1**. The original wrapper receipt remains **INVALID**: its final source check ran from the wrong working directory, after native success, correct output, and raw/executable stamp comparisons. This document does not rewrite that receipt as successful.

The separate closed `evidence/raw-normalized-reexecution-verification-a0001` records verification exit **0** and terminal status `VERIFIED_EXISTING_NATIVE_EXECUTION`. Its correct-directory source check, executable check, normalized check, raw stamp comparison, and stdout check succeeded **without rerunning native normalization**. The local follow-up `terminal.json` SHA-256 is `659bb97e1566ab77424bde024c216164f915712289dd0539b912697beb52da99`. Per-file metadata hashes are in [verification.json](verification.json).

## Archived evidence and claim boundary

The original assignment supplies archive key `research/semantic-router/20261017/source-block32-packed-closure-a0001/normalization-evidence.tar.gz`, SHA-256 `5806b60045ae0ba5653899d9823befac5477e18a8262f28a18c20cf785695970`, and root archive `terminal.json` SHA-256 `6b76bed1cf00e51bb1d1adb7a4a1372de422664fc25e840aff294f764d1eabe4`. The root had already HEAD-verified both S3 objects. That root terminal digest is distinct from the local follow-up terminal digest above; this task made no new S3 API calls.

This is a fresh source-bound reexecution of the original normalization API. It does not recover the original producer ELF with prefix `4f502...`, provide an independently implemented arithmetic oracle, or establish new ANN, quality, performance, or vendor-win results. Query and truth remain unopened. The live full1000 ANN measurement `borsuk-block32-packed-full1000-abba-a0002` is outside this evidence slice.

This documentation task read only closed metadata, not raw or normalized payloads or incomplete measurement JSONL. It ran no native work, builds, or tests and changed only these two documents. Verification is limited to metadata consistency, JSON parsing, and the static two-file Git diff.
