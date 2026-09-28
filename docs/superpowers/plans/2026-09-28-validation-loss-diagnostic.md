# Historical Validation Loss Diagnostic Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox syntax for tracking.

**Goal:** Identify discovery, nomination and physical-fetch losses on the rejected ReLAION layout without changing its recipe or calling consumed queries fresh.

**Architecture:** Reproduce historical code `2db5b8ff192cd310bb67edca47d4915c15087c17` on one AWS Spot worker. Require the recorded raw, normalized, order, SQ8, root, truth and plan hashes before interpreting traces. Use the existing trace API and closed returned-score receipts; no new route or production change.

**Tech Stack:** Existing Rust APIs, pinned NumPy 2.3.3/PyArrow 24.0.0, stdlib Python, boto3 and the existing bounded Spot launcher.

**Spec:** `docs/research/validation-loss-diagnostic-20260928/config.json`; causal question and corrections in `docs/research/next-route-research-20260928/decision.md`.

## Global Constraints

- AWS profile `causality`, eu-central-1 Spot; no DGX/Spark or local builds/tests/benchmarks.
- One worker, four compile/BLAS threads, 2700-second instance limit (3000-second controller limit); compile 1200 seconds, replay 900 seconds. Spot maximum $0.30/hour; compute estimate cap $0.30, excluding EBS/S3; no automatic replacement.
- Historical generation is an experimental format-v1 artifact, isolated from current production readers/defaults. Never weaken current version rejection to open it.
- Stop before query execution on any source/root identity mismatch; never change seeds, arithmetic, compiler flags or thresholds to make reproduction match.
- ReLAION first100k D768 cosine k100, consumed development 0–63 and validation 256–999. No CoHere validation extension, scale promotion or vendor claim.
- Physical plan remains at most 32 ranges and 16,773,120 bytes; report planned I/O, not physical S3 query measurements.
- Monitor only terminal markers and EC2 health. Authenticate closed artifacts and terminate immediately.

## Review Focus

- Use ARM Graviton to match the historical ARM host; cross-machine floating arithmetic can change fit/root bytes: mismatches invalidate reproduction before query access.
- Merged ranges include gap pages: candidate, nominated and physical coverage must be separate, including a gap bonus.
- Missing truth object: regenerate with the existing V282 routine and require the registered 400,000-byte truth SHA; no GT in source fitting.
- Wrong trace order/count or source ID permutation: reject rather than silently align rows.
- Partial/interrupted output: no diagnosis without complete traces, original-plan hash parity and matching fetched counts; discard interrupted cell.

### Task 1: Reproduce and decompose one historical cell

**Files:** Create `docs/research/validation-loss-diagnostic-20260928/probe.py`, `aws-check.py`, receipts and decision. Correct the malformed terminal digest in `docs/research/v282-100k-attempt-ledger.md` using the independently authenticated original closeout. No production source edits.

**Interfaces:** Consume registered config and historical source archive. Produce complete JSON traces and per-query discovery/nomination/fetch/returned counts, input hashes, process time/RSS labeled as diagnostic, and an authenticated terminal.

- [x] Write one runnable self-check: with GT pages `[0,1,1,2]`, candidates `[0,1,2]`, nominations `[0,2]`, fetched pages `[0,1,2]`, require coverage `(4,2,4)`; reject nominations outside candidates or physical coverage. Preserve a missing-implementation red snapshot, then implement the set membership helper.
- [x] Prepare the historical git archive and current harness archive. Review the complete harness read-only before launching. Expected: no concrete methodology or identity/budget defect remains.
- [x] Run red then green self-check on AWS; build only the historical normalizer/fitter/builder/planner. Extract registered Parquet raw rows and reproduce normalization/order/SQ8/root. Expected: all hashes match or an explicit invalid reproduction, with no queries executed on failure.
- [x] After source admission, authenticate requests, regenerate registered V282 truth and require its SHA. Replay development trace and match original plans and trace hash. Replay validation and match original plan hash. Expected: exact recorded identities, counts and budgets.
- [x] Derive GT page membership using authenticated SQ8 IDs, verify it equals the authenticated order, and verify every physical fetched count against the closed score receipt. Separate candidate loss, nomination loss and gap bonus; use existing returned counts without re-scoring or fitting. Expected: complete counts with the accounting identity `100 - discovery_loss - nomination_loss + gap_bonus == fetched_hits`.
- [x] Collect the original terminal, confirm termination, verify source/artifact hashes, record the decision, commit and fast-forward push. Task completion uses this receipt verifier, not another paid replay. No full workspace rerun is needed because production code is unchanged.

## Execution ruling

The operator has already authorized autonomous research and inline execution toward the full goal. Continue without another approval request. Existing isolated worktree is verified. The source hashes provide the reproduction gate; a failed gate is useful evidence and does not authorize tuning or a new architecture arm.
