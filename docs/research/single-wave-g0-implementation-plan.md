# Single-wave G0 returned-recall implementation plan

> **For agentic workers:** Use superpowers:executing-plans to implement this plan task by task. Steps use checkboxes for tracking.

**Goal:** Measure two-bit versus exact returned top-100 recall on the same authenticated rows selected by the closed ReLAION-100k development code wave.

**Architecture:** Reuse the terminal-closed group code bytes and the 1,000 frozen group-range plans. On a Causality Spot worker, authenticate all input objects, score exactly the selected rows with the old primary and paired exact formulas, and count distinct returned truth IDs. Publish a new immutable terminal, read back its artifacts and independently replay the decision.

**Tech Stack:** Python 3, NumPy, PyArrow, boto3, existing BORSUK cell/Spot helpers.

**Spec:** `docs/research/single-wave-code-serving-design.md`, gate 1.

## Global constraints

- Do not alter the old 100k terminal, plans, code files, source layout or query cohort.
- Historical ReLAION-100k development primary/exact page-containment is 98.418%/98.515%, not returned recall.
- Candidate rows are the exact old group ranges; no new selection, row omission or truth-aware decision.
- Every result names ReLAION-100k development, 1,000 queries, 100 GT100 IDs/query and the paired exact control.
- Heavy scoring runs on Causality Spot under the same 3-GiB sampled worker cap, with zero remote swap; no local full suite.
- Immutable attempt prefix, source archive identity, terminal marker, independent readback and prompt instance termination are mandatory.

## Review focus

- A group byte range has the expected offset/length but a wrong digest: abort before scoring.
- Two rows tie in float32 score: return the smaller source ordinal first, on both arms.
- A query's group range repeats or overlaps: reject the plan rather than double-count a row.
- Truth IDs are missing or duplicated: reject the cohort before reporting recall.
- A Spot worker fails before terminal: mark the attempt ineligible and do not reuse its prefix.

## Task 1: Pure returned-ID scoring

**Files:** Create `scripts/native_two_bit_returned_recall.py`; create `scripts/test_native_two_bit_returned_recall.py`.

**Interface:** `ranked_hits(scores, source_ordinals, stable_ids, truth_ids, top_k=100) -> int` accepts one arm's scores and the same candidate roster for both arms. The replay adapter calls the existing `score_records` and `exact_scores`, then `ranked_hits` twice. This keeps the ranking unit importable without NumPy on the memory-pressured devbox.

- [x] Write a focused test where exact and code scores swap one of three truth IDs at `top_k=2`, plus a tie resolved by source ordinal. Assert both returned counts.
- [x] Run `python3 -m unittest scripts.test_native_two_bit_returned_recall`; observed the expected missing-module error.
- [x] Implement `ranked_hits` with `sorted(range(n), key=lambda i: (float(scores[i]), source_ordinals[i]))`, unique-source and finite-score checks, and a set of selected stable IDs. Do not reuse page-nomination counts.
- [x] Run the focused unittest (3 passed) and offline Ruff (passed). Commit the verified unit.

## Task 2: Closed-plan adapter and source authority

**Files:** Create `scripts/native_two_bit_returned_replay.py`; create `scripts/test_native_two_bit_returned_replay.py`.

**Interface:** `selected_positions(sample, codes) -> tuple[int, ...]` reconstructs physical row offsets from `codes.page_row_counts`, verifies every `sample.group_ranges` tuple against `codes.group_ranges` and the old 32-GET/16-MiB limits, and rejects overlap. `replay(root, out)` authenticates the old terminal/evidence/code seal/groups/mean against their pinned identities and the four `FROZEN_INPUTS`, then computes per-query paired returned hits and canonical evidence/result files.

- [x] Write tests for valid disjoint ranges, repeated groups, a wrong digest, and accounting drift against the old byte cap.
- [x] Run the focused test to observe the missing-module failure, implement `selected_positions`, then rerun it (3 passed; Ruff passed).
- [x] Implement `run_closed_replay` with `read_two_bit_codes`, `read_two_bit_evidence`, `_read_inputs` and `_read_queries_truth`. Bind each old artifact to the complete terminal SHA and receipt before it is used. Assert all 1,000 query ordinals, source IDs and truth IDs; reuse the existing float32 scoring formulas and source-ordinal tie rule.
- [x] Write canonical per-query primary/exact returned GT100 hits, paired losses, p05 and sub-90 counts. GT10 was removed from this bounded scorer gate because the design decision is Recall@100 fidelity; the independent serving gate still requires Recall@10. The rank-boundary test proves returned hits can change on a fixed fetched roster.
- [x] Run focused unittest (8 passed) and offline Ruff (passed). Commit the verified adapter.

## Task 3: Spot cell and independent closeout

**Files:** Create `scripts/launch_native_two_bit_returned_replay_spot.py`; create `scripts/validate_native_two_bit_returned_replay.py`; create focused tests for both; update `docs/research/single-wave-code-serving-design.md` after the terminal closes.

**Interface:** `launch_and_monitor(plan)` uses the existing Causality Spot bootstrap, archive and termination helpers but a unique create-only output prefix. The worker fetches pinned old artifacts and frozen source/query/truth inputs, runs only the new replay, uploads result/evidence/resource/validation files and a complete terminal. The validator independently ranks selected rows using a separate reference sort and compares every per-query hit count.

- [x] Add launcher tests for Spot specification, terminal roster and artifact readback; add a validator test that rejects changed returned hits or decision. Fail-closed output prefix and capacity handling are implemented in the controller.
- [x] Implement the remote worker with bounded process-tree RSS/swap monitoring, exact source archive, terminal upload on both success and failure, and `finally` termination. Interruption invalidates the cell and uses a new ordinal.
- [ ] Run only the launcher/validator focused tests and offline Ruff; inspect the worker script and launch specification. Commit and fast-forward push to `origin/main` before the Spot launch.
- [ ] Launch one immutable attempt; while it runs, watch only terminal and infrastructure state. On terminal, collect the original controller exit, confirm instance termination, and independently authenticate/replay all terminal-listed artifacts.
- [ ] Apply the preregistered G0 decision: paired mean loss ≤0.25 percentage point and primary marginal p05 at most one hit below exact; report p95 paired per-query loss and sub-90 counts. Record measured units and limits without extrapolating to 1M. Commit and fast-forward push the closeout.

## Self-review

The plan covers the scorer, old-plan and input authentication, immutable remote execution, independent replay, and the single G0 decision. G1 page layout, 1M routing and product serving are outside this bounded plan and remain in the design contract. The run must fail closed if the old source and artifact identities do not match the pinned terminal.
