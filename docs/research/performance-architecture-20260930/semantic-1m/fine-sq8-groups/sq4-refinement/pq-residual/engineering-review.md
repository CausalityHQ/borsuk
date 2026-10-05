# Engineering review

Consultation `0331793e18774f32`, group `651b4ee78c01498d`, GPT-6 Astra; completed exit0. Reviewed prospective method at e3569b82. Native execution remains unqualified.

**Not ready to freeze for real execution.** Reviewed exact `e3569b821b97813c26d6078b03039a6229a80356`. These findings concern the prospective method and existing code reuse; they are not evidence that PQ residual refinement fails.

**Research findings**

1. **P1 — Correct the description of the native numerical baseline.** The method says native SQ8 omits the query-norm constant. It actually accumulates `qnorm`, subtracts `qnorm/2` from `shift`, and computes `norm - 2*(inner + shift)`. Its dot product is also factored through coefficients, rather than accumulated over decoded coordinates. See [native scorer](/home/rb/worktrees/borsuk-prod-ready-v9/crates/borsuk/src/exact_sq8_nominee.rs:87).

   Keep the proposed `original_norm - 2*ordered_dot(q,xhat)` unchanged, but remove the parity implication. Adding a constant or changing accumulation order can change f32 ties. The tiny oracle must independently reproduce the candidate’s literal operations and distinguish them from unchanged native SQ8 and the existing decoded-cosine control. Exact residual reconstruction must not imply bitwise native-score equality.

2. **P1 — Finish the residual-bin numerical contract before observing source results.** “Freeze policies before execution” leaves the actual trainer underspecified. Existing DP operates on occupied integer bin positions, uses integer moments and f64 interval costs, chooses the smallest predecessor on exact ties, then rounds centers to f32. It does not directly fit arbitrary residual values. See [DP implementation](/home/rb/worktrees/borsuk-prod-ready-v9/crates/borsuk/src/fine_sq8_groups.rs:4635).

   Specify residual-to-bin mapping, endpoint inclusion, arithmetic precision, center conversion back to residual units, nearest-center ties, constant axes, and collapsed centers after rounding. Pass 3 must choose among final centers using the **actual residual**, as proposed. Independently enumerate tiny weighted partitions using direct squared deviations; add boundary, constant-axis, cancellation, and center-rounding cases. The historical histogram SSE check is not proof about actual residual distortion or recall.

**Engineering findings**

3. **P1 — Reuse the DP algorithm, not the existing histogram encoder/scorer unchanged.** The histogram encoder overwrites the source norm with a reconstructed norm, and its scorer implements the old coefficient-based convention. See [encoder](/home/rb/worktrees/borsuk-prod-ready-v9/crates/borsuk/src/fine_sq8_groups.rs:4477). PQ’s cosine scorer also multiplies by predictor and query inverse norms: [PQ scoring](/home/rb/worktrees/borsuk-prod-ready-v9/crates/borsuk/src/pq64_nominee.rs:263).

   The residual codec must copy the original ID/norm bytes, reconstruct raw codewords directly, and use native SQ8 query preparation—including its near-unit shortcut. Required scalar cases: nonunit and near-unit queries, zero/exact residuals, negative coordinates, near ties, odd nibble padding, and dimensions below/nonmultiple of 64. Independently verify floor subspace boundaries and padded book offsets.

4. **P1 — Resolve the output layout and cumulative budget before body reads.** Two complete FIRST100k payloads require **79,200,000 bytes**, exceeding **64 MiB by 12,091,136 bytes**, before books, rankings, or receipts. Existing paired output admission explicitly includes both complete payloads: [output model](/home/rb/worktrees/borsuk-prod-ready-v9/crates/borsuk/src/fine_sq8_groups.rs:6026).

   Freeze whether the source-only probe retains cohort payloads or uses full payloads as bounded scratch with declared cleanup. Preserve the caps. Account for both panels, all three passes, authentication/rechecks, PQ decoding, DP, 16-center encoding, scoring, sorting, serialization and closure.

   Do not accidentally reset the allowance through existing wrappers: [histogram fit creates a fresh guard](/home/rb/worktrees/borsuk-prod-ready-v9/crates/borsuk/src/fine_sq8_groups.rs:4315), while the corrected diagnostic has separate construction and query budgets. The new attempt needs one cumulative 20B ledger and one 600-second deadline. Budget failures remain INVALID, with the unmet bound reported.

5. **P1 — Make binding and terminal-state requirements executable acceptance gates.** The method names the right bindings, but needs explicit falsifiers before those declarations can authorize execution:

   - **Mapping/selection:** use a nonidentity permutation to verify logical ordinal → inverse physical mapping → matching SQ8/PQ row. Recompute `floor(j*N/4096)` and anchors independently; exclude self before selecting exactly 100 unique IDs; freeze `(score.total_cmp, ID)` ordering.
   - **Authority:** reject same-size substitutions of PQ, order, coefficients, source norm, books, payload groups and codec/root marker. Bind the new codec/shared-DP source into the implementation identity. Existing [manifest validation](/home/rb/worktrees/borsuk-prod-ready-v9/crates/borsuk/src/fine_sq8_groups.rs:185) provides relevant checks, but its old schema alone cannot identify residual rows.
   - **Pipeline:** instrument opens to prove requests/GT remain unopened; reauthenticate all three source passes with exact EOF checks. Inject between-pass mutation, existing-output collision, partial write, file/directory sync failure, sealed-result corruption and a late deadline failure. No surviving report may authorize promotion without matching exit, resource, drain and cleanup receipts. Existing [late-failure tests](/home/rb/worktrees/borsuk-prod-ready-v9/crates/borsuk/src/fine_sq8_groups.rs:7268) are useful patterns, not coverage of OWN5.

The closed evidence supports retaining all incidental fetched rows: CoHere nominee coverage is only `.95609375`, while passing SQ8 rankings recover 254 truth hits from incidental rows. Preserve every completed REJECT. Keep the separate-panel **6336/6400 and fourth-smallest ≥98** mechanism gate and subsequent unchanged scientific gates.

The 100M graph residency and `4N` query workspace remain unresolved. Mechanism survival would establish only a bounded numerical result, not product completion.

No edits, builds, native/data execution, GT access, AWS actions, children or consultations were performed.
