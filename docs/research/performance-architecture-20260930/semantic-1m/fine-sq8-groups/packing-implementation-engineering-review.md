# Packing implementation review

Reviewed source: 4d1e0de32bed9e354b9c396d972d2a235738bca4.
Review group: c4ff6ab0379b403a. Role: engineering_critic. Effective model: gpt-6-astra. Original consultation exit: 0.

Reviewed frozen **`4d1e0de32bed9e354b9c396d972d2a235738bca4`** against `c79700e5`. **No concrete static implementation blocker found. One qualification test gap remains.**

1. **P2 — The seven new tests do not protect replay’s actual 32-range rejection gate.**
   Replay supplies `32` at [fine_sq8_groups.rs:2437](/home/rb/worktrees/borsuk-fine-sq8-graph-packing-falsifier/crates/borsuk/src/fine_sq8_groups.rs:2437), but the integration fixtures contain only 83 or 135 rows at D2: their entire payloads are **1,162 or 1,890 bytes**. Both survive regardless of using 32 or 256 ranges. The large-gap assertions at [fine_sq8_groups.rs:2986](/home/rb/worktrees/borsuk-fine-sq8-graph-packing-falsifier/crates/borsuk/src/fine_sq8_groups.rs:2986) call `mapped_cover` directly, so changing replay’s literal to `256` would evade these tests.

   **Smallest correction:** add one direct `replay` fixture with identity maps, D768/100k geometry, and 128 plans selecting 500 groups spaced three apart. Distribute 1,024 unique nominees within those groups. Independently calculated costs are **17,921,280 bytes at 32 ranges** versus **12,330,240 at 256**. Assert 128 result entries, `[0,0]` passes, and `survived=false`. Then corrupt the last event: replay must return an error despite earlier locality failures. This tests the production replay call and prevents premature REJECT without building a large graph.

Static inspection supports the following:

- Directed base arcs contribute symmetrically with multiplicity; accumulated affinity, ID ties, score reset, tail exclusion and old-to-new translation match the frozen method.
- Replay checks all 128 plans with explicit cap 32 and the unchanged 16 MiB constant. The supplied real-prefix visit/evaluation maxima satisfy its guards.
- Both maps are published and synced before prefix opening. Prefix reads are bounded beneath buffering; I found no production path opening forbidden payloads.
- Original seal → root → graph bindings and separate current-source/original-trace identities are present. Output creation uses no-overwrite semantics; persistence errors propagate nonzero. Supervisor admission still depends on the caller authenticating the original receipt.
- Existing nomination/scoring code is unchanged. The shared decoder preserves the previous base-reachability requirement.

**Evidence limits:** the arithmetic above was independently evaluated; implementation conclusions are static. `git diff --check` passed and the candidate remained clean. Compilation, Clippy, all seven tests, resource behavior and supervisor execution remain **UNRUN/unverified**. No files were edited; no Cargo, native, cloud or large-body reads were performed.
