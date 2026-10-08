# engineering_critic

Review group: dd1e505d37bc425a
Consultation: 4ab6afe0d0254580
Reviewed source: 64d519b0da5d0d0d9eb0c4e415ec8a9f244d50ef
Status: completed; exit 0
Native execution: not performed.

**Verdict: repair 1 before trusted numerical replay. No static compilation blocker found.** Reviewed exact `64d519b0`, including its full diff from `7c11957`; line references below refer to that commit.

1. **[P2] SQ8 validation accepts physically impossible charges.** At [compare_native_replay.rs:1650](/home/rb/worktrees/borsuk-prod-ready-v9/crates/borsuk/examples/compare_native_replay.rs:1650), byte validation requires only a multiple of `dimensions + 12`. For the existing 512-row, D16 fixture, `submitted_gets=1, verified_bytes=28, failed_gets=0` passes these checks, although the historical reader authenticates whole 256-row pages: the minimum successful payload is **7,168 bytes**. With query/terminal totals and seals updated consistently, the shared reader adds no rejection. The report then uses that impossible charge in its cost delta.

   Validate whole-page accounting with the clipped-tail exception, and ensure successful GET counts are compatible with the fetched page count. Add an authenticated negative fixture with consistent totals and seals. This is a concrete validation gap; it does **not** establish that pinned B1 contains incorrect charges.

The remaining reviewed logic supports proceeding after that repair:

- Historical seed completion and sorted walk reconstruction match. Closure equality, ordered completion, the 2,544-unit cap, and partial tails are checked.
- The unchanged shared cover helper selects the cheapest gaps. Its 32-row calculation under the same 128-GET allowance is a valid optimistic hindsight byte bound.
- Direct SQ8 costing uses the complete closure, 256-row pages, `D+12` and 32 GETs. Broader scoring populations and extra bytes are explicitly disclosed; production caps remain unchanged.
- Source padding matches the historical codec. Recursive duplicate rejection, exact identity pins, full 1,000-query/seal/terminal/SHA validation, descriptor checks, report limits and create-only output remain intact. Old modes retain their no-op callback path.
- Prior packing, centroid and SHA/SIMD dispositions remain closed; this accounting provides no evidence to reverse them.

**Not checked:** compilation, test execution, Clippy, runtime resource limits or actual B1 reduction. No files, real traces, vector bodies or external systems were opened or modified beyond the authorized Git source/document inspection; qualification remains unproved.
