**Concrete blocker at `a304e580`; fix before native qualification.**

1. **The SQ8 planner accepts a valid two-bit authority and returns incorrect byte ranges.** At [budgeted_page_rank.rs:86](/home/rb/git/borsuk/crates/borsuk/src/budgeted_page_rank.rs:86), row width is unconditionally `dimensions + 12`. However, `PageAuthority::load_two_bit` constructs the same public type with a different record width. An existing `is_sq8()` accessor distinguishes them at [sq8_page_authority.rs:218](/home/rb/git/borsuk/crates/borsuk/src/sq8_page_authority.rs:218).

   **Smallest decisive falsifier:** reuse the existing two-bit fixture at [sq8_page_authority.rs:317](/home/rb/git/borsuk/crates/borsuk/src/sq8_page_authority.rs:317): 33 rows, D5, 10-byte records, 32-row pages. Call the new planner with ordinal `[32]` and limits `{max_gets: 1, max_bytes: 17, max_planner_bytes: 4608}`. Source tracing gives `Ok` with range `544..561`; the authority’s actual tail is `320..330`, and its entire object is 330 bytes. This produces a successful but unusable physical plan; downstream fetching or authentication fails.

   **Required minimal fix:** add `!authority.is_sq8()` to the initial `InvalidGeometry` guard, before allocation. Add one regression assertion that the valid two-bit fixture is refused. No authority redesign is needed.

No additional source blocker found in nominee retention, shortest-gap selection, deterministic ties, bridge charging, or partial-tail clipping. For an SQ8 authority, loader validation establishes positive geometry and checked total object size.

Scratch admission precedes collection allocation. Tree construction, run/gap vectors, join set, and output overlap remain proportional to the input roster; I found no concrete violation of `512*N + 4096`. The tests check formula boundaries and refusal, not actual peak allocation.

Reviewed the pinned diff, full changed file, relevant authority code, and spec/plan; no edits.
Not checked: native gates or jobs. This review establishes no measured speed, quality, lifecycle, or RSS result.
