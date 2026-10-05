# Packing implementation review

Reviewed source: 4d1e0de32bed9e354b9c396d972d2a235738bca4.
Review group: c4ff6ab0379b403a. Role: research_critic. Effective model: gpt-6.1-sol. Original consultation exit: 0.

**No concrete static defect found that blocks the isolated diagnostic at `4d1e0de32bed9e354b9c396d972d2a235738bca4`.** This is a source review, not native qualification. All line references below refer to that checkpoint.

- **Affinity, direction and tail:** adjacency preserves each directed base edge in both aggregation directions, so reciprocal arcs count twice. Packing accumulates affinity across the current pack, breaks ties by old ID, resets scores between packs, and excludes the short tail until the final append. The non-self-inverse mapping fixture checks translation direction. [Implementation](/home/rb/worktrees/borsuk-fine-sq8-graph-packing-falsifier/crates/borsuk/src/fine_sq8_groups.rs:2109)
- **Gate across all 128 plans:** replay explicitly passes **32** to the cover primitive, charges `D+12 = 780` bytes per row, evaluates every plan before deciding, and survives only with `[64,64]` byte-cap passes. The range limit relies on the existing cover primitive; its retained brute-force test asserts the limit. There is no touched-pack-count rejection. [Replay](/home/rb/worktrees/borsuk-fine-sq8-graph-packing-falsifier/crates/borsuk/src/fine_sq8_groups.rs:2383)
- **Authentication and isolation:** the original seal binds both roots; authenticated manifests bind graph descriptors and complete identities. Both maps are published and synced before prefix acquisition. Prefix reading uses raw `File::take`, authenticates and parses the same bytes, and performs no suffix EOF probe. Current diagnostic digests remain separate from the original trace’s source identity. [Input reader](/home/rb/worktrees/borsuk-fine-sq8-graph-packing-falsifier/crates/borsuk/src/fine_sq8_groups.rs:1886), [execution ordering](/home/rb/worktrees/borsuk-fine-sq8-graph-packing-falsifier/crates/borsuk/src/fine_sq8_groups.rs:2456)
- **Serving and failure behavior:** nomination/scoring code and serving `MAX_GETS=256` remain unchanged. Shared decoding retains base-layer reachability validation. Output creation uses exclusive creation; sync/deadline failures propagate to a nonzero exit with best-effort INVALID output. [Shared decoder](/home/rb/worktrees/borsuk-fine-sq8-graph-packing-falsifier/crates/borsuk/src/resident_vector_graph.rs:1057), [persistence](/home/rb/worktrees/borsuk-fine-sq8-graph-packing-falsifier/crates/borsuk/src/fine_sq8_groups.rs:1943)

**Independently reproduced arithmetic only:** the spaced-group fixture costs **12,330,240 bytes at 256 ranges** and **17,921,280 at 32**, exceeding the 16,777,216-byte cap. The conservative adjacency payload bound is approximately **204.3 MB**, including the 48 MiB fixed workspace; its 32 MiB runtime reserve is included once. This establishes allocation arithmetic, not RSS or five-minute execution.

The smallest useful additions are test corrections, not an architecture change:

1. Extend the [cap fixture](/home/rb/worktrees/borsuk-fine-sq8-graph-packing-falsifier/crates/borsuk/src/fine_sq8_groups.rs:2986) into a full replay falsifier: one failing plan, 127 fitting plans; require complete `REJECT`, 128 reported results, correct panel counts, and every range count ≤32. Current tests establish cover cost but never exercise a completed rejection.
2. Strengthen the [prefix test](/home/rb/worktrees/borsuk-fine-sq8-graph-packing-falsifier/crates/borsuk/src/fine_sq8_groups.rs:3134) to observe underlying read offsets. Its present counter records destination chunk lengths and would miss read-ahead introduced beneath the counter. The current reader itself is correctly bounded.

`admit_survival` intentionally accepts synthetic fixture reports. External admission must therefore verify the strict CLI invocation, exact executable/configuration, and companion artifact hashes—not treat that helper alone as frozen-input qualification.

**Unverified:** compilation, seven authored tests, Clippy, native prefix replay, RSS and deadline closure. No files were edited; no Cargo, native, cloud or large-body execution occurred. Recall and broader product requirements remain open.

