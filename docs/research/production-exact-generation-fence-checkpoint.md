# Exact-source generation fence: production checkpoint

The production exact-source path now binds an authenticated source tier and
source-ID map to the already fenced router, SQ8 mirror, page authority and
ETag. Binding rejects a different source generation, row/dimension geometry,
original-source SHA-256 or map/source artifact identity before a serving
generation can be constructed. The policy is independent of dataset and
candidate budget. It does not yet provide the live S3 query coordinator.

The focused `serving_generation` test passed on Causality Spot
`i-065729722526eaff7` (terminated): one passed, zero failed, with positive
binding and wrong router-source and map-artifact cases. The immutable source
archive SHA-256 was
`84cd434305d9ac76f6f1e919341f705d679a828a46f81373cc02fba2036459fe`;
the complete terminal is at
`s3://borsuk-bench-453182569524-euc1/research/prod-check/exact-serving/31993f67/20260924T054816Z/terminal.json`
with SHA-256
`a55db98db51881d17b10a1b95215f419cf255f5af7e3faa45a74eab90214dbd4`.
The two edited Rust files also passed `rustfmt --check` and `git diff --check`
locally without compiling on the memory-constrained devbox.

The workspace Rust test gate is **not green**. An x86 Spot attempt at
`s3://borsuk-bench-453182569524-euc1/research/prod-check/full-rust/31993f67/20260924T055139Z/terminal.json`
(terminal SHA-256
`28fcc53d5119cd537bdcc6fcecb09115aa69d99dae421e3b065a94ae5924d292`)
stopped while compiling the historical `borsuk-v26` package: on x86 its
ARM fused-only match leaves `block_scores` as the never type, which cannot be
zipped as an iterator. A diagnostic archive that temporarily typed that value
compiled the package, then ran 1,547 BORSUK library tests successfully,
including the new fence test; 66 failed and six were ignored. The 66 comprised
59 `index`, two `format`, two `manifest`, one `native_ann_build`, one V36 and
one V37 test. Examples include a 26-column/27-field Parquet fixture and
`InvalidStorage("native bounded ANN build configuration differs")`. This
diagnostic run's terminal is
`s3://borsuk-bench-453182569524-euc1/research/prod-check/full-rust-v26-type/31993f67/20260924T055636Z/terminal.json`
(SHA-256
`535c6d7740b6081fa0f7969ea348cf3440f4796ea77e726d4336740809f30fdc`).
The temporary V26 change was reverted: two V26 tests also require its ARM
fused path on x86, so a type-only patch would leave the platform contract
unresolved. All diagnostic Spot instances terminated. None of these runs
measures ANN quality or latency.

`cargo fmt --all -- --check` also reports many formatting differences in
untouched files, so whole-repository format is a separate baseline gate. The
next production gate is live 100k S3 serving with authenticated generation,
one-attempt conditional range GETs, exact source hydration and measured wire
GETs/bytes, p50/p95/p99 latency, throughput and charged memory. Its 100k
quality baseline remains V131's offline deep-image development replay; that
replay cannot establish live latency or 10M selectivity. The historical V26
package and other full-suite failures need an explicit architecture cleanup
decision; they are not evidence against the new generation fence.
