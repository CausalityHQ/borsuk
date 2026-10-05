# Closed histogram SQ4 decision

Status: valid scientific REJECT. Original exec92660 exited0; the acknowledged instance i-00824fbde5beb5995 was terminated and waited. All158 terminal bodies were independently authenticated. Independent pinned truth recount reproduced all three arms on both panels.

Matched workload: ReLAION and CoHere FIRST100k, D768, cosine, k100, consumed64 queries each. This is a local packed-payload diagnostic, with unchanged nominations and fetch cover; no cold HTTP, physical S3 query latency, QPS, fresh-query generalization or vendor comparison.

| Mean recall@100 / p05 hits | ReLAION | CoHere |
|---|---:|---:|
| Original256 SQ8 reference | 99.484375% /99 |98.515625% /95 |
| Same fetched population SQ8 |99.515625% /99 |99.03125% /97 |
| Previous uniform SQ4 |93.078125% /88 |89.046875% /84 |
| Learned histogram SQ4 |95.8125% /91 |93.359375% /89 |
| Required |98% /95 |98% /95 |

The learned book improves mean recall by2.734375 and4.3125 percentage points over uniform SQ4, but misses both quality gates. Same-population SQ8 minus learned SQ4 remains3.703125 and5.671875 percentage points. This supports investigating scoring/quantization correction before another routing change. SSE improvement alone did not establish recall survival.

All128 payload envelopes passed: <=32 ranges, <=16MiB. Mean/max verified bytes: ReLAION6,271,749/16,625,664; CoHere8,488,854/13,749,120. These counts exclude separately charged startup: six generation reads504,161B, **including** the two books100,128B. Book counters are a subset, not additional reads or bytes (`histogram::Loaded::open` computes root + book + group bytes). Native whole diagnostic10.124856319s, kernel peak144,527,360B, swap/OOM0, drained and cleaned. Whole instance lifetime93s. The diagnostic time includes transcoding, paired scoring and truth; it is not per-query latency or QPS.

Decision: do not promote the learned codec as a qualified serving winner or run fresh1M performance with it. Preserve the closed result and native-qualified research code. Next design work must specify one causal quantization/score correction, keep these unchanged source nominations/queries/quality gates, and explicitly account any extra per-vector bytes. No parameter sweep or simultaneous routing arm. A new source-corrected arm needs cheap native numeric/recall falsifiers before scale, then matched payload qualification. Incremental lifecycle and100M router/pin feasibility remain unqualified.

## Constraints for the next native codec

The current row is 396 bytes: ID8, reconstructed squared norm4, and 384 packed code bytes. The largest measured ReLAION cover contains 41,984 rows. Adding another four-byte field would make that cover 16,793,600 bytes, exceeding 16MiB by 16,384 bytes. Replacing the norm field requires an explicit new score convention; its space cannot be counted twice.

Source inspection at `371e0988df7bff465fa0e21711ce0b3754911db8` confirms that `exact_sq8_nominee::score_nominees` uses the stored norm and decoded coefficients for squared-L2 ranking. The diagnostic normalizes its query, but does not silently renormalize decoded database rows. A unit-direction codec must therefore retain the unchanged SQ8 reference and separately declare its source normalization and target score. Any normalization control must be reported separately from quantization error.

[Multi-bit RaBitQ, section 3.3](https://arxiv.org/html/2409.09913v1) provides a per-vector denominator correction for inner-product estimation. Its mechanism motivates the next investigation; it does not prove either BORSUK recall gate. Require an independent scalar oracle for the serialized correction, rotation and score, including nonunit source rows, before a new measured arm. Rotation state, transform CPU, startup reads, query scratch and pinned generations remain charged even if row width is unchanged.
