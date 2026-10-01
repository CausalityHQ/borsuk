# Positive inputs prepared

Actual preparation passed under unchanged 200 MiB/no-swap safety cap: 47.11 seconds, 83,012 KiB peak process RSS. Root authenticated all three outputs, exact raw digest, 64 query f32 bit sequences and all 6,400 zero-extended source-ordinal truth IDs. No ANN quality or latency measurement.

Initial attempts failed due to input-column buffering and dirty output-page accumulation. A 64 KiB Parquet buffer and per-chunk flush/fdatasync keep memory reclaimable; decode-only diagnostic fits at 82,468 KiB RSS. The final synthetic self-check passes on the same adapter source. No source normalization, quantization refit, mapping change or cap increase.
