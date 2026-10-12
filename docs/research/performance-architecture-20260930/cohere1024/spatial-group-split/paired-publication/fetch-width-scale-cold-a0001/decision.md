# 1M joint fetch16/32: advance width32

Source1a8c0a4617474bda78eea7059fda5f0ab765bb0d; qualified reducer8d3e5b54, unchanged baselineb1c12829. Exact same published spatial generation,1M corpus,1000 queries/truth,D1024,cosine,k10. Fixed ABBA order, fresh processes,CPU0/100%,common outer1GiB/library payload512MiB,swap0,pids128. Payload/local-drive cache off;resident router and reused connections allowed;provider cache uncontrolled.

| Cell | R10 | p50 ms | p90 ms | p95 ms | p99 ms | Sequential QPS |
|---|---:|---:|---:|---:|---:|---:|
|16-A1|97.80%|327.839|417.165|540.406|768.142|2.91846|
|32-B1|97.80%|199.871|243.195|280.108|381.946|4.84395|
|32-B2|97.80%|203.458|257.823|291.690|345.371|4.76107|
|16-A2|97.80%|329.844|418.476|498.416|667.943|2.94046|

All four9780/10000 hits,zero underfill. Both Rust parity pairs preserve exact ordered IDs/score bits,complete trace AND physical plan,logical GET/verified-byte charges. Width32 improved both measured repetitions with no recall loss. The selector changes SOURCE and SQ8 jointly: SOURCE sums fell from235.273/233.227s to133.017/135.344s per1000;SQ8 from86.502/84.865s to52.548/53.352s. Planning stayed19–20s. Process peaks16~66.3MB,32~69.5MB. Full native reports and resource receipts carry exact values.

Original SSMb26ed1ee-f978-4159-810d-9628b4d7a96e Success0 in1139.53s;all18 stages verified,including actual-input527 admission and separate ordinal0 canaries atbothwidths. ArchiveSHA authenticated;only native aggregates/admin receipts opened locally. Same Spot i-01e0d1e8151e4f4a7 terminated,vol-03e4165691627d5a3 absent,timerinactive,BGoff. No duplicate launch/retry/cell or changed scientific threshold.

Decision: use explicit width32 for the next matched benchmark candidate. Preserve historical default16 and all prior protocols/results; no library/default edit is bundled with this evidence. Two repetitions perwidth are limited evidence,not a robust confidence interval. SerialQPS is not concurrent service capacity. No billed-request,total lifecycle-dollar or matchedvendor win. Internal98% recall reference remains unmet at97.80%;not silently lowered.

Next: Rust canonical-record1M S3 Vectors adapter with explicit service-operation/ledger/readback envelope,then source qualification,actual-input admission and separate servicecanary before the once-only cold service comparison. Turbopuffer published references lack exact selected IDs/split and remain unmatched. Source/layout/SIMD changes require profiling or recall-loss evidence; this scheduling result is not a license to preload payloads and call them cold metadata.
