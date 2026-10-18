# SOURCE frame range replay decision

Source: 83e8673dd1f81bfeaeeea5efa8888da2711e087e. Cohere 1M D1024, retained full1000 chronological A1.A trace, fixed leaf40/M2/Q2/Offload2/fetch64.

| Cost | Original | Framed counterfactual |
|---|---:|---:|
| SOURCE bytes, all1000 | 24,255,289,344 | 23,118,198,255 |
| Planned logical GETs | 97,797 | 97,797 |
| p95 SOURCE bytes/query | 28,047,360 | 26,722,428 |

Retain as a valid 4.688% SOURCE byte-saving Pareto candidate. No production format adoption, recall, query latency, throughput, physical wire/billed cost or competitor win is established. Directory fetch cost is excluded; hypothetical directory storage is 93,832 bytes. SQ8 bytes are unchanged.

The full trace lacks physical range diagnostics. Ranges are reconstructed with the pinned shared cover planner and checked against every authenticated query's original GET/byte charges before SOURCE opens. This is not recorded physical-span evidence. Headers, bridge pages and clipped final frames are included. Requests, truth and original f32 were not opened. SOURCE units and whole original/decoded SHA were authenticated. Canary and measured per-query costs match exactly.

All10 repaired native qualification stages passed, including locked release, workspace correctness/suspicious Clippy and actual shim-unset workspace test compilation. Original 9166 concurrent QPS failure is preserved: locked JSON parsing rounded an emitted f64 one ULP differently. A deterministic native RED/GREEN witness and float_roundtrip fix preserve exact validation, without tolerance. Both independent reviews' actual full-trace/schema and pre-SOURCE admission defects were repaired.

Next falsifiable serving gate: implement this same bounded lossless frame codec behind generation-pinned SOURCE reads, then compare identical frozen queries/results/plans against flat records with matched end-to-end p90/p95/QPS and physical GET/byte accounting. Keep the flat incumbent; compression proceeds only as a measured tradeoff. Do not infer an end-to-end win from this metadata replay.

Original measured unit borsuk-source-frame-range-root-measure-a0001 invocation4e263620df024147a3312ecbe32f288e: exit0, 4.69s wall, 12,164KiB maxRSS, swap0/OOM0. Local cache was uncontrolled; scan CPU is descriptive, not query latency.
