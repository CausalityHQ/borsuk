# Spatial leaf partition: qualification and next gate

Source: 5cc7330b761f08d5d0777afec121d18cc06bd53f, preserving feature7372f116 and repair0226b352. Native correctness qualification is verified: all nine original stages passed and infrastructure closure is authenticated. This is a correctness disposition, not a performance claim.

The library adds opt-in widest-axis quota partitioning within existing trained groups. Radial partitioning remains the default. Both use original FP16 unit centroids, unchanged prototypes and L2 top48 nomination; the new BORSUSR3 format records the partition explicitly. Publication authenticates and replays spatial membership from centroid data, and store/import/repackage admission accounts for partition-specific allocations and retained caller buffers. The format does not persist training centers, so publication replay cannot independently prove original training assignments or terminal radial ordering.

| Gate | Evidence | Disposition |
|---|---|---|
| Native debug/release routing | Exact27+27 names, including seven spatial fixtures | PASS |
| Build adapters/publication/bin/generation | Exact3+2+7+4 names, authenticated source unchanged | PASS |
| Release artifacts | Three retained executables; exact hashes in qualification receipt | PASS |
| Workspace Clippy correctness+suspicious | Original command exit0, source unchanged | PASS |
| Complete workspace test compile | Original shim-unset jobs1 command exit0,381s | PASS |
| Historical radial parity | Accepted independent design; actual historical regeneration not run | PENDING |
| Real paired spatial quality | Producer313/320 and historical radialSOURCE314/320 controls required | UNRUN |
| Cold latency/QPS/lifecycle cost | No new measurements from this qualification | UNRUN |

Synthetic falsifier improves selected source coverage but also increases page closure384→392: sixteen gained pages and eight displaced pages. It demonstrates a possible mechanism and a cost tradeoff, not a real-corpus quality or performance win. The fast next gate is authenticated centroid reconstruction plus exact historical radial regeneration, then paired SOURCE closure with both selections durably sealed before opening truth. The original producer and radial controls must reproduce; screening requires at least315/320 with gains greater than losses under unchanged fetch protocol.

There is no new cold comparison in this slice. Existing same-corpus/query-byte100k observations remain BORSUKR10 97.23%, p90 116.43–118.56ms, p95 123.88–126.89ms, QPS10.09–10.12 versus S3 VectorsR10 96.56%, p90 118.479ms, p95 149.056ms, QPS13.754. S3 idle/backend state differs, and BORSUK disables application payload caches without controlling backend cache state; these are not a fully matched cold comparison or an overall win. Turbopuffer's first-party10M cache-off figures are an unmatched published reference; exact IDs/query split and recall are not disclosed. New spatial results cannot be merged with historical architecture results as one frozen system.

All substantial implementation is Rust. Parquet/Arrow belongs to ingestion; serving uses the existing custom binary layout. SIMD or extra parallelism requires evidence of a compute bottleneck. The spatial candidate addresses a measured coverage loss mechanism first; warm-cache work remains deferred.

Historical comparison interpretation: see [closed direct-pair evidence](../cold-direct-closure/cold-pair-a0001/closed/README.md) and [published targets checked2026-10-07](../published-comparison-targets-20261007.md). Vendor references differ in backend/cache state or workload; same bytes alone are insufficient for a matched cold claim.
