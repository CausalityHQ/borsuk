# Paired spatial SOURCE coverage screen — consumed panel

| Quantity | Native result |
|---|---:|
| Original returned-hit control | 313 / 320 |
| Radial selected-source coverage | 314 / 320 |
| Spatial selected-source coverage | 316 / 320 |
| Gained truth slots | 2 |
| Displaced lost truth slots | 0 |
| Bridge-only truth slots | 0 in each arm |
| SOURCE disposition | PASS |

The frozen gate required at least 315 selected-source slots and gains greater than losses, with original controls unchanged. Both arms' selection seal was durable before the Rust truth read. Root checked its SHA against the native summary, all 46 retained objects' server SHA/lengths, and both exact stage manifests. Canary native exit 2 and screen native exit 0 were retained separately from manager exits 0.

Scope is 32 previously consumed ordinals from the frozen 1M D1024 corpus, cosine ground truth, k=10. This is planner coverage: candidate returned IDs were not computed and selected payloads were not scored. It is not candidate recall, held-out quality, cold S3 latency, QPS, or a vendor win. No default promotion follows.

The local native screen took 1.77 seconds and 262,180 KiB maximum RSS, with no swap/OOM. These are engineering screen costs, not query-performance metrics. The instance i-0633e3d6c0873440f is terminated and volume vol-094bd4508d139ebf4 deleted; watchdog and root background marker are off.

Next falsifier: actual returned top-10 quality from a serving generation containing the spatial publication, followed by independent held-out/cold measurements. The existing graph-only Native100k repackage adapter cannot simply be used for this semantic v8, 1M/D1024 generation; its exact authority and resource path must be established first.
