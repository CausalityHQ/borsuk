# ARM platform correctness gap closed

Frozen current source7df4ef293d28569128f7fb602f0f4e3f7f80cb4f, archive93bc284a78f60a5202b1c612a87a94e7938f17ac132448b5fa905051a45ad5e0. ARM library code is unchanged from the latest e6c2e97 x86 full gate.

Attempta0001 was an AWS Spot capacity rejection in eu-central-1a; no instance/measurement existed, verified by unique-tag EC2 listing. Its reservation/user-data/rejection are preserved. Sequential attempta0002 used registered eu-central-1c Spot, i-0608034a94b43f6f3, independently verified terminated. Native controller94910 closed exit0; terminal complete exit0.

| Existing check layer | Passed | Failed | Existing ignored |
|---|---:|---:|---:|
| borsuk-fma library |7 |0 |0 |
| borsuk-pq4 library |17 |0 |0 |
| centroid_hnsw mechanics |14 |0 |2 |
| unit_centroid_graph mechanics |6 |0 |0 |
| V26 quality backend-contract synthetic fixture |1 |0 |0 |
| V26 serving backend-contract synthetic fixture |1 |0 |0 |
| **Focused ARM total** |**46** |**0** |**2** |

Passing backend-detection test explicitly asserts Aarch64Neon on this aarch64 Graviton3 target. Boundary block scores and full production PQ4 ranking match independent scalar controls; fused arithmetic bit parity and current sequential-diversity determinism/bounds tests execute. Both retained V26 positive branches execute on ARM, including authentication and frozen-depth assertions. These are synthetic correctness checks, not external corpus quality or serving latency/QPS. No historical method or measurement was changed.

Independent closed-artifact verifier checked all4 artifact hashes/lengths, each named filter executing (not zero tests), all six target summaries, actual CPU/toolchain metadata, source archive22,421,911bytes/SHA and372 Rust/Cargo/controller file identities. Terminal SHAa0f45578d61b9636274f14bda7d2ae696d1d21a6c8666783b6b96b74e87fa4e8. Existing ignored centroid checks were unchanged. Latest full workspace assurance remains x86 **2683passed/0failed/26ignored**; this focused46 is separate, not an ARM full-suite claim or an inflated workspace total.

Worker observed298s; estimated compute **$0.0149**, from observed eu-central-1c Spot quote, excludesEBS/S3 and is not an invoice. No active BORSUK worker remains. No new performance, recall, physicalS3, generation-swap, lifecycle-dollar or vendor evidence follows.

Next: reconcile the held default dual architecture critique after18:16:42UTC, using the closed validation-loss, source-cost and platform evidence plus the topology falsifier draft. This correctness gate neither approves that architecture nor authorizes a quality campaign. Full product goal stays active.
