# V252 strided anchor routing: rejected

Source commit `136e6ed61e64a5a9ee8b18ab86fd8637b3f2d771`;
one `causality` c7i.4xlarge Spot attempt `a0001`, instance
`i-003fd094404963ddd`, terminated after the complete terminal.
Immutable prefix:
`s3://borsuk-bench-453182569524-euc1/research/v252-cohere-strided-anchor-100k/136e6ed61e64a5a9ee8b18ab86fd8637b3f2d771/runs/a0001/`.
Source archive SHA-256
`5b6983e2bb9c8b7964c24e7dff2aafeebf9086675404c38402407636907947c8`;
terminal SHA-256
`3e87061f17a06266244e3192cd5849a013ff86eef1cd34753a216ec63ee43294`.
The launcher replayed every terminal artifact's size and SHA-256. The
single remote graph unit test passed 1/1 before measurement.

CoHere-large-10M first100k D768 cosine, k100, development ordinals
0–255 and validation 256–999, both previously used. The source F32,
FP16 plane, PQ books/codes, map, query panel and exact GT100 truth match
V248–V251 byte for byte. The rebuilt V250 diverse graph SHA-256 is
`688941c7c61c89a39739909af14cb2b4a935a7a4967a168f9503ac34e170d0e7`.
V252 sealed raw IDs SHA-256
`9fb07aa247328049ce32f5653e4bb5f28a2226a43f861e79939503a3d6179538`.
Independent replay reproduced the hit totals and compared V251/V252
per query: three wins, 995 ties and two losses.

| Method | Development hits / 25,600; p05 | Validation hits / 74,400; p05 | Combined hits / 100,000 | Loaded p50/p90/p95/p99 ms | Loaded QPS | Scored rows p95 |
|---|---:|---:|---:|---:|---:|---:|
| V251 exact FP16, ef512 | 25,369; 96 | 73,851; 97 | 99,220 | 13.892 / 16.556 / 17.136 / 18.173 | 558.6 | 9,558 base |
| V252 256 strided anchors, ef512 | 25,369; 96 | 73,852; 97 | **99,221** | 13.919 / 16.637 / 17.171 / 18.184 | 556.5 | **9,824 including anchors** |

V252's development mean R@100=0.990977, validation=0.992634;
both fail the frozen ≥0.995/p05≥98 quality gate. The work limit
≤10,000 passes, as do loaded p95≤35 ms, serving peak RSS≤256 MiB
(210,280,448 B) and zero vector-body GETs. Cold hydration was
323.477 ms; graph rebuild 169.620 s/536,633,344 B peak RSS;
source/PQ preparation 50.42 s/1,771,260 KiB peak RSS. Loaded times
are in-process, not vendor service latency.

**Decision:** reject single-anchor coarse entry on this graph. The
near-identical output shows that the graph's local exploration, not
its starting row, governs this 512-beam candidate set. Do not try more
anchor counts or beam variations. Keep 10M closed. The next material
design is a coarse partition plus PQ candidate routing format that can
retrieve the globally competitive candidates without relying on this
graph's 14,508-row quality-passing traversal. First prove its quality,
work and resource behavior on the same 100k panel; then measure 1M
end-to-end before any larger promotion.
