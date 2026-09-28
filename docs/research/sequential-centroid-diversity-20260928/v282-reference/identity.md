# Authenticated unchanged V282 reference

Original a0002 complete terminal SHA23cf19cdc03c5ce2df116b3eeb70e57c57f9aeaf84ea95ab110dbe598f78b3bc, source e74d75acb40647f606b498023b11dec26701e4aa, archive3facfaf935a3e4921466281e79afe15f8cc945e30870a97806f814845972998d. Downloaded only10 already-closed evidence artifacts (raw/provenance/source/truth/generation manifest for both corpora), each exact original terminal hash/length checked before parsing. Original artifacts are unchanged; gzip copies here are storage wrappers, not new measurements.

Both first100k,D768,cosine,k100; query ordinals0–63 are a subset of the original development0–255. Native config matches both exact truth hashes; frozen original run_v282_100k.sh at e74d75ac explicitly checks the same request/input hashes. ReLAION raw Parquet inputa199e151.../requests b2485629.../truth4bd3ac79... and original-ID mapac7ee849...; CoHere raw input0f3631d7.../requests86d94064.../truth06cd59b3... and original-ID mapbaa5f49f.... Full hashes and counts are in development64-reference.json and original provenance copies. V282's ordinal mapping is explicit; native extraction uses the same corpus row order/truth namespace.

| Historical reference / identical first64 ordinals | Mean returned recall@100 % | p05 hits |
|---|---:|---:|
| ReLAION V282 graph route |97.984375 |94 |
| ReLAION V282 flat centroid route |98.296875 |95 |
| CoHere V282 graph route |96.25 |92 |
| CoHere V282 flat centroid route |96.046875 |90 |

These are verified reductions of already-completed records, not newly executed measurements. The flat arm is **not exhaustive SQ8**: the frozen v282_local_falsifier invokes plan_pages_flat_control on the same bounded routing recipe. The current native exhaustiveSQ8 scorer is a separate baseline. Layout, encoding, architecture and hardware differ from the native topology control; old CPU/RSS timings are stale for the new system and no causal topology or speed claim follows from this historical comparison. No aggregate256-query value is used as a paired64-query reference.
