# Complete native preparation: frozen paired100k gate

Implementation `bb406498`, with its original optimized fitter and public
normalizer/SQ8 writer/generation builder/planner. No source/parameter changes
between datasets or after seeing queries. Source-only construction accepts
registered original raw vectors and no queries/GT. Fresh normalized bytes,
order, SQ8, centroid graph and root are authenticated; old artifacts stay intact.

ReLAION and CoHere first100k, D768 cosine k100. Initial development0–63 may KILL
on mean returned<98%, p05<95 or loss>0.5pp to paired native SQ8 flat. Only if BOTH
pass, score frozen validation256–999 (744 queries each), under<=32 GET and
<=16,773,120 bytes. Rust public plans and the already verified Python f32 SQ8
mirror provide offline quality; they do not certify full Rust corpus scoring,
HTTP, cold latency, serving RSS or vendors. Include original native-SQ8/Python
layout paired receipts as historical baselines, never mix their roots.

Existing Spark only, one sequential preparation per corpus and no paid launch.
Each source-only phase and planner has a15min timeout, declared build payload
256MiB, process address space4GiB; aggregate campaign timeout30min. Stop the
arm at first hard failure; no retry with larger cells/GETs or a changed seed.
Retain original job, terminal statuses, stage times/RSS, artifact/code hashes,
per-query hits/GETs/bytes and paired decision. A quality pass permits the next
product gate; it is not release, scale or vendor qualification.
