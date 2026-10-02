# CoHere fixed64 quality: FAIL

Execution completed and all 21 artifacts authenticate; owned instance terminated. FIRST1M D768 cosine, fixed64 new queries, source-qualified production scorer: recall@10 584/640 = 91.25%, recall@100 5463/6400 = 85.359375%. The frozen 95% R10 gate misses by 24 hits (3.75 percentage points). Preserve FAIL; no retuning or re-scoring this panel.

Nomination covers 569/640 top10 truth hits; page closure recovers 17 to 586. Source scoring, source-ranked pages and SQ8 admission all retain 586; final returned ranking retains 584. Thus 54 misses occur before final ranking and 2 downstream. The next causal investigation should improve discovery/nomination coverage under a newly frozen panel and measured resource envelope; widening metadata staging cannot fix this quality loss. Do not classify the whole architecture as killed.

One build completed in 81.500 seconds (344476 KiB process peak RSS); one score completed in 5.127 seconds (46568 KiB process peak RSS). Aggregate cgroup peak 4 GiB, max/reclaim 4366, zero OOM and swap; sampled scratch peak 7,251,182,322 bytes. These are construction/scoring measurements, not cold serving latency, QPS, or billed cost. ReLAION performance remains a separate dataset/protocol.
