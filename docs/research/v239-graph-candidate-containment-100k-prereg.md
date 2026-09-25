# V239 PQ graph candidate containment falsifier

Use the authenticated V218 ReLAION-100k D768 physical order, reachable
graph, FP16 ID authority, PQ64 books/codes and fixed 1,000-query panel.
Queries 0–255 are prior-used development; 256–999 are prior-used method
held out. This is an offline candidate test, not an object-store or product
latency result. The V218 FP16 reranker returned 99,771/100,000 GT100 hits
at ef/shortlist 2,048 and p05 99.

For each query, run the existing cosine PQ graph with ef=4,096 and retain
the first 4,096 physical candidate rows. Independently score all 100,000
rows with the same PQ code scorer and retain the best 4,096 as the flat
oracle. Both use stable physical-ordinal tie breaking. Seal both ordered
ID streams to S3 before downloading the same authenticated GT100 witness.
Score prefix containment at 512, 1,024, 2,048 and 4,096, including
development, method-held-out and combined totals and per-query p05.
Find the smallest prefix meeting at least 99.6% GT100 containment and
p05 at least 98 on **each** split. Count distinct 256-row physical SQ8
pages for every prefix. At 780 bytes per row, page count times 199,680
bytes is a lower bound on full-page reads, before gap coalescing.
Record per-query graph visits and graph/flat CPU time as diagnostics,
not product latency. All parameters are fixed before truth access.

The current physical layout passes the remote-tier falsifier only if
that smallest passing graph prefix has p95 minimum page bytes at most
16 MiB, the existing object-read budget. V218 already implies that its
2,048 candidate list contains its 99,771 returned hits; repeating a
looser 4,096 containment threshold alone would not test anything new.
If the byte gate fails, do not promote this physical layout to 1M:
measure whether a generic graph-local row permutation can reduce the
page bound before another live read. If flat PQ also misses the quality
target at 4,096, investigate code precision; if flat passes but graph
fails, investigate navigation. Graph may beat flat on some queries,
because top PQ scores are not an upper bound on true-neighbor coverage.
If both quality and byte gates pass, replay the same method on frozen
ReLAION-1M before any live remote rerank. SQ8 order and actual S3 I/O
must still be measured. No N-invariant shortlist or vendor win follows
from 100k alone.

Run one c7i.4xlarge Spot attempt with AWS profile `causality` in
eu-central-1c. Authenticate every input against the frozen V218 SHA/size
roster and V218 terminal identity; put the source archive and reservation under the unique attempt
prefix. The worker uploads a sealed raw candidate stream before fetching
truth, syncs terminal artifacts and their sizes/hashes to S3, writes one
terminal marker, then shuts down. The launcher independently replays the
terminal and quality evidence and terminates the instance. If Spot
interrupts the cell, discard it and restart under a new attempt prefix.
