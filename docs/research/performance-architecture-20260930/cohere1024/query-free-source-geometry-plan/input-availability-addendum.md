# Retained input availability — source and metadata evidence only

Exact source e359f0a33661107f55861af9a17d5fe58f56d432. This addendum does not modify the decision under review or authorize execution.

The retained generation object listing contains ten metadata/payload objects and no centroids.bin. router/leaves.bin is listed at 64125000 bytes; a fresh HeadObject agrees, ETag "9e33a35fc6aacdbc334456266df592f3-1". The listing and HEAD establish availability only, not authenticated body correctness.

semantic_unit_router.rs construction writes each unit's u32 physical ID followed by its original D*2 FP16 centroid bytes into leaf payload. The generation Semantic discovery binds leaves_sha256/leaves_bytes, and open checks those against the root manifest leaf_payload descriptor. validate_leaf checks exact per-leaf hash/length, membership, uniqueness and row counts. Therefore the minimal prospective diagnostic should stream/authenticate the existing leaf payload, validate every descriptor and exactly-once unit roster, and obtain stored unit coordinates from it; it must not require an unavailable standalone centroids.bin or silently reconstruct reference vectors from SQ8.

Canonical candidate metadata: retained semantic/objects/72d58c8e733415c6354ab7f8f28c5b50a816cf6f6aa7bce62d30c1bd9449c93c, 4104000000 bytes. This candidate must be matched to the authenticated generation canonical descriptor by the Rust runtime validator. A content-looking key or caller hash alone is insufficient authority. No body has been downloaded or parsed for this addendum.

Source-coordinate correction remains: the stored FP16 unit means are derived from SQ8 reconstruction; canonical reference neighbors use actual normalized FP32 coordinates. Report residuals with those distinct names. Availability does not establish exact canonical means, quality, cold latency or feasibility at 100M.
