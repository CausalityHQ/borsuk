# V271 Rust RC fresh-query frontier closeout

Status: complete, 2026-09-26 UTC. The single Spot attempt `a0001` on
`i-04cab669678f2025c` terminated after its terminal marker. The launcher
replayed all 42 artifact sizes and SHA-256 hashes and read back the closeout.
Source commit `979f9a2db13a5c2406071add1864edeb043f0241`; frozen library commit
`aa4182a2e6b78a254bca732663c7528818eb6561`; source archive SHA-256
`e293ebca4dbb778810bb5c657e91cd9bb2cbc4bfd8da0d4d9ec725bdf667341f`.
Terminal SHA-256 `13775dc6c5176c636fc8442b4bd2bddd2ee375818a98d1328d2c0352423bc80b`;
closeout SHA-256 `d64015fc9501cc2146378a54b8e24866898cf2c6fcd446c37c42ce2b0a3147b0`.
Immutable artifacts:
`s3://borsuk-bench-453182569524-euc1/research/v271-rust-rc-fresh-frontier/979f9a2db13a5c2406071add1864edeb043f0241/runs/a0001/`.

The corpus was CoHere-large-10M's first 1,000,000 rows, D768 cosine, with
1,000 held-out canonical train rows 1,000,000–1,000,999 as fresh queries,
excluded from the index. Ground truth was FAISS `IndexFlatIP` on normalized
FP32 vectors at k100. All search numbers below are **verified in-process
resident API** samples on the same c7i.4xlarge Spot host, one query at a time,
after warmup; they are neither HTTP product latency nor a vendor comparison.

| Arm | GT100 hits / 100,000 | p50 / p90 / p95 / p99 (ms) | 1,000-query elapsed (s) |
| --- | ---: | ---: | ---: |
| New generic Rust builder | 99,771 | 49.590 / 58.978 / 61.314 / 65.807 | 49.601 |
| V269 authenticated generation through same Rust API | 99,779 | 53.416 / 64.167 / 67.166 / 73.127 | 53.392 |
| FAISS IVF-Flat, 2,048 lists, nprobe 128 | 98,498 | 29.669 / 33.211 / 34.048 / 35.613 | part of seven-arm sweep |
| FAISS IVF-Flat, nprobe 256 | 99,696 | 57.400 / 61.993 / 62.896 / 64.328 | part of seven-arm sweep |
| FAISS IVF-Flat, nprobe 512 | 99,989 | 106.722 / 110.224 / 111.044 / 112.677 | part of seven-arm sweep |

The seven-arm FAISS sweep and each raw query record are sealed in the terminal.
FAISS used Python bindings, while BORSUK used Rust; language overhead differs,
so the FAISS rows are an algorithmic control, not a product winner/loser claim.
The 100k falsifier passed with 9,994/10,000 exact hits and p95 27.556 ms.

New 1M generation files total 1,880,915,325 bytes. Building took 2,064.570 s
(34m25s), peak RSS 5,547,312 KiB; serving peak RSS was 1,948,228 KiB
(1,994,985,472 bytes). V269 hydration fetched 5 objects / 1,880,914,634
response bytes in 35.960 s; new local generation search made zero object GETs.
FAISS IVF-Flat built in 23.702 s and its control process peaked at 7,190,380
KiB, including index and truth/control state. Build cost is the main scale
risk: the new Rust builder is about 87 times slower than this FAISS build at
1M, though the build methods and artifacts differ. Compute quote was
$0.3696/hour and quoted compute through terminal was $0.31734; this excludes
EBS, object-storage requests/bytes, and persistent storage.

**Decision:** preregistered `go_10m` passed: new recall was only 0.008
percentage points below V269, p95 was 8.7% lower, and peak serving RSS was
below 3 GiB. Proceed to one 10M scale/cost gate on the frozen Rust library.
Do not infer a S3 Vectors or Turbopuffer win from this in-process comparison.
