# Closed cold CPU profile

The preregistered candidate-only profile completed on Spot instance
`i-0eef0e01eed8d61cb`, which was terminated and waited before collection.
The protocol is revision `8e9cf047bf61c2e80aa09898262cecaf333998e8`.
Native exit, profiler exit and completed native reduction all exited zero.
All 51 archived artifacts matched the authenticated manifest. All 1,000
ordered results, IDs/score bits, routing traces and read charges match the
completed input-admission run exactly. Recall remains 9,723/10,000 (97.23%).

| CPU function | Self sampled CPU |
|---|---:|
| SHA-256 x86 digest blocks | 34.65% |
| Prepared two-bit score | 13.65% |
| SQ8 nominee score | 13.43% |

There are 4,104 runner samples and zero lost samples. Only 554 samples have
multiple decoded frames; 739 contain an unknown frame. Self-function
attribution is useful, but inclusive phase attribution is incomplete.
Samples do not measure network wait or establish that all hashing belongs
to page authentication. The x86 symbol alone does not establish whether
hardware SHA instructions were selected.

The instrumented pass took 99.155 seconds of query wall time and 48.328
seconds of query process CPU. Its p50/p90/p95/p99 were
93.263/117.087/127.158/164.075 ms, with 10.085 serial QPS.
Use the earlier uninstrumented ABBA campaign for performance comparisons;
this run is a CPU diagnostic, not a new vendor-win measurement.

CPU quota was one core, memory limit 512 MiB and swap zero. Cgroup peak was
164,569,088 bytes, with zero OOM events. Router query GETs/bytes were zero;
source 27,343 GETs/8,971,573,248 bytes and SQ8
24,600 GETs/16,699,756,416 bytes remain unchanged across 1,000 queries.

The saved S3 reference still leads on median latency and serial QPS;
BORSUK improves recall and tail latency on this 100k panel. Published
Turbopuffer 10M results are a different-scale reference, not a matched win.
Next: select one generic Rust intervention from these measured CPU hotspots,
preserving authentication and exact score/refusal semantics, then qualify
the exact revision before a paired cold falsifier. No warm cache work.

Full query JSONL and decoded samples remain in the authenticated S3 archive
at the prefix in independent-verification.json. Raw perf.data was hashed
and removed before collection; no raw stack snapshots were published.
Original perf text trailing whitespace is retained as immutable evidence.
No production Rust code changes are included in this evidence commit.
