# Closed profiler platform canary

PASS: original instance i-00180615e2b84beec was terminated and waited before collecting the authenticated evidence. The record/report/decode path produced 117 decoded multiframe CPU samples, zero lost samples, peak 88,793,088 bytes, swap/OOM zero under CPU1/512MiB/pids256. perf7.0.14 and the exact kernel/packages are recorded.

The unchanged qualified Rust binary returned expected config-SHA INVALID/exit2 with zero queries, GETs and truth opens. No ANN or performance experiment ran here. Linux sampling/decoding is qualified; actual Rust hot-path attribution is still unverified.

The first root verifier incorrectly required the executable DSO in the flat CPU report. Real sampled CPU is in libcrypto; decoded SHA256_Update and sha256sum call chains establish the preregistered platform check. Corrected only that verifier assertion, preserving the same original cloud run and receipts. No cloud/native rerun.

Full immutable evidence: s3://borsuk-bench-453182569524-euc1/research/semantic-router/20261008/cold-cpu-profile-canary-a0001/evidence.tar.gz (195899 bytes, SHA256 812d5f38cb9525f4f2af3fbf62c9742cf2f19c12b2a091ba9fd2978a8f576d3f). Binary perf.data stays in that authenticated archive; Git retains decoded evidence and the complete hash roster.
