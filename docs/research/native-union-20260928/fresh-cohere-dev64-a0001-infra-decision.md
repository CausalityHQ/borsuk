# CoHere dev64 a0001: startup failure, no quality measurement

2026-09-29. Original controller50759 closed1; Spot instance
`i-01ae6b4e33a2a40fa` independently verified terminated after97s.
Terminal`64ae61dd…` and all closed artifact bodies reauthenticated against S3.
Compute$0.0048 is an **estimate excluding EBS/S3**, not total cost.
Cgroup peak653,205,504B, zero swap/OOM; native response body is empty.
No native recall, HTTP latency or QPS was measured. This is an infrastructure
failure, not a scientific FAIL or a competitor result.

Root cause: CoHere's source publication records canonical data separately from
its nine metadata generation assets. The dev64 download roster omitted the
canonical body because it already existed on S3. However,
`publish_two_bit_generation` unconditionally authenticates local `canonical.bin`
through `upload_authenticated_file` before publishing a native namespace.
Native startup therefore reported `Upload(Io(NotFound))` before any response.
ReLAION's working roster includes this local canonical artifact.

Correction: include the source-authenticated canonical object as
`generation/canonical.bin` in both the config and launch authority roster.
The regression check first failed for its absence, then passed with the fix.
Source root, SQ8, scorer binaries, fixed queries, exact GT, HTTP protocol and
quality/resource gates remain frozen. The original a0001 archive/config and
terminal are retained unchanged; current config is a distinct revision.

One manually initiated bounded a0002 may exercise the corrected preparation.
It uses the same dev0–63 panel; a0001 opened requests but produced no ANN output.
No automatic interruption replacement or concurrent worker is authorized.
Collect the terminal and actual termination, then independently verify integer
recall and HTTP reductions. Prospective64–999 remains unopened by ANN.
