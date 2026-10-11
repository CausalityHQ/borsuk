# Sealed 1M independent32 loss decomposition — closed

Exact Rust source: 26100d05dcacebcc115481a504b090955c84324b, compare_native_replay.rs SHA256 ad5e58164545459e60d29d30fd30cce2b86967b10089b59c65637ff43df38349. Full native source identity: 49b4d0f34b7522357ead214c6749a4b0ffef6fce0a93b33e6c65fa68dd218d5f (430 files, sorted SHA256<TAB>path<LF>).

Same causality/eu-central-1 Spot i-05f4d4ae54bb4f5b6; no ANN rerun. Four narrow tests and all 98 affected-example tests passed; locked release, workspace/all-target Clippy correctness+suspicious, and actual override-unset complete workspace test compilation all exited 0. Complete workspace tests were compiled, not executed. Every collected native stage manifest verified and source before/after matched. Release ELF SHA256 33b2735fe040945407e8d45feda3017bc8f5a149c6b33864462c0c70f41118c6, 5220336 bytes; ELF retained in immutable S3 stage, excluded from Git.

Qualified Rust reducer authenticated original completed producer, full SQ8 body/header order/bijection and truth. Result: 32 queries, 313 hits, seven misses, exactly 320 disjoint truth slots. 313 source-covered hits; six misses outside both SOURCE and SQ8 ranked coverage (queries 742:2, 823:3, 991:1); one covered ranking loss (query 537, ID936587, physical row625802). No source-to-SQ8 nomination loss or bridge rescue observed. A covered ranking loss is not a quantization attribution.

Original independent32 recall remains 313/320 = 97.8125%, FAIL against frozen >=314 requirement. This is an offline diagnostic, not population recall, cold latency, capacity QPS, vendor parity or scientific win. Next causal work should investigate candidate coverage and separately falsify the covered ranking miss. Production ANN code was unchanged.

Preserved failed attempts: original source070fddf3 compile exit101 (missing Hit Serialize), repaired by explicit returned-hit JSON serialization; AWS CLI high-level transport under 256MiB exited OOM before SQ8 authentication. Single-stream s3api get-object transport passed under the unchanged 256MiB/swap0/CPU0 cap and deadlines; original files/unit/journal preserved. No scientific limit was relaxed.

Offline stage native/manager0, owned cgroup drained, OOM0/swap0. EC2 terminated, root volume vol-0e5113694f3485911 returned InvalidVolume.NotFound, external timer inactive/dead, background flag cleared. All resource-heavy execution occurred on EC2. Local work was source/static/Git/opaque artifact collection and reading Rust-produced reduced output.
