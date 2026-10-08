# Closed source-utilization attempt a0001

Disposition: **execution INVALID — compiler macro recursion limit**. No algorithm rejection or numerical result.

Frozen protocol: `845844c55618432b1a978933a97805056c337c20`.
Source candidate: `9b98778dec172972c2e57d695e26341a8cfb18be`.
Original Spot instance: `i-031f52784480b4e71`, terminated and independently waited before collection.

The first auxiliary test-inventory command failed during compilation with native exit 101. Rust diagnosed `json!` recursion at `compare_native_replay.rs:1809:9` and suggested crate recursion limit 256. The qualification wrapper returned 1. No test executed; all four mandatory stages and the B1 metadata replay remain unrun.

The root independently authenticated all 42 artifacts and every one of the 425 native source blobs against immutable Git. Full source/support hashes agree before and after. Actual CPU2, 8 GiB, zero-swap and 512-task limits were admitted; no OOM or swap was observed and the experiment task group drained. Resource and scientific limits were not changed.

Raw compiler log SHA256: `9754b3b4f1952bb909925ff77088d3d19c2618a2c1964b931c397621549d6067`.
Closed evidence: `s3://borsuk-bench-453182569524-euc1/research/semantic-router/20261008/cold-source-utilization-a0001/evidence.tar.gz`, 265,092 bytes, SHA256 `52a7191d26983a67d09d96d3632448c30d9d917d95583079485db133f642d211`.

Required next action: a separate one-line compiler-limit repair on the same source worker, then a new recorded attempt starting at the failing inventory compilation and proceeding through the unchanged mandatory gates. The original source, protocol, failed logs and receipts remain immutable. Current measured cold comparison numbers are unchanged.
