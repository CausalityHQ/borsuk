# Source utilization and single-wave cost evidence

Status: frozen method for attempt a0002; actual source, input and launch pins are in protocol.json. No execution is claimed by this method.

This reruns the unchanged method/resource limits after the source-only crate recursion-limit repair 44e116b9, preserving failed a0001 and candidate9b98778d.

## Purpose

Determine whether smaller authenticated source units could reduce bytes without silently dropping scored completion units. Separately cost fetching SQ8 for the complete discovered closure in one wave. The latter removes source nomination but changes the scored population and byte/CPU trade; it is not a qualified algorithm or a recall/latency result.

Use only the already closed Cohere D1024/k10 membership B1 metadata. Original input: 10,777,291 bytes, SHA256 `29da105428503267e73e9e23e1a61bef9967ccdc9fb4f43ef64b23d306ba0cbb`. Source record width: 264 bytes. Root binds original producer `fc8a23dac44ad635fc1236736df68878690a8389` and archive `3927688ec1344c8c62058bcecd054251c4befcff1c6fd7dd800b1a860587faa2`, identity and input rows, source 128 GET/64 MiB/2,544-unit limits, and historical SQ8 32 GET/16,773,120-byte limits. Corpus vectors, requests and truth bodies remain unopened. The original closed campaign records no failed GETs.

## Execution

One uniquely named EC2 Spot attempt in eu-central-1c. Compile under CPU2/cpuset0,1, 8 GiB RAM, zero swap, 512 tasks and jobs1. The four compiler gates share a 7,200-second deadline: affected example tests, release example build, workspace Clippy correctness/suspicious checks, and the real env-unset workspace test-build script. Retain the actual example test inventory and require every old/new/inherited test to pass without failed or ignored tests. Hash full immutable native and support source before and after.

Only after all compiler gates pass, retain the release example and run its source-utilization mode on the authenticated B1 metadata. The separate native replay unit has CPU1/cpuset0, 256 MiB RAM, zero swap, 128 tasks, AF_UNIX-only networking, and a 120-second runtime ceiling. Record actual cgroup limits, original exit, peak, events, resource usage, binary hash and input hashes. The encompassing unit allows 7,440 seconds for qualification plus replay/cleanup; compilation retains its independent 7,200-second limit. Machine shutdown deadline remains 9,000 seconds.

The existing raw Spot lifecycle uploads a sealed evidence archive and terminal, shuts down with instance-initiated termination, and the root independently waits for termination before collection. Spot interruptions discard the attempt; preserve receipts and use a separately recorded attempt if needed. Environment or compiler failures are execution INVALID, not algorithm rejections.

Compute reservation: $1.50 maximum, ancillary reservation $0.15, EBS 80 GiB. Fresh account/Spot quote and unique attempt checks precede launch. No second overlapping job or idle retained instance is allowed.

## Decision boundary

The report must complete all 1,000 queries, reproduce recorded source charges, reject impossible SQ8 page charges, disclose ideal/scored/walked/completion/bridge bytes, and count direct closure costs exceeding historical SQ8 bytes. Its counters are evidence, not a claimed win.

If the optimistic smaller-unit saving is zero or negligible, stop that line. If the direct single-wave cost is promising, preregister one generic Rust serving change with an explicit new byte/RSS/CPU envelope, run an actual fast native recall falsifier, then one frozen paired cold benchmark. Do not infer quality from the broader population or latency from fewer GETs. Warm caches, new datasets and 100B remain deferred.
