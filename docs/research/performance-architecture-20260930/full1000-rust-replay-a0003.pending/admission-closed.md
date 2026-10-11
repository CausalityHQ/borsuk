# Rust replay admission closed

Causality Spot attempt a0003, instance i-03e97ddd605145cb3, root volume vol-0c48ca54962d07674.

The qualified Rust reducer authenticated the completed historical, local and S3 diagnostic runs. Each returned 314/320 truth hits (98.125% R@10) with no underfill on the same 32-query prefix of the 1M-row Cohere fixture. The separate qualified Rust parity command exited 0 and reported EXACT_PREFIX_PARITY: ordered IDs and score bits, per-query logical charges, and authenticated request/truth prefixes all match. See the original report and manifest in three-arm-prefix-parity/closed.

The parity collection completed with every manifest body SHA verified. Native and manager exits were 0, the owned cgroup drained, OOM counters and swap peak were 0. The stage retained CPU0, CPU quota 100%, memory 1GiB and PID128 bounds.

Original termination waiter exited 0. DescribeVolumes returned the exact InvalidVolume.NotFound for the recorded root volume; the external stop timer is inactive/dead. There is no retained idle experiment instance.

This closes admission only. Diagnostic population percentiles are invalid, and no cold latency, throughput, vendor or scientific win is claimed. The published retained generation is supported by the prior authenticated source/producer, body-authentication and publication evidence recorded in this same campaign directory; parity is not a substitute for that provenance.

Next: separate disposable staging/canary smoke, then a frozen full-1000-query cold measurement using the same Rust executables, corpus, requests, truth and generation. Preserve this protocol and all historical failures. Use causality EC2 Spot; no new Python or TypeScript features, corpus/truth rebuild, or warm cache arm.
