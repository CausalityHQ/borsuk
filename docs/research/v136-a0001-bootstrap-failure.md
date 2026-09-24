# V136 a0001 bootstrap failure

The preregistered V136 science method did **not** run in a0001. Source commit
`717757d9ad18139fb8dc020414ec4a1b4f3e9931` and archive SHA-256
`49ad3ea4df570823b1681bacebb5be866c92cc23f68e59e773756bd63679486a`
were authenticated by cloud-init on Causality Spot instance
`i-0b9d6f14610a7ec00`, but the launcher's user data invoked the absent
`repo/scripts/run_v136_hull_route_remote.sh` path. The committed runner was
`repo/scripts/run_v136_concurrent_route_remote.sh`. Cloud-init failed at
2026-09-24 07:47:40 UTC before installation, compilation, input download,
or any request. Its failure trap had been cleared immediately before the
wrong `exec`, so the instance stayed idle without a terminal marker.

The failure was diagnosed from EC2 and SSM infrastructure health, not a
partial measurement file. SSM command
`14f69742-540e-4046-8c01-0eed72f4bac0` reported the cloud-final failure
and missing path. The same command's host process tree showed no science
process; EC2 instance and system checks were `ok`. We wrote an explicit
operator-recovered **failed bootstrap** marker to the attempt's immutable
terminal key, SHA-256
`718164be56ddf9ccfc3b26a5962421c79c84c561466109e99301acaf4163e04b`.
It records exit 127, `status=failed`, zero artifacts, and the recovery
provenance. The original launcher then observed the terminal, exited 1,
and independently observed the instance **terminated**. No V136 quality,
latency, throughput, or memory result exists for a0001.

The a0002 repair changes only launch plumbing: invoke the real runner, keep
the bootstrap failure trap active until `exec`, and check that the complete
source archive contains that runner before allocating Spot. The Rust route,
concurrency cells, dataset, thresholds, generation, and raw evidence schema
remain the preregistered method. A new immutable source archive and attempt
prefix are required; a0001 will not be resumed or overwritten.
