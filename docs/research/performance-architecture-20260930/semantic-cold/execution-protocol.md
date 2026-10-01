# Native semantic cold comparison: execution envelope

Preregistered 2026-10-01 before publication or compute launch. This supplements
semantic-router-cold-http-gate.md; its fixed population, ABBA order, parity,
quality and latency gates remain authoritative.

## Platform and resource bounds

Use one EC2 Spot m7i.2xlarge (8 vCPU, 32 GiB) in eu-central-1, profile causality.
The x86_64 AL2023 image ami-06121aa3085b6f918 was independently described as
available on 2026-10-01. Both graph control and semantic candidate use the same
qualified x86_64 HTTP executable. No remote compilation or ARM binary reuse.
This platform change is disclosed; historical ARM latency is not the control.

Encrypted 80 GiB gp3, public subnet and existing research instance role.
Machine wall limit 3600 seconds; runtime wall limit 3000 seconds. Runtime cgroup
8 GiB RAM, zero swap. Native address-space limit 4 GiB, memory admission and
per-process RSS ceiling 512 MiB. Native CPUs 0–3, client CPUs 4–5. No concurrent
measurement cells or retained service. Reject incomplete populations.

Spot bid ceiling $0.50/hour: at most $0.50 estimated compute for the machine
wall envelope, plus $0.15 EBS/S3 allowance. These are bounds/estimates, not
measured lifecycle cost. Record actual quote, duration, instance and billing
scope. Publication upload/read costs are separate and belong in lifecycle cost.
No automatic replacement: interrupted cells are invalid, terminal artifacts
are synced, every ACKed ID is persisted and terminated/waited before collection.
A restarted interrupted cell requires a distinct declared attempt.

## Publication and authority

Reuse the library publisher through existing two_bit_plan_demo --live-s3 on
the same owned Spot worker, before the cold profile phase. Its validation
reader requires instance credentials; no local credential shim is introduced.
Authenticate local packages and all 399 native source files before publication.
Publish each arm in a distinct fresh namespace, HEAD last, and read back the
actual root/generation/control epoch. Expected initial epoch is 1; a local
reference envelope is not evidence of remote publication. Existing canonical
and SQ8 identities are preserved. The current publisher uploads canonical
bytes even when a matching application-owned key already exists; charge these
uploads and disclose them rather than assuming zero publication traffic.

The publisher's validation calls use the consumed development panel and are
not cold latency measurements. This validation can warm opaque S3 service
caches; that state is uncontrolled and common to both arms. The cold gate
still starts 512 separate native
processes under the fixed ABBA protocol. No source or SQ8 re-encoding/refit.
Require completed exact-source workspace execution, release, Clippy and test
compilation authorities, standalone binary proof, authenticated source archive,
the authenticated publication executable/assets, exact runtime/controller
hashes, and final config. Native assurance must be complete before launch.
The worker must finish publication and verify actual HEADs and development
parity before starting any cold measurement. A publication failure ends the
attempt; all publication receipts are retained with the closed cold artifacts. No claim of a vendor win follows this
100k comparison.

## Credential and transport boundary

Use the instance IMDSv2 credential protocol. The SDK shares the native connector
with its credential provider: startup includes one PUT token and two GETs
(role and credentials). Count these process submissions and consumed response
bytes; never capture credential values. Credential byte attribution inferred
from process totals minus authenticated S3 startup payload is labelled inferred.
Query deltas remain GET-only, with no hidden retries. Report logical object
requests, submitted HTTP calls and consumed bytes separately; confirmed wire
requests, wire bytes and unread response bytes remain unknown unless measured.


## Preregistered runtime repair for a0002 (2026-10-01)

Attempt a0001 failed in the dynamic loader before the publisher could execute:
`GLIBC_2.38` was unavailable on the selected AL2023 image. The first native
invocation did not reach Rust or issue a native query; cold calls = 0. The
original failed terminal and all available bodies remain immutable. Its only
owned instance was terminated and waited before artifact authentication.

Both unchanged, qualified executables require at most GLIBC_2.38, independently
inspected with `readelf --version-info`. Use Canonical Ubuntu 24.04 x86 AMI
`ami-0b8a830d6339a9758`, available in eu-central-1, dated 2026-09-23. The AWS
DescribeImages response verifies owner `099720109477`; Canonical documents that
owner as its official account ([Canonical image guidance](https://documentation.ubuntu.com/aws/aws-how-to/instances/launch-ubuntu-ec2-instance/)).
Ubuntu 24.04 provides libc6 2.39 ([Ubuntu package authority](https://packages.ubuntu.com/noble/libc6)),
which is a compatible minimum ABI inference; the actual worker must prove all
required libraries and symbols resolve before payload staging or publication.
This does not assert identical libc to the local assurance host.

The runtime-only controller repair installs Ubuntu prerequisites before the
first AWS CLI transfer and records an authenticated runtime ABI report for both
frozen binaries. No Rust rebuild, source/scorer change, policy change, quality
gate change, memory increase or relaxed scientific gate is authorized by this
repair. The same instance, time, compute, concurrency, GET/byte and cleanup caps
remain. Freeze fresh a0002 namespaces, changed OS/bootstrap code identities and
actual ABI gates before the next paid launch. Reuse completed unchanged Rust
assurance and immutable input assets; run only affected controller checks.
