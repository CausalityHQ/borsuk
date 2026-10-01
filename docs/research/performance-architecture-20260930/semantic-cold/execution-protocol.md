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

Ubuntu root volume is explicitly `/dev/sda1`, encrypted 80 GiB gp3 with deletion on termination (AMI root-device authority checked). Shared historical ARM default stays `/dev/xvda`. a0002 config SHA `3c7fe631dd22f0ef017a5b96007ce1e50ccc2723c3783e0e776ab68a15917abd`; frozen a0001 config preserved in `config-a0001.json`. Native bins/source/full assurance unchanged; runtime ABI is an unexecuted remote gate.

## Preregistered bootstrap repair for a0003 (2026-10-01)

a0002 is closed with instance-initiated shutdown and missing terminal artifacts.
Delayed console output confirms the cloud-init user script failed; its initiating
command remains unknown because output was redirected to the local log. The
current Ubuntu Noble package catalogue lacks APT awscli, a likely trigger
([Ubuntu package catalogue](https://packages.ubuntu.com/awscli),
[confirmed Ubuntu package bug](https://bugs.launchpad.net/ubuntu/+source/awscli/+bug/2066199)).

Use the official AWS CLI 2.36.11 x86 installer instead of APT awscli. Root
downloaded its exact versioned HTTPS body, verified the AWS signature with the
published key, and pinned SHA256
50fbb7a2f44a78eab4a210088040e8f0bc4b9937cac8043c2354269d58614df6
and length 73,022,935 bytes. Installation instructions and signing authority:
[AWS official installer](https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html).
The isolated local install and version command passed; all 65 bundled ELF bodies
require at most GLIBC 2.17. Actual target installation remains an unexecuted gate.

Preregister precise dependency/source phases and synchronous bounded serial
failure diagnostics before cleanup network operations. Retain the original exit
status separately from upload failure. Emit the terminal to serial before its
S3 upload; console evidence alone cannot qualify a scientific result. The
closure log must be created before its final artifact authentication. Never
reclassify a0002 as a successful experiment or Spot eviction.

Reuse unchanged native source, binaries, full assurance, corpus, fitted router,
panels, immutable input assets, quality and resource limits. Freeze fresh a0003
namespaces and actual Python code/config identities before ONE paired Spot
attempt. No independent paid smoke or native workspace gate is required by this
bootstrap-only change.

## Preregistered exact startup-accounting correction for a0004 (2026-10-01)

a0003 passed Ubuntu ABI, all four native publications and all 256 consumed-panel
validation calls. Its first cold control query returned HTTP 200 and correct
ordered IDs, then the Python startup-accounting gate failed; a0003 remains FAIL
with 511 aborted positions and no qualified cold distribution.

The unchanged native startup path authenticates head.json and then the
generation manifest in read_two_bit_head/head_from_control. open_remote later
stages that manifest again. Charge both authority GETs and both manifest bodies,
separately from staged metadata and the unchanged three IMDS calls. Subtract
head JSON, authority manifest and staged payload from process consumed bytes
before reporting inferred credential bytes. Continue rejecting every unexpected
GET/HEAD/PUT, failed read, retry, payload mismatch and altered receipt.

Correct the Python accounting contract and fixtures; retain all source/scorer,
quality, memory, time, concurrency and policy gates. Reuse unchanged native
source, binaries, assurance and verified bootstrap. Freeze four fresh a0004
namespaces and Python/config identities before ONE paired Spot run. The old
failed sample is diagnostic evidence only, never a retroactive PASS.
