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

Reuse the library publisher through existing two_bit_plan_demo --live-s3.
Authenticate local packages and all 399 native source files before publication.
Publish each arm in a distinct fresh namespace, HEAD last, and read back the
actual root/generation/control epoch. Expected initial epoch is 1; a local
reference envelope is not evidence of remote publication. Existing canonical
and SQ8 identities are preserved. The current publisher uploads canonical
bytes even when a matching application-owned key already exists; charge these
uploads and disclose them rather than assuming zero publication traffic.

The publisher's validation calls use the consumed development panel and are
not cold latency measurements. The cold gate still starts 512 separate native
processes under the fixed ABBA protocol. No source or SQ8 re-encoding/refit.
Require completed exact-source workspace execution, release, Clippy and test
compilation authorities, standalone binary proof, authenticated source archive,
actual publication receipts, exact runtime/controller hashes, and final config.
Pending authority must reject launch. No claim of a vendor win follows this
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
