# V21 Dual-Authority Feasibility Diagnostic Design

## Goal

Run the claim-ineligible V21 selector feasibility diagnostic from the reviewed
current source without rebuilding the immutable 9.99-million-row V20 Deep
Image index. Preserve complete provenance for both the diagnostic executable
and the historical index, use S3 Standard only, and terminate the Spot host at
the first terminal outcome.

## Problem

The V21 diagnostic was added after the latest completed V20 Deep Image build.
The current Publication V3 runtime path assumes that one frozen manifest,
source archive, protocol, and binary also produced the index. Consequently it
cannot both authenticate the historical index and execute the new diagnostic.

The diagnostic also retains authenticated exact geometry for the complete
corpus while it projects each arm. The 3 GiB serving allocation is therefore
the wrong host boundary even though the projected production selector must fit
within 40 MB and projected production RSS must fit within 768 MiB.

## Authority Model

The diagnostic has two immutable authorities:

1. **Diagnostic authority** binds the current Git commit, source archive,
   lockfiles, frozen manifest, diagnostic protocol, compiled binary, runtime
   attempt, and result namespace.
2. **Base-index authority** binds the historical frozen manifest and protocol,
   completed-build terminal receipt, source archive digest, index receipt,
   object roster, inventory, index URI, dataset identity, index profile, and
   builder identity.

The two manifests must have identical campaign, Deep Image dataset material,
index profile, environment architecture, and workload geometry. The historical
source commit must be an ancestor of the diagnostic source commit. Source
identity and cell/result identity are deliberately not required to match.

The CLI accepts one exact base-build terminal URI and its SHA-256. Every other
historical URI and digest is derived from the authenticated terminal receipt or
its historical manifest. The controller downloads and validates those objects
before it uploads the current source, manifest, or protocol and before it can
request EC2 capacity. A malformed or incompatible invocation therefore creates
no immutable S3 debris and spends no compute money.

## Execution

The controller creates a distinct `runtime-v21-feasibility` attempt under the
current diagnostic cell. Keeping the result under current-source authority
prevents two diagnostic revisions over the same base index from colliding; the
base-index authority object makes the reused generation explicit. It launches
the registered BORSUK build-worker class
on Spot because the projection's diagnostic working set is intentionally much
larger than a serving process. The host still uses no local index or corpus;
all authoritative data comes from S3 Standard.

The worker:

1. authenticates and extracts the current source and current diagnostic
   manifest/protocol;
2. authenticates the historical manifest, protocol, build terminal, index
   receipt, roster, and inventory;
3. installs the pinned build toolchain and compiles only the current release
   `production_bench` diagnostic executable;
4. observes the historical S3 index against its roster without populating a
   query cache;
5. downloads only the query, ground-truth, and metadata dataset objects;
6. runs the diagnostic with swap disabled and a 32 GiB systemd memory limit,
   recording the cgroup's configured limits and observed peak rather than a
   self-reported requested-limit label;
7. validates and uploads the three raw artifacts, canonical result, runtime
   attestation, and execution contract before publishing the terminal receipt;
8. shuts down immediately on success or failure.

The 32 GiB diagnostic ceiling is not a production allowance. The decision uses
only the reported projected selector bytes and projected production RSS.

## Runtime Validation

`run_publication_v3_cell.py` validates the current diagnostic cell against the
current manifest. In V21 feasibility mode it separately validates the base
index receipt and roster against the historical build cell and historical
source digest, reconciles the historical inventory, and then substitutes only
the authenticated historical index URI into the execution plan. Ordinary
build, recall, concurrency, and lifecycle paths retain their single-authority
behavior.

Every raw V21 artifact names the historical index ID and historical source
archive digest. The diagnostic source digest is recorded in its separate
authority object and must never be substituted for the index producer's
identity.

The canonical result includes a base-index authority object with the historical
source, manifest, protocol, build-terminal, index-receipt, roster, and
inventory digests plus index ID and URI. The terminal receipt binds that object,
the current compiled binary digest, all four result artifact digests, host
instance type, process memory ceiling, and `claim_eligible:false`.

## Failure and Retry Semantics

- Any missing, noncanonical, mismatched, or reordered authority fails before
  scientific execution.
- Base authority is validated before current-authority S3 publication, attempt
  selection, client-token creation, or EC2 launch.
- A non-ancestor base source, changed dataset, changed index profile, changed
  architecture, non-Spot request, or wrong host class fails before EC2 launch.
- Attempt numbers and client tokens retain the existing immutable retry model.
- Raw artifacts are uploaded before the result and the terminal receipt remains
  last.
- Markerless termination uses the existing durable controller observation and
  advances to the next bounded attempt.
- No V20 or V21 index object is written or mutated.

## Verification

TDD coverage must prove:

- the current-source/historical-build mismatch fails under the old path;
- exact dual authority succeeds and every digest reaches the worker and
  terminal receipt;
- mutations to either manifest, protocol, source, terminal receipt, index
  receipt, roster, inventory, dataset, profile, ancestry, host class, or memory
  ceiling fail closed;
- the worker compiles the current binary and never downloads the historical
  binary as executable authority;
- every V21 CSV and JSON artifact names the historical index identity while the
  receipt separately names the current diagnostic source and binary;
- a malformed base authority touches neither S3 writes nor EC2 APIs;
- the request is Spot, uses the registered build-worker instance and storage,
  preserves the current-source V21 result namespace, enforces exactly 32 GiB
  and zero swap, and attests the observed cgroup values;
- the launcher exposes an exact `--diagnose-v21-selector` path and forwards the
  immutable base-terminal URI and digest without ambient defaults;
- ordinary Publication V3 execution remains byte-for-byte unchanged;
- shell syntax, focused Python tests, Rust V21 tests, strict Clippy, workspace
  tests, methodology tests, and research evidence validation are green before
  launch.

## Paid Decision

One diagnostic attempt is launched. An arm qualifies only when selector bytes
are at most 40,000,000, every query uses at most four actual GETs and 1 MiB,
minimum GT coverage is at least 0.990, recall@10 is at least 0.975, and projected
production RSS is at most 768 MiB. If no arm qualifies, the result is recorded
as a terminal negative and the next design uses independently authenticated
selector-region fetch units. No gate is relaxed and no V21 index build begins.
