# V114 paired 1M development Spot preregistration

This cell tests whether the V114 exact local SQ8 nominee route preserves
returned Recall@100 under one 32-GET, 16,777,216-byte data wave. It uses the
frozen ReLAION-1M development inputs and the V109 capped reader as the paired
baseline. It does not measure live S3 latency or final product memory.

The algorithm is fixed before the run: V77 1,024-region PQ64 top-512 nomination;
one exact SQ8 top-100 primary set; a 513-point vote for each primary row and
one point for each other nominee; deterministic optimal physical intervals.
There is no ground-truth-dependent route choice and no vector-count placement
switch. The 99.0% returned-recall threshold is an evaluation gate, not a fitted
algorithm parameter. Promotion still requires untouched ReLAION validation
and an unrelated deep-image-96-angular corpus.

The five frozen object URIs, sizes and SHA-256 values are
`scripts/launch_v112_precise_nominee_spot.py::INPUTS`. The V77 manifest must
regenerate to SHA-256
`131a4cd80dfee8ef4d0486b2043349e01b6c230d702f8d17ad1224e3fbc6a874`.
The existing V70 SQ8 object is sealed from source only, before development
queries or truth are downloaded. A score-bit, primary, route or cap mismatch
stops before quality is reported.

The first 200 queries must reproduce V109's 19,739/20,000 GT hits and pass
independent validation. Only then may the same frozen code run all 1,000.
The all-1,000 V109 baseline must reproduce 98,803/100,000 hits. The candidate
must return at least 99,000/100,000 hits; its p05 hits/query must be at least
90, V109 p05, and paired exact-oracle p05 minus one; its count of sub-90-hit
queries must not exceed V109. Every query must have RAM/file/reference score
and route parity and at most 32 GETs and 16,777,216 bytes. A failed gate is
negative evidence and does not advance to live S3.

Compute is one `c7i.12xlarge` EC2 Spot instance in `eu-central-1`, with a
120-GiB encrypted disposable gp3 root volume and a four-hour science wall cap.
The EC2 Spot price history observed on 2026-09-23 at 22:00 UTC was
$0.982100/hour in `eu-central-1c`; latest entries for `1b` and `1a` were
$1.005300 and $1.025100/hour at 20:00 UTC. These are observations, not a
future price guarantee. Four hours at those observed rates would cost
$3.93–$4.10 for instance time, plus storage and transfer charges. The
launcher requests only Spot and records the launched instance identity.

The worker checks the Spot interruption notice every five seconds. An
interrupted measurement cell exits without a complete result, syncs terminal
artifacts to immutable S3 keys, then terminates. A resumed attempt must use
a new attempt prefix and restart the entire 200/1,000 measurement cell.
The parent observes only the terminal marker and EC2 health while running;
it never reads incomplete measurement JSONL files. The terminal lists each
artifact's byte count and SHA-256 for independent post-run streaming checks.
The instance terminates immediately after terminal upload or failure.
