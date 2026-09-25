# V222 two-host graph HTTP gate

**Decision:** does the selected BORSUK graph preserve V220 quality and
usable tail latency over a VPC peer connection from a separate client
host? Compare the closed raw result with V221 direct S3 Vectors on the
same ReLAION-1M validation panel. Report cache and transport differences;
do not infer a matched Turbopuffer result.

- Gate dependencies: authenticated complete V219 and V221 terminals.
  One committed source archive and one immutable attempt prefix.
  ReLAION-1M D768, validation ordinals 0–999 already used, k=100,
  eight concurrent persistent HTTP/1.1 client connections.
- Two `causality` c7i.4xlarge Spot hosts in `eu-central-1c`, peer-only
  TCP/8080 security group rule, 120-minute hard stop on each host.
  Launch the server first. It authenticates the V219 graph/FP16/PQ
  artifacts, builds the V220 Axum service, and publishes a private-IP
  readiness receipt. Verify that IP against EC2 before launching the
  client. If either host interrupts, discard the whole attempt and
  restart under a new prefix. Record each EC2 launch identity and the
  Spot quote in S3 as soon as it is known, plus an interruption marker
  or observed EC2 state reason on failure. Confirm both hosts terminate
  at closeout.
- The separate client authenticates V219's 1,000 request vectors, runs
  one first pass and one immediate repeated pass, and records raw IDs,
  per-query start/end times, p50/p90/p95/p99, completed QPS, logical
  request/response bytes and observed concurrency. Its timed region
  includes JSON encode, the full HTTP exchange and JSON decode. The
  V220 loopback timer excluded JSON codec work, so its latency is a
  different timing scope and cannot be subtracted from V222.
  service uses a resident FP16 plane after authenticated hydration,
  no response cache, and zero vector-body GETs by construction.
- Seal and verify both raw ID files in S3 before downloading the V219
  reference IDs and V198 exact GT100 witness. Each pass must match
  V219's 4,096/4,096 ID lists exactly: 99,664/100,000 GT100 hits,
  p05 98. Each pass must also have p95 <100 ms, p99 <150 ms and
  >=100 QPS; server peak RSS <=3 GiB. Record both instance identities,
  hardware/AZ, elapsed time, Spot quote and compute cost estimate.
  A negative quality, latency or memory result remains a complete
  measurement with sealed raw samples and a false gate verdict; only
  interrupted or incomplete measurements are discarded.

V221 uses the same panel, k, client instance class and eight concurrent
requests, but S3 Vectors is a managed HTTPS service with opaque server
cache state. BORSUK uses private HTTP to a resident index. These
conditions must remain visible in the comparison; p90, p95 or p99
alone cannot establish an equal-cache or equal-transport win. The V36
GT100 is squared Euclidean while both V221 and V222 search by cosine;
the source has measured near-unit norms, not exact unit norms.
