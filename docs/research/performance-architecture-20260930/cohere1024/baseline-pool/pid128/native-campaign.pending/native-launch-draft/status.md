# Native EC2 launch draft — source only

This is minimal experiment glue, not library or benchmark implementation.
Native execution remains on causality EC2 Spot in eu-central-1.

The transport entrypoint takes a SHA-pinned manifest and fetches seven opaque
bodies. It authenticates exact length and SHA before create-only publication;
it never decodes Parquet, requests, truth or results and runs no native program.
The original qualified ELF and shard pins remain unchanged.

Static check on 2026-10-10: bash -n and ShellCheck exited 0 in user service
run-p3513636-i703822791.service, invocation
dc8d1f098e184ea4b7cf25133bf3a7b3, CPU1/CPU0 affinity/256MiB/zero swap/
PID128/120 seconds. Runtime 111ms, peak 24.1M. This did not execute transport,
jq admission, a fixture, or Rust. Runtime is UNVERIFIED.

Source assembly update:

- native-worker.sh and native-watch.sh derive from the accepted r7 platform
  sources; assemble-worker.py reads text only and generates 19 pinned support
  rows. It executes no experiment entrypoint.
- Native worker setup and transport share launch+700. The full controller
  allowance plus 400 seconds must remain before launch+17900. The independent
  watcher requests termination at launch+17900, waits for confirmation and
  root-volume absence before downloading the evidence archive. These are
  request/observation deadlines, not an AWS billing guarantee.
- Cleanup authenticates the parent and the four recorded observer identities,
  then the eight possible phase names in each stage. Its manager calls share
  a 90-second safety budget. Failed identity, stop or drain remains INVALID.
- The prospective terminal schema is borsuk-native-pid128-ec2-v1 and records
  controller_execution_attempted rather than claiming no native work occurred.
- S3 transport now HEAD-pins ETag/length, GETs with If-Match and an inclusive
  expected-length-plus-one bounded range, checks response ETag/ContentRange,
  then checks exact body length/SHA. HTTPS remains HTTPS-only/max4 redirects.

Latest static source assembly/check service run-p3658573-i703980799.service,
invocation d2f54459c66f485ebf552167023e4700: assembly, bash -n and ShellCheck
worker/watcher all exited 0; CPU1/CPU0 affinity/256MiB/zero swap/PID128/120s,
497ms, peak34.8M. Transport bash -n/ShellCheck exited0 in
run-p3566844-i703868667.service, invocation1c57d17b970d4087a4a648e21066fd46,
140ms/20.7M. These checks did not run transport, jq admission or Rust.

Remaining launch requirements:

- Independently review the assembled bootstrap/watcher/transport against the
  accepted source and exact current config. Resolve demonstrated findings.
- Keep the 18,000-second machine envelope. Before native start require at least
  16,800 seconds for the controller plus 400 seconds for teardown, evidence
  publication and termination request. Refuse before native execution otherwise.
  The independent watcher must request termination before the machine cutoff;
  confirmed termination remains separately recorded, not guaranteed billing time.
- Install/setup and transport share the remaining 700-second budget. Every
  blocking call is clamped to its remaining deadline. No retries or extension.
- Review the assembled wrapper and transport once as an immutable source slice.
  HTTPS redirects are HTTPS-only/max4; unlike the older historical campaign,
  this prospective transport does not authenticate each redirect host.
- Target HEAD is only a length/ETag precheck, never body authentication. Exact
  body SHA remains mandatory. Range/If-Match response handling is target-UNRUN.
- The receipt means transport authentication only; the actual runtime input
  admission, separate native staging, controlled-negative drain and PID128
  widths remain required. No performance or cold comparator claim is admitted.
- Only the final root-owned launch may create an attempt/token/RunInstances.
  No launch, instance, runtime fixture or paid experiment was created here.

Root resource/request draft, 2026-10-10: request.pending.json retains the accepted
platform AMI/profile/subnet/SG/IMDSv2/termination options, changes only to
c7a.2xlarge, 48GiB encrypted gp3 root and Spot max $0.25/hour, and keeps an
explicit PENDING_ROOT_UNIQUE_ATTEMPT token/name. It is not an admitted request.
resource-plan.pending.json records the five-hour envelope and $1.50 planning
allowance ($1.25 compute plus $0.25 ancillary), not a billing cap. A read-only
causality EC2 quote in spot-quote.json returns $0.193400/hour, quoted at
2026-10-10T08:00:00+00:00. No RunInstances call was made.

One new immutable-wrapper dual review group259d91aacc8c4b8f completed against
7cffa1552a62fab65f6c983524d027dd3e57132a. Both critics were confirmed running;
the research critic continued through the configured usage-limit fallback.
No duplicate group was launched. All runtime and release acceptance remains
pending the actual reviewed findings and exact EC2 execution.

Superseding reconciliation: see review-reconciliation.md. The original reviewed
wrapper remains immutable in7cffa155; current generated source incorporates the
verified findings. Runtime remains UNVERIFIED.
