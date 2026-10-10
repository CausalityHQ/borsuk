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

Remaining launch requirements:

- Assemble the bootstrap and independent root watcher from the accepted platform
  source. Preserve original parent/observer/payload identity and terminal status.
- Keep the 18,000-second machine envelope. Before native start require at least
  16,800 seconds for the controller plus 300 seconds for teardown, evidence
  publication and termination request. Refuse before native execution otherwise.
  The independent watcher must request termination before the machine cutoff;
  confirmed termination remains separately recorded, not guaranteed billing time.
- Install/setup and transport share the remaining 900-second budget. Every
  blocking call is clamped to its remaining deadline. No retries or extension.
- Review the assembled wrapper and transport once as an immutable source slice.
  HTTPS redirects are HTTPS-only/max4; unlike the older historical campaign,
  this prospective transport does not authenticate each redirect host.
- Target HEAD is only a length precheck, not body authentication or a bound on
  an object changed between HEAD and GET. The source objects are prerequisites
  for an immutable campaign; exact body SHA remains mandatory. Resolve a strict
  download byte cap before launch, including the S3 GET path.
- The receipt means transport authentication only; the actual runtime input
  admission, separate native staging, controlled-negative drain and PID128
  widths remain required. No performance or cold comparator claim is admitted.
- Only the final root-owned launch may create an attempt/token/RunInstances.
  No launch, instance, runtime fixture or paid experiment was created here.
