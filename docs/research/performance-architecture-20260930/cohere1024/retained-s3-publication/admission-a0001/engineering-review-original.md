**Hold the protocol freeze for three wrapper fixes.** I found no new native publication correctness blocker in the reviewed paths.

1. **P1 — Early bootstrap failures bypass cleanup.**
   [user-data.sh:3](/data/target/borsuk-retained-s3-publication-admission-a0001/user-data.sh:3) arms the shutdown timer before installing the exit trap at line 36. If timer creation fails, `set -e` exits with neither shutdown protection nor a terminal receipt. Later, an apt failure before installing `jq` leaves `finish()` unable to generate its JSON terminal.
   **Smallest fix:** install a minimal failure/shutdown trap before fallible setup, with a serial terminal fallback that requires neither `jq` nor AWS CLI. Preserve the original failure code.

2. **P1 — The deadlines do not reserve time for closeout or enforce the stated machine ceiling.**
   The timer starts relative to its activation, after boot, and has no explicit accuracy setting; systemd’s documented default permits another minute of timer slack. [Input downloads](/data/target/borsuk-retained-s3-publication-admission-a0001/user-data.sh:51) have no total timeout, and native execution starts without checking remaining time. A slow bootstrap can therefore start publication immediately before shutdown, interrupting publication or evidence upload. `RuntimeMaxSec=600` also leaves termination grace implicit.
   **Smallest fix:** bind the cutoff to the recorded machine deadline, set timer accuracy and stop grace explicitly, bound staging downloads, and refuse native startup unless its 600 seconds plus bounded closeout fit. Freeze shutdown-to-termination behavior in the launch configuration.

3. **P2 — Terminal failures can still produce a successful process exit.**
   Inside `finish()`, `set +e` applies, but [terminal generation and upload](/data/target/borsuk-retained-s3-publication-admission-a0001/user-data.sh:29) do not update `status` on failure. If native execution and evidence uploads succeed but terminal upload fails, the script still exits zero. IMDS identity lookup failure likewise permits an empty instance identity.
   **Smallest fix:** check identity, JSON generation, and terminal delivery explicitly; preserve `original_exit`, record a separate closeout failure, and emit the fallback terminal. This does not forge valid admission evidence, but it currently reports success without delivering the required terminal.

The promised root closeout must cover **every outcome**, including timeout or missing terminal. The [existing lost-head-response test](/home/rb/worktrees/borsuk-prod-ready-v9/crates/borsuk/tests/two_bit_generation.rs:613) demonstrates that publication can return an error after committing the head. Record destination head/root state independently and preserve referenced metadata. For leftover MPUs, freeze an attempt-scoped abort-and-recheck procedure; an empty preflight listing alone cannot cover interruption. No shared lifecycle change is needed.

Verification performed: both scripts individually passed `bash -n`; config/script hashes match the draft; retained manifest SHA matches the approved root; all three reviewed Rust files match both `910b8b0` and qualified candidate `4c2e535`. The qualification receipt records seven zero-exit gates and instance termination; I did not rerun them.

Before launch, exercise the wrapper’s failure paths in a disposable smoke. Larger-sidecar support and scale qualification remain future limitations. Full query admission and the separate canary remain prerequisites for measured panels. No files changed, jobs launched, network accessed, or corpus bodies read.
