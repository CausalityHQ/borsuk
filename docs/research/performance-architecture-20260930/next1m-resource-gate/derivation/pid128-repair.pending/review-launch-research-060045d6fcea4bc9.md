What this change does: It composes one Spot launch, a remote mechanics canary, and collection after instance termination and volume deletion. The collection marker correctly remains provisional.

**Source verdict: block paid launch pending the repairs below.** I reviewed exact blobs at `8fc761b6362420cbd637afda72c034e2b4c8500b`; nothing was edited or executed.

References below are relative to `docs/research/performance-architecture-20260930/next1m-resource-gate/derivation/`.

1. **Launch authority does not enforce one instance or the complete resource envelope.**  
   `runtime-support.pending/next1m-canary-freeze.py:31–43` checks selected request fields but omits `MinCount`, `MaxCount`, complete volume mappings, encryption, and other launch settings. Changing both counts to `2` passes these checks. The launcher discovers the violation only after creation; its failed assignment leaves `instance` empty, so cleanup cannot terminate either instance (`next1m-canary-launch-once.sh:18–19,35–39`).

   Smallest repair: authenticate the exact reviewed request template and validate the complete rendered request before `RunInstances`. Bind the freezer, template, envelope and support roster into root admission. Replace authorization-critical `assert` checks with explicit rejection: Python optimization currently removes them, including the admission and 16 KiB gates.

2. **Receipt failures can leave an accepted launch without a watcher or cleanup.**  
   `runtime-support.pending/next1m-canary-launch-once.sh:35–46` writes the command-exit receipt before extracting or reconciling the instance ID. A failed write exits while `instance` is empty. A failed reconciliation command also exits before the unresolved-outcome notification. Even with a known ID, failure to open `emergency-terminate.json` prevents the termination command itself (`:16–20`).

   Smallest repair: make post-request failure handling reconcile the original token regardless of receipt-write success, retain the exact returned ID immediately, and reuse the watcher’s output-descriptor fallback. Unknown outcomes must remain explicitly unresolved and reach the operator. Never send another launch request.

3. **The outer service starts asynchronously, but ownership is sampled once.**  
   `runtime-support.pending/next1m-canary-worker.sh:154–161` uses `--no-block`, then immediately requires a valid InvocationID. A queued start can fail this check before activation. The identity lookup also uses the setup budget: transport finishing near its deadline can leave insufficient budget to capture ownership after the service has already been submitted.

   Smallest repair: reuse the bounded, blocking `--service-type=exec` launch pattern already present in `wrapper-canary.sh:113–131`. Retain its original launch exit and capture ownership within a separate bounded startup allowance. The worker’s later terminal-state polling is appropriately strict.

4. **Missing cgroups are treated as cleanup proof without resolving ownership or queued work.**  
   `runtime-support.pending/next1m-canary-worker.sh:40,59–77` accepts an absent cgroup or skips the registered unit entirely. An early failed launch can therefore have durable registration, no identity receipt and no current cgroup, yet contribute no cleanup failure. Cgroup absence alone does not establish that a start job cannot subsequently create it.

   Smallest repair: require authenticated terminal/removal evidence for registered units. Missing ownership must record cleanup as unproven even when the cgroup is absent; retain the existing refusal to invent an InvocationID or kill an unidentified unit. The enclosing INVALID result should preserve this distinction rather than reporting `cleanup_exit: 0`.

5. **The watcher does not enforce the 120-second cleanup allowance.**  
   `runtime-support.pending/next1m-canary-watch.sh:18–30,34,68` gives termination its own API timeout and then a fresh 120-second confirmation wait. If that wait fails, the EXIT trap can repeat the sequence because `terminated` remains false. This exceeds the allowance modeled in `pid128-repair.pending/canary-execution-envelope.pending.json:61–62,103`.

   Smallest repair: use one absolute cleanup deadline across the API calls, confirmation wait and EXIT path. Clip each timeout to its remaining allowance and retain the original failure. Report AWS completion as unproven when confirmation fails; elapsed allowances do not guarantee control-plane completion.

6. **The worker’s 180-second cleanup/publication budget is not enforced.**  
   `runtime-support.pending/next1m-canary-worker.sh:32,80–92` permits cleanup to consume 120 seconds, followed by unbounded sync, archive creation, hashing and two upload calls allowing nearly another 60 seconds. The `1690` remaining-time guard (`:152`) treats the whole finish stage as 180 seconds without enforcing that bound.

   Smallest repair: carry one finish deadline through cleanup, archive creation and publication, reserving time for the terminal receipt. Keep the existing scientific limits and shutdown deadlines. The minute-rounded shutdown also needs to be accounted for when admitting the command.

The asset evidence is internally consistent: all nine recorded sizes and SHA-256 values match the reviewed blobs. The worker is **12,403 bytes**; the transported wrapper being **16,543 bytes** is permissible because it is not user data. The final bootstrap remains ung generated and its ≤16 KiB check must survive optimization settings. The four ELF hashes agree between worker and canary; their remote objects were not inspected.

The cost arithmetic is correct **for the assumed duration**: `2620 × $0.60 / 3600 + $0.15 = $0.586667`. The current lifecycle code does not establish that duration bound.

Deliberately UNRUN evidence remains separate: target systemd and kernel enforcement, dependency-stop behavior, escaped-child drain, AMI dependencies, actual asset transport, and full closure replay/refusal coverage. The unchanged full-chain verifier expects a different bootstrap schema and archive layout (`runtime-support.pending/verify-closed.py:938–961`); component checks and authenticated collection cannot substitute for that acceptance seam. No automatic retry or performance qualification is established by this composition.