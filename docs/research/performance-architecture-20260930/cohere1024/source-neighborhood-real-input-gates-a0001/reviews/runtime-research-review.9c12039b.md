What this change does: It stages the six complete authenticated inputs and qualified ELF, then runs 1/1 admission followed by a fresh 32/32 smoke. It preserves native geometry as opaque evidence and publishes checksummed artifacts before removing stage outputs.

**Verdict: BLOCK freezing these exact script hashes. Two small resource-accounting fixes are needed; neither requires Rust changes.**

1. **The loop backing cache can escape the owning memory slice.**

   [setup-data.sh](/data/target/borsuk-source-neighborhood-data-gates.pending/setup-data.sh:17), lines 17–30, enables CPU, memory and task accounting, but no I/O accounting. Its line 49 mounts a buffered loop filesystem. [run-data-stage.sh](/data/target/borsuk-source-neighborhood-data-gates.pending/run-data-stage.sh:21) likewise supplies no I/O-controller setting.

   Linux’s loop driver obtains its memory-cgroup association from the request’s **I/O cgroup**, then uses that association for backing-file allocations (`loop.c`, lines 1748–1786). If the effective I/O cgroup is root, backing-file cache is charged outside `borsukneighborhood.slice`. Systemd defaults I/O accounting to off unless configured or enabled elsewhere. Thus the current memory checks can pass while omitting backing cache. This is a concrete supported configuration, not proof that an unspecified future AMI will exhibit it. [Linux loop source](https://raw.githubusercontent.com/torvalds/linux/v6.8/drivers/block/loop.c), [systemd controller activation](https://raw.githubusercontent.com/systemd/systemd/v255/man/systemd.resource-control.xml), [systemd defaults](https://raw.githubusercontent.com/systemd/systemd/v255/man/systemd-system.conf.xml).

   **Smallest fix:** Explicitly enable `IOAccounting=yes` for the owning slice and hydration/native services. Before hydration, fail closed unless the effective I/O hierarchy covers the owning slice; retain that evidence and pin a kernel with the required loop memory-accounting support. Buffered backing cache must remain part of the bound. Otherwise, a successful smoke could falsely qualify the charged-memory envelope.

2. **Acceptance does not enforce a measured 1 GiB peak.**

   [native-data-inner.sh](/data/target/borsuk-source-neighborhood-data-gates.pending/native-data-inner.sh:35), lines 35–43, records service and slice peaks but checks only swap and OOM. Its line 59 checks the Rust **arithmetic estimate**. [run-data-stage.sh](/data/target/borsuk-source-neighborhood-data-gates.pending/run-data-stage.sh:40), lines 40–47, can accept without comparing the recorded owning-slice peak with `1073741824`.

   `memory.max=1GiB` establishes the configured kernel limit; it does not establish that measured usage never exceeded it. Linux explicitly permits temporary excess. Consequently, a zero-OOM, zero-swap run with a peak above the proposed actual ceiling can receive `GATE_ACCEPTED`. [Kernel memory-limit semantics](https://docs.kernel.org/admin-guide/cgroup-v2.html).

   **Smallest fix:** Require valid numeric service and cumulative owning-slice peaks no greater than `1073741824`, including hydration and the final stage snapshot. Preserve an exceedance as execution **INVALID**. Otherwise, describe the result only as execution under a configured limit, not proof of an actual 1 GiB peak.

The remaining reviewed mechanics support proceeding after those fixes:

- **Scratch:** `setup-data.sh:43–52` creates a fresh 8 GiB backing file and verifies ext4 capacity. This provides an enforced filesystem ceiling; the `du` checks are supplemental. Inputs, ELF and stage evidence fit the declared layout. Root-volume installer/support overhead still needs separate final admission.
- **Native authority:** Exact `fc60c18e` source authenticates generation descriptors before large-body opens (`compare_native_replay.rs:6157–6192`), validates every leaf (`6359–6416`), and performs both complete canonical scans (`6437–6476`, `6679–6758`). The 1/1 configuration preserves those full validations.
- **Opacity:** Native stdout remains redirected to `native.log`; report/seal predicates select identity metadata. I found no wrapper path printing neighborhood geometry.
- **Publication:** `run-data-stage.sh:65–81` supplies SHA256 on each create-only PUT, checks the returned checksum, and uploads the terminal last. This is a valid integrity mechanism; a second full-body download is unnecessary solely to verify successful single-object PUTs. [AWS PUT checksum contract](https://docs.aws.amazon.com/cli/latest/reference/s3api/put-object.html).
- **Stage separation:** Admission evidence/config deletion follows successful uploads, and smoke refuses their continued presence. Original native and manager statuses are retained.

**Missing final-freeze conditions, separate from those code bugs:**

- Freeze the actual SSM command, AMI/kernel/systemd identities, hydration service properties, SDK-install resource limits, serialized config hashes and support/transport hashes. Resolve `authority_pending:true` only after authority verification.
- Replace the stale **3600/3720** figures in `protocol.draft.md:15` with the reconciled **4500/4620** envelope. Account explicitly for timeout kill grace, possible service stop grace, receipt generation/sync and closeout. `run-data-stage.sh:62` starts its publication clock after receipt preparation, so that preparation needs its own enclosing bound.
- Bind stage artifacts to the exact instance and attempt. The stage terminal at `run-data-stage.sh:52` currently contains no instance identity; qualified source line 6785 requires that binding.
- Treat the uploaded `GATE_ACCEPTED` terminal as native-stage evidence until the original wrapper also exits successfully. It is published **before cleanup** at lines 81–88, so it cannot itself certify cleanup completion. Final acceptance must include cleanup, instance termination, volume deletion and external-timer closure.
- Freeze all failure-path archival and stop behavior, including install/hydration failures before a native terminal exists. Bind the final Spot/AZ/subnet quote and ancillary-cost accounting to the unchanged $0.25 ceiling.

The separate 32/32 smoke remains necessary: 1/1 success cannot establish its compute deadline. No runtime feasibility or scientific conclusion follows from this static review.

I read the complete requested scripts and documents, checked the qualified source through `git show fc60c18e`, and confirmed all three script hashes match `glue-draft.sha256`. No scripts, native experiments, mocks, cloud actions, children or consultations were run. Compiler receipts, checkpoints, historical 312/320 KILLs and the product goal remain unchanged.
