**BLOCK freezing this exact wrapper.** Two memory acceptance defects need small wrapper fixes. I found no reason to change Rust or repeat its qualification.

I read all named drafts and prior wrappers, inspected committed source at `fc60c18e`, and verified that all three script hashes and the Rust source hash match the supplied pins.

**Must fix**

1. **The loop backing cache can escape the shared memory boundary.**  
   [setup-data.sh:17](/data/target/borsuk-source-neighborhood-data-gates.pending/setup-data.sh:17) enables CPU, memory and task accounting, but does not enable or verify the I/O controller. Line 49 mounts a buffered loop filesystem.

   This matters because Linux’s loop driver derives the backing-file memory charge from the request’s **block-I/O cgroup**, then applies that memory cgroup while performing backing-file I/O. On a host without I/O attribution at this slice, canonical-file cache inside ext4 can be charged correctly while backing-image cache is charged outside it. All current wrapper checks can still pass. This is a source-supported failure condition, not an observed runtime result. See [`loop_queue_rq` and `loop_handle_cmd`](https://raw.githubusercontent.com/torvalds/linux/v6.8/drivers/block/loop.c) and [systemd controller activation](https://raw.githubusercontent.com/systemd/systemd/v255/man/systemd.resource-control.xml).

   **Smallest fix:** explicitly enable I/O accounting at the owning slice before hydration, verify effective controller availability/attribution, and retain the corresponding receipts on the pinned kernel. Alternatively, use and verify direct I/O for the owned loop backing file to remove that second buffered cache. Slice `memory.stat` alone cannot establish that excluded charges are absent.

2. **Acceptance never checks measured peak memory against 1 GiB.**  
   [native-data-inner.sh:35](/data/target/borsuk-source-neighborhood-data-gates.pending/native-data-inner.sh:35) records service and slice peaks, but checks only swap/OOM afterward. Its line 59 checks the Rust **allocation estimate**. Similarly, [run-data-stage.sh:40](/data/target/borsuk-source-neighborhood-data-gates.pending/run-data-stage.sh:40) records the slice peak but can set `GATE_ACCEPTED` without comparing it with `1073741824`.

   A successful run with zero swap/OOM and a recorded peak above the proposed ceiling therefore passes. Linux explicitly permits temporary overshoot of `memory.max`; setting the limit is not equivalent to verifying the observed peak. [Kernel memory-controller documentation](https://docs.kernel.org/admin-guide/cgroup-v2.html).

   **Smallest fix:** require valid numeric service/slice peaks within the frozen ceiling, including hydration and the final owning-slice receipt. Preserve an over-limit attempt as `INVALID`.

**Final-freeze conditions, separate from those defects**

- **Reconcile actual timeout bounds.** [run-data-stage.sh:21](/data/target/borsuk-source-neighborhood-data-gates.pending/run-data-stage.sh:21) specifies `RuntimeMaxSec=1230` plus `TimeoutStopSec=30`: a failure path can consume **1260 seconds**, not 1230. Those bounds make the stated serial ledger **4320 seconds before additional receipt work**, rather than ≤4290. Hashing and filesystem sync also precede the publication deadline at lines 53–62; hydration hashing sits outside the individual download timeout. Freeze aggregate enforcement and count termination grace explicitly. The proposed **4500/4620** ceilings may accommodate this; the current ledger does not demonstrate it. [systemd timeout semantics](https://raw.githubusercontent.com/systemd/systemd/v255/man/systemd.service.xml).

- **Complete the authenticated execution package.** Resolve `authority_pending:true`, freeze actual config/support/transport bytes and hashes, and supersede stale deadline/setup text in `protocol.draft.md`. Bind the terminal evidence to the exact instance and execution identity; the terminal constructed at [run-data-stage.sh:52](/data/target/borsuk-source-neighborhood-data-gates.pending/run-data-stage.sh:52) currently contains neither.

- **Freeze outer supervision and closure.** The missing SSM wrapper must enforce install/hydration limits, collect original failure receipts, stop progression on any failure, preserve PUT receipts, and terminate the same instance immediately. Guest/external timer proof, encrypted-volume deletion, exact Spot placement and the ≤$0.25 cost admission remain launch conditions.

- **Pin environment assumptions.** The scripts assume the AMI supplies usable ext4/loop utilities, cgroup files and systemd properties. The checksum PUT construction matches documented AWS semantics, but compatibility of the installed **2.36.11** binary remains unexercised. [AWS PutObject documentation](https://docs.aws.amazon.com/cli/latest/reference/s3api/put-object.html).

The remaining core flow is coherent: the fresh 8 GiB image provides a real filesystem ceiling; native metadata predicates match the qualified implementation; full input authentication remains intact; geometry stays redirected; checksum PUTs are create-only with terminal last; admission output removal precedes smoke. The 738197504-byte payload cap plus the declared 320 MiB reserve equals 1 GiB arithmetically—without proving runtime usage.

No files changed and no scripts, tests, jobs or consultations ran. Compiler receipts, checkpoints, both historical 312/320 KILLs and the outstanding product goal remain undisturbed.
