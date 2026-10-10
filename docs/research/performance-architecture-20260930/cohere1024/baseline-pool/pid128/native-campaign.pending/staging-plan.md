Feasible with one small root-owned staging/transition script; no Rust changes or compilation. This specification uses Git `835d6edf26d01bafbb5284019703bbd2c98c882f` and the verified recipe SHA `ee8327efb4f744d5acc6d8b2665867ca1a3f8a80908e45a83b0a650168baad65`.

1. **Close actual admission before staging.** Run the existing admission recipe unchanged. Its ordinal0 diagnostic uses real Q32 requests, records and truth, then emits immutable admission evidence ([recipe lines 786–824](/tmp/borsuk-root-pid128-repair-r3/run-native-pid128.sh:786)).

   Launch its observer as a unique `system.slice` service with `Type=exec`, `RemainAfterExit=yes`, CPU0/quota100%, memory268435456, swap0 and Tasks128. An enclosing Bash wrapper must wait for the recipe, record the actual returned status under `$E/outer/admission/outer.exit`, and exit with that status. `$E` is the common campaign evidence root, outside `ROOT`, on the same filesystem.

   The external collector must wait finitely for the **same InvocationID** to reach terminal process state. Before stopping the retained observer, capture:

   - Raw manager fields: exact Id/Description/InvocationID/ControlGroup, `MainPID=0`, `Result=success`, `ExecMainCode=1`, `ExecMainStatus=0`.
   - Actual enclosing exit file containing exactly `0\n`.
   - Actual observer `cgroup.events` containing `populated 0`.

   Only then stop the exact owned unit and write the root-owned `borsuk-native-pid128-outer-closure-v1` receipt. Missing raw evidence fails closed. These are the existing widths admission requirements ([recipe:600](/tmp/borsuk-root-pid128-repair-r3/run-native-pid128.sh:600)). A successful `systemd-run` start is insufficient.

2. **Run a separate staging smoke on the admitted fixture.** Preregister a fresh sibling namespace:

   ```text
   P=/mnt/borsuk-pool-pid/prepared-parent
   S=/mnt/borsuk-pool-pid/staging-<attempt>
   ```

   Create canonical, nonsymlink `$S/native/scratch`. Transform the existing diagnostic config by changing **only** `scratch_parent`:

   ```bash
   jq --arg scratch "$S/native/scratch" \
     '.scratch_parent=$scratch' "$P/configs/diagnostic.json" \
     > "$S/native/config.json"
   ```

   Require equality after deleting `scratch_parent` from both configs. Preserve width32, `diagnostic_panel`, ordinals `[0]`, tracefalse, count32, all receipt/input descriptors, generation prefix/root and the existing local store.

   Exact native argv:

   ```bash
   /mnt/borsuk-pool-pid/assets/bin/check_cohere_native_baseline \
     "$S/native/config.json" "$staging_config_sha256" \
     "$S/native/result.jsonl"
   ```

   Use the recipe’s clean environment and layered payload runner unchanged: CPU0/quota100%, memory536870912, swap0, Tasks128, native timeout900s, kill grace10s and existing bounded closure. Validate using the exact `validate_result` function with count1. The native entrypoint has no validation-only mode; this invocation performs genuine receipt admission, source binding, generation open, query sealing and truth reduction. Source references at the authoritative revision: `check_cohere_native_baseline.rs:2540–2606,2664–2684`.

   Also run the frozen nine-case and fifteen-case shell validators in separate staging output directories, each under CPU0/256MiB/noSwap/PID128/120s. Disclose their synthetic records and mocked metadata. The native smoke performs real LocalFileSystem operations; these synthetic checks establish validator behavior only. Reuse existing import/SDK/transport canary operations with exact new pins, without duplicating cohort construction or publication.

3. **Include one controlled cleanup falsifier.** In its own disposable observer/payload service, invoke the exact extracted `run_phase` with a five-second payload deadline. A bounded Bash payload starts one TERM-ignoring, `setsid` descendant running a finite 60-second sleep; it remains inside the authenticated payload cgroup while escaping GNU timeout’s process group.

   Require the expected INVALID outcome, exact ownership checks, TERM grace followed by recorded `cgroup.kill=1`, actual drain proof, terminal manager evidence and successful owned-unit stop. A missing `native.exit` remains missing—never synthesize zero. Preserve this negative separately; staging passes only if it fails for the intended timeout and proves cleanup. Preflight `/usr/bin/setsid`; no unbounded loop or extra descendant tree.

   Proposed staging maximum: **1800s**, including both validator runs, native smoke, negative cleanup and authentication. Keep observers at256MiB and the native payload at512MiB, serially. Reserve512MiB transient native metadata scratch,64MiB result output, the recipe’s64MiB observation allowance, bounded logs, and separately frozen closure/evidence space. Metadata staging admits cumulative bytes beneath the512MiB memory budget (`object_native_generation.rs:422–531`). Count retained staging files through `ROOT` and evidence through `$E` exactly once; endpoint `du` is not a peak measurement.

4. **Authenticate immutable state, then freeze widths deterministically.** Before and after staging, reproduce the recipe’s complete immutable roster and compare it with `admission.json.immutable_files`, including path, bytes, SHA and device/inode/mtime/ctime stamps ([recipe:580](/tmp/borsuk-root-pid128-repair-r3/run-native-pid128.sh:580)):

   - Every file under `cohort`, `derived`, `generation`, `store` and `configs`, plus `publication-receipt.json`; reject additions, omissions and symlinks.
   - Parent device/inode and actual staged SQ8 ETag.
   - Both Parquet bodies, all five qualified ELFs, authority receipts, recipe and input/template manifests.
   - Admission’s exact closure roster and hashes, terminal, wrapper exit and external closure evidence.

   Specifically preserve `configs/{diagnostic,query16,query32}.json`, `store/semantic/index/head.json`, the published generation manifest, and `store/semantic/objects/<actual-sq8-sha>`. Never copy or republish the head to establish staging authority.

   Root verifies and seals the staging receipt, then create-only materializes the widths config by changing only `stage`, `root_gate`, `admission` and `admission_closure`. Preserve the preregistered scratch/resource fields. Bind the staging receipt SHA and actual admission/terminal/manifest/outer SHAs in the existing root-gate-v2 fields. Run the unchanged recipe with fresh `$E/widths`; its create-only marker, width16 closure and subsequent width32 sequence already exist ([recipe:826](/tmp/borsuk-root-pid128-repair-r3/run-native-pid128.sh:826)). Collect widths’ actual outer exit before stopping its retained observer, then seal/upload terminal evidence and immediately terminate and prove disposable volumes absent.

The concrete recipe constraints are: **no staging mode**, admission cannot rerun against the occupied parent, and widths authenticates a **staging SHA only**, not the staging receipt body ([recipe:412](/tmp/borsuk-root-pid128-repair-r3/run-native-pid128.sh:412)). The root driver must enforce staging acceptance before minting that gate. Extract exact helper blocks with pinned source/count checks; do not source the whole executable recipe.

Unresolved launch pins remain the driver/canary hashes, exact SDK/import operations, AMI/package/counter support, aggregate scratch/free-space reserves, observer overhead, whole-campaign/watchdog deadlines, transport identities, Spot instance/cost ceiling and final publication pins. The inner runner’s single manager-final read after drain also remains a possible fail-closed timing race; staging must expose it.

Only static source/metadata reads were performed. No native admission, cleanup, infrastructure or performance execution occurred; this plan establishes no cold,1M or competitor result.
