Use **one direct SSM payload on a fresh Spot instance**, reusing the committed campaign’s supervision and receipt pattern. No launcher extension or new controller is needed.

Git evidence confirms OWN1 `d077a54f456af30242753ed1e2e397a9416191f7`, parent `6f4b94609dca5f37bcf6cb07a01481a059c6390d`, changes only `crates/borsuk/examples/compare_native_replay.rs`. Its SHA256 matches `d31d454a8e5ec5ed6f7027ea2b6af79fb0dc6ad09346799243635f9d20c393d2`.

The inspected routes cannot qualify it unchanged:

- At `39647f41`, `scripts/launch_native_workspace_execution_spot.py:575–594` fixes the baseline-only delta, replay count **80**, and release artifact `check_cohere_native_baseline`.
- At OWN1, `docs/research/performance-architecture-20260930/scale-derivation-qualification-a0002/user-data.sh` hard-codes the historical source and receipts. Its `gates.sh:26–54` supplies the useful serial stage wrapper: remaining deadline, command/log/time records, and separate native/tee exits.

Execute this plan:

1. **Freeze authenticated inputs.** Produce an archive from the exact OWN1 Git tree, never the dirty worktree. Freeze archive bytes/SHA256, a complete extracted-file manifest, the five commands below, environment, payload digest, and unique attempt/S3 prefix. Authenticate the archive and every source file on EC2 before compilation; recheck afterward. Use a fresh target directory.

2. **Launch one bounded compiler instance.** Root supplies verified AMI, subnet, security group and instance profile. Use `aws --profile causality --region eu-central-1 ec2 run-instances` with:
   - `--instance-type c7i.2xlarge --min-count 1 --max-count 1`
   - `--instance-market-options` containing `MarketType=spot`, one-time Spot, and `InstanceInterruptionBehavior=terminate`
   - `--instance-initiated-shutdown-behavior terminate`
   - an **80 GiB** EBS mapping with `DeleteOnTermination=true`.

   Preregister the prior compiler envelope separately from the pending no-compiler replay: CPU quota **200%**, CPUs **0,1**, memory **8 GiB**, swap **0**, tasks **512**, jobs **1**, gate deadline **7200 s**. Preserve the historical outer unit/machine deadlines **7440/9000 s**. This envelope remains to be qualified for OWN1.

3. **Dispatch native gates through SSM.** Use `ssm send-command --document-name AWS-RunShellScript --instance-ids <id> --parameters file://<frozen-payload.json>`, with `commands` and `executionTimeout=["9000"]`. Record its command ID. Reuse the campaign’s `systemd-run --slice=… --wait --pipe` pattern and resource properties; assert actual cgroup limits before Cargo. Install the machine shutdown deadline before starting gates.

   Set:
   ```bash
   export CARGO_BUILD_JOBS=1 CARGO_INCREMENTAL=0 RUST_TEST_THREADS=1
   export RUSTC_WRAPPER= RUSTC_WORKSPACE_WRAPPER=
   unset BORSUK_TEST_BUILD_COMMAND RUSTFLAGS CARGO_ENCODED_RUSTFLAGS
   ```

   Run these **serially**, stopping on the first failure:

   ```bash
   cargo test --locked -p borsuk --example compare_native_replay completed_header_binding_tests:: -- --test-threads=1

   cargo test --locked -p borsuk --example compare_native_replay -- --test-threads=1

   cargo build --release --locked -p borsuk --example compare_native_replay

   cargo clippy --locked --workspace --all-targets -- -D clippy::correctness -D clippy::suspicious

   env -u BORSUK_TEST_BUILD_COMMAND BORSUK_TEST_BUILD_JOBS=1 CARGO_BUILD_JOBS=1 bash scripts/check_rust_test_build.sh
   ```

   Capture the full harness roster using the same example target with `-- --list`. There are **41 declarations in the example itself**, including seven new binding tests; **41 is not the complete harness count**. Embedded modules include `source_cover`, `scale_native_runner`, and its `scale_derivation_producer`. Require their discovered tests to run successfully in the unfiltered gate.

4. **Close with independently checkable evidence.** Preserve each stage’s command, timestamps, native/tee exits, logs and resource counters; sync completed-stage receipts before proceeding. Copy and hash `target/release/examples/compare_native_replay`, recording bytes and ELF identity. After completion or failure, stop the unit and verify process/cgroup drain before sealing evidence.

   Require every expected zero exit and final receipt explicitly. The historical `terminal-repair.json` documents **systemd reporting success after SIGTERM with missing gate receipts**; systemd status alone is insufficient.

   Upload authenticated evidence and terminal disposition under the unique attempt prefix, using conditional `s3api put-object --if-none-match '*'` writes. Record SSM terminal status through `get-command-invocation`. Then immediately call `ec2 terminate-instances --instance-ids <id>` and `ec2 wait instance-terminated --instance-ids <id>`; retain both lifecycle receipts.

5. **Handle interruption without accepting partial qualification.** Monitor Spot interruption notice/instance health and terminal markers. Preserve completed receipts, classify interrupted or incompletely evidenced execution as **INVALID**, terminate/wait, and restart the unchanged qualification in a new attempt. Do not resume partially completed gates or report a performance KILL.

The bounded falsifier is the **first targeted gate**: it must execute exactly the seven `completed_header_binding_tests`, with seven passes and zero failures/ignored tests, within the shared 7200-second envelope. Compilation failure, test failure, timeout, or missing native exit immediately stops qualification.

Missing prerequisites are the newly authenticated OWN1 archive/manifest/payload, frozen budget and fresh Spot quote, verified EC2/SSM/IAM/S3 access, and a provisioned Rust/Clippy plus workspace native build dependencies. The historical campaign uses Rust **1.98.0**; that toolchain choice needs explicit freezing for this attempt.

No runtime, network, launch, or repaired-logic review was performed. This is a qualification route; OWN1 remains **UNVERIFIED** until those receipts exist.
