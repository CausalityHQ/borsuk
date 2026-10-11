# Reconstruction native continuation a0002

Source91107713f6f7bd700b5b6d92a83e438febac8069; origin/main483b7f69 unchanged until complete native qualification. Native source431/full closure464; source archive c8241fc08be7c7207e8a618a34225e660c7ccfcd635c8c5443fabfc68339d7fe, full map85083d48eecd90dec99ae38f0fdfd8095ae17b09e8f1d45b4fcc7bcf7d0ee0dc, native map377d171c0b40c1291a3fd75779b2a9cdabe6a66e3c140bd39d5324f974bdaf65.

Original a0001 SSM8ff109ef-4471-4846-b22f-664b5438383e CLOSED42, preserved INVALID roster/setup. Original debug10/release10/full137 passed once each, body manifests and full before/after/root maps authenticated, exact names match separately source-derived roster-r2; no OOM/swap. No threshold or source change. Original instance i-0995be499aee87583 terminated, volume vol-0efa299ba5c74a400 deleted, root timer stopped, BG cleared. Original failing observation: 124 roster omitted13 tokio async tests; actual native full137 passed473.17s, not a performance measurement. Reuse only these exact unchanged-source tests, never relabel original batch42 as success.

New continuation runs ONLY the genuinely UNRUN original commands, in order:
1 cargo build --release --locked -p borsuk --example compare_native_replay (retain/hash actual ELF)
2 cargo clippy --locked --workspace --all-targets -- -D clippy::correctness -D clippy::suspicious
3 env -u BORSUK_TEST_BUILD_COMMAND BORSUK_TEST_BUILD_JOBS=1 CARGO_BUILD_JOBS=1 bash scripts/check_rust_test_build.sh

Scripts copied from frozen a0001 with ONLY output/input prefix a0001->a0002. Independent inverse recovered original exact bytes. Same Rust1.98.0, manifest defaults, no scalar-control, unset flags/target/wrappers/test-build shim, jobs1, CPU0-3/400%,16GiB/swap0/pids512/core0, full before/after source/hash/readback and drain receipts. Bootstrap source authentication and installed compiler identity are the disposable environment smoke; no measurement/data/query/truth/ANN runs occur. Native batch submitted only after actual source/setup receipts independently pass.

Spot-only c7i.4xlarge,96GiB encrypted gp3/DeleteOnTermination, eu-central-1c. Fresh API quote0.3629/h at20:00Z (not guarantee); MaxPrice0.60. Safety max3h compute1.80USD+0.50 ancillary=2.30USD ceiling, not spend. Root timer10920s, machine shutdown10800s, native deadline min(now+9000,machine_stop-240). One RunInstances/client-token/claim. Spot interruption makes attempt INVALID; preserve terminal partial receipts, no automatic paid retry. Stop at first native/environment failure; drain owned unit and record native+manager separately. Terminate immediately at terminal, independently prove volume deletion; no idle reuse.

Final qualification must compose six actual original commands across a0001/a0002 with complete source/config/compiler equivalence, actual exits0, ELF verification and infrastructure closure. This continuation's batch never claims total native_qualified. No real1M reconstruction, historical production parity, ANN recall, cold latency, competitor win, defaults or product acceptance follows from compiler/synthetic correctness.
