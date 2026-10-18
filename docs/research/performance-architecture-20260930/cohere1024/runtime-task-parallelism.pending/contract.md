# Same-binary runtime/task comparison

Base 55f09d4272adfe3da1691a14543f3fe089097314. This packet qualifies code and synthetic fixtures; it does not establish a performance result. Root owns integration, actual admission, source/config/ELF pins and experiments. Existing Q1/Q2 cooperative measurements remain a separate Pareto point.

## Intervention and ownership

Concurrent config v17 / result v18 accepts explicit max_active_queries 1 or 2 and query_runtime current_thread (default) or multi_thread2. Production comparison holds Q2, full execution, baseline serving, fixed40 Scale1m, one million rows, fetch64, Offload2 and modeled cap2GiB. Split/byte-window interventions remain forbidden. The legacy serial config v15 / result v16 remains current_thread by default.

Both new runtime arms dispatch the same owned_query through QueryTasks::spawn, with the same ordered batches. Only the runtime worker configuration changes. Tokio rt-multi-thread already existed; Cargo and library algorithms are unchanged.

The old join_all polled both borrowed queries in one task. The replacement owns each query row and Arc pins of the one generation and reader; tokio::spawn statically enforces Send and 'static for the real future and output. Generation slots remain library-owned. SOURCE compute slots/workers remain global2, independently of per-query downloads64.

Every started handle is awaited in issue order, including after an error or panic. Successful siblings and their charges survive; earliest issue-order error wins. Later batches, seal and truth stop on failure. QueryTasks Drop requests abortion, not synchronous completion. Explicit abort followed by drain, or destruction of the owning runtime, is the cancellation completion boundary. Runtime destruction also joins blocking work. The library's SourceAuthWorkers Drop waits for its group; last SourceAuthPool Drop closes and joins its workers. No new library cancellation protocol is introduced.

## Model and evidence schema

The runtime builder explicitly sets 2MiB stacks and maximum blocking threads64. Dedicated runtime worker threads are0 for current_thread and2 for multi_thread2. Before receipt, generation or backend access, Handle.runtime_flavor and metrics.num_workers must match the configured runtime. The observed metrics count is1/2; dedicated worker count is0/2. A mislabeled runtime refuses before backend creation.

Runtime reserve is checked arithmetic:
(max_blocking_threads + 2 charged runtime workers) * (2MiB stack + 64KiB state allowance) + 512KiB caller future.
Both arms reserve143261696B. Actual dedicated workers remain0/2; the common conservative charge preserves identical already_pinned_bytes and staging capacity. These are modeled bounds/allowances, not observed physical occupancy or a claim to account for arbitrary SDK/TLS/allocator internals. The separate external8GiB gate remains required.

Each task checks max(actual unpolled future size, output size) + JoinHandle size +4096 state allowance <=512KiB before spawn. Task count is bounded by admitted Q1/Q2. Existing query rows/results/plans/trace reservations scale by active queries; cohort/terminal JSON retains its1MiB reserve. The actual caller future is checked in tests. Model-1 refuses and model admits before any SOURCE/SQ8 payload read. Runtime threads are prechecked before their allocation.

Identity and bound_inputs independently pin query_runtime, query_dispatch=spawned_ordered_batches and query_wall_semantics=dispatch_to_completion_including_initial_runtime_queue. Startup records observed runtime flavor and num_workers, dedicated workers0/2, common charged workers2, blocking bound, stack/state/task allowances and the computed reserve. Reader validates their exact roster, values and agreement with independently authored headers. Config v16/result v17 concurrent artifacts intentionally require their historical reader; no new compatibility path was added.

Each query wall starts synchronously before spawn, so its latency includes initial runtime scheduling delay. Library stage intervals retain their existing boundaries. Per-query process CPU and transport remain null. One real cohort CPU interval/counter delta covers all tasks and record emission. Cohort elapsed must cover the sum of ordered-batch maximum query walls. Complete/seal/SHA/EOF checks remain intact.

## Focused checks

Runner tests:
- runtime_task_workers_and_dispatch
- runtime_task_exact_parity_and_selector
- runtime_task_owned_query_fault_panic_cancel_drain_and_model
- runtime_task_ordered_faults_full_drain
- runtime_task_runner_panic_or_fault_stops_later_batches_without_seal
- runtime_task_drop_drains_at_runtime_boundary
- runtime_task_caller_admission_boundary_and_runtime_mislabel
- runtime_task_source_active_cancellation_drains_and_reuses_slots
- runtime_task_dispatch_wall_includes_delayed_first_poll

Reader adds runtime_task_reader_mutations and imports the nine runner cases. Count these as10 distinct names and19 executions. Preserve concurrent_probe's4 runner cases plus1 reader case, ten source_auth_ library tests, and the current CPU profile producer/reader regression.

The worker witness synchronously holds two tasks inside the same production dispatcher and requires distinct OS thread IDs under multi_thread2. Current_thread tasks run on the caller thread. Real query tests additionally prove owned lifetimes, exact IDs/score bits/plans/logical GET+byte charges, both slots reusable after faults/panic/cancel, no later batch after failure, and zero payload reads at library model boundaries. The configured runtime constructor separately refuses caller/runtime reserve minus1 and admits the exact reserve. SOURCE cancellation holds real response bodies with both shared workers alive, then checks explicit drain, body release, worker exit, slot reuse, weak generation/client/transport pins and bounded runtime teardown. A test-only issue ordinal/completion scope forces ordinal1 to finish before ordinal0 fails; production dispatch is unchanged. Q1 parity uses the independent legacy inline producer. The coherent alternate-runtime reader fixture first passes internal consistency, then fails against frozen original expectations.

Qualification commands, exits, original failures and final ELF pins are in qualification.md. Unchanged343 historical reader passes are reused from the base's recorded evidence; no full historical20-minute reader rerun is represented as new evidence.
