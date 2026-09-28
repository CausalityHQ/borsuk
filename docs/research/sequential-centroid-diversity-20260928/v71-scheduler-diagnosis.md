# Full-gate scheduler fixture failure

Green workeri-0f4d6452c2915e3a3 completed with terminal101, authenticated artifacts/source and verified termination. Focused centroid14passed/2ignored, unit graph6passed. Partial full workspace144target summaries:2674passed/1failed/26ignored; not a full pass. Failure is V71 throughput_jobs_execute_independently_under_the_admission_limit atmain.rs1248: peak1 vs2.

All helper callers traced: run_spawned_bounded lazily spawns jobs while buffer_unordered admits at most limit join handles; production throughput invokes it with the worker count. That code is unchanged. The old fixture's synchronous50ms sleeps assume the second Tokio worker will schedule another job before the first sleep ends. Independent spawn capability does not guarantee this timing under load; the same untouched fixture passed the preceding full gate. No graph-code caller connects to V71 scheduling.

Replace timing inference with a synchronous Mutex/Condvar rendezvous. Both jobs must enter synchronous work before either exits; no async yield can make a serial parent look concurrent. A10second condition wait is only a deadlock/failure ceiling; success releases immediately on two arrivals. Return whether the rendezvous occurred and require both outcomes plus peak2. No production scheduler, throughput/QPS method, historic artifacts or measurement changed.

Original failed terminal/logs remain immutable. Run the failing V71 bin layer first, then one final full gate on a new, serial frozen correctness worker after prior termination. This is a repaired test layer, not an automatic retry of the unchanged failed source.
