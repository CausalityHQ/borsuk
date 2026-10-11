# Runtime wrapper repair R1 — source/static only

Both critics in completed group79a72f3097974bb0 returned BLOCK on the exact original hashes; results are retained separately in full. Original scripts remain immutable in review-input/. Rust source, ELF and all native qualification evidence are unchanged.

Repair scope: existing three ephemeral Bash scripts only. IOAccounting=yes is explicit on the owning slice/native service; hydration requires io enabled in root/slice subtree_control and records hierarchy/service evidence. Final hydration service launch must also set IOAccounting=yes.

To avoid relying on buffered backing-cache attribution (including the loop worker allocation fallback), hydration now explicitly creates an owned loop mapping with --direct-io=on, verifies sysfs dio==1 and exact backing path, mounts that exact device, and verifies mount source. Hydration and native completion recheck DIO; native preflight rechecks backing path/mount source. Loop filesystems still have their ordinary cache charged to the shared memory boundary. Actual target support and behavior remain UNVERIFIED; no kernel-wide cache/overhead exclusion claim.

Hydration service/slice and native service/slice peaks must be nonempty decimal values of at most10 digits and <=1073741824; final supervisor also rejects missing/malformed/over-limit cumulative slice peak. Zero swap/OOM checks remain. Failures are execution INVALID, never scientific KILL.

RuntimeMaxSec1230 plus stop grace30 is counted as1260s; supervisor now requires that full remainder before launch. Protocol draft supersedes stale3600/3720 by4500/4620 and conservative4320s serial ledger. Receipt generation/sync/cleanup, instance/attempt binding and final outer supervision remain open; this repair does not authorize freeze.

Authoritative syntax-only check: borsuk-neighborhood-glue-repair-static-a0002.scope, invocation d91ed9152bb74aac9dc44d14f4d901cd, CPU100%/AllowedCPUs0/256MiB/zeroSwap/TasksMax128/RuntimeMaxSec120, exit0; bash -n invoked separately on each of three files. Earlier a0001 invocation passed multiple filenames to bash -n, which only parses the first; it is NOT evidence for all three. No script runtime/mock/native/data parsing/EC2 launch occurred locally.

The final one-line1260s remainder change passed the affected-file bash -n check in borsuk-neighborhood-glue-repair-static-a0003.scope with the same bounded properties, exit0. Final script hashes: setup9c873a605260afa118ca8730491102b7995137f49404cdadb7b3602f15343fc2; inner015e2a73fc62b8c5aca745057a1c39d3e603fdd60f64b453245337aa853a506d; supervisor12d5c29734ed49e9c7598bba8cb36a6aa17b4dbed8d68495462e71366236ea6e. Remaining necessary work: exact identity-bound package, aggregate stage supervision/receipt bounds, failure archival, final closure acceptance, pinned environment and fresh price/placement/cost metadata. No duplicate review has been started.
