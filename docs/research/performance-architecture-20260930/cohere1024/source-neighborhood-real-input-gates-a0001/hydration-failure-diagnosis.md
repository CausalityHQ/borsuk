# Original a0001 hydration failure

Authenticated batch collection completed with 31 artifacts; opaque lengths, server SHA256 checksums, and SHA256SUMS passed. Original SSM 5ba5b8eb-93be-4c5a-919b-5112b8319a64 ended Failed/1, completed_stages=0, cleanup_exit=0. No native admission or smoke executed; no scientific result.

hydrate.phase.log records all six input SHA checks and the executable SHA check as OK. hydration.slice.memory.peak is 1116114944 bytes, exceeding the unchanged 1073741824-byte acceptance ceiling by 42373120 bytes. setup-data.sh stops at the numeric slice peak predicate (lines 92–93), before recording the service peak. The recorded slice memory.events has max=78294, oom=0, oom_kill=0, oom_group_kill=0; swap peak=0. The source order and retained receipts identify the first failing acceptance predicate. They do not establish why the kernel peak overshot memory.max or how much headroom a repaired enforcement limit needs.

The captured memory.stat has file=985509888, anon=704512 and kernel=22384640 bytes. This is an end-of-hydration snapshot, not a decomposition of the earlier peak. The available controllers and enabled subtree controllers both include cpuset/cpu/io/memory/pids. Linux documents temporary memory.max overshoot and memory.high reclaim/throttling; neither setting alone proves a strict observed-peak guarantee (https://docs.kernel.org/admin-guide/cgroup-v2.html, memory.high/memory.max).

Disposition: execution INVALID during hydration, native UNRUN; no algorithm rejection, recall result, cold latency result, or vendor win. Preserve the original protocol and artifacts. Any follow-up must explicitly reserve enforcement headroom beneath the existing measured 1 GiB ceiling, verify the owning hierarchy, and use a new preregistered attempt. Do not silently raise acceptance thresholds or repeat paid work.

Closure: exact instance i-0a424b7ede571a137 terminated (waiter exit 0); root volume vol-083a17be78a5d5804 returns InvalidVolume.NotFound/254. External timer is inactive; timer/service stop command returned 5 because service was not loaded, not 0. Background marker query reports off.

Collection-only repair: original automatic ListObjectsV2 pagination omitted IsTruncated/KeyCount, so the original collector refused before downloads. Adding --no-paginate retrieved the same immutable closed attempt; collector exit 0. No experiment was repeated.

Local host inspection: memory PSI avg10 full=0; IO PSI avg10 full=4.06%. Largest observed processes are control/provider processes. No local heavy execution started or unrelated process stopped.
