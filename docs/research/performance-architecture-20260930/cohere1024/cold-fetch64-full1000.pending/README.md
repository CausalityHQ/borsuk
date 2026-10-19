# HOLD: fetch64 frozen1000 confirmation packet

Prepare on the current host only. No native execution here. Root moves EBS to a fresh host, owns input gates, native/integration, and shutdown. No push or merge.

API: launch-root.sh measure. Before launch root fills launch-binding.json with ROOT_BOUND, the actual fresh instance, host_launch_epoch, launch_before_epoch, measurement_launch_before_epoch, whole_unit_finish_before_epoch, guest_shutdown_epoch and external_shutdown_epoch. The packet rejects the prep instance, checks DMI identity and exact scheduled poweroff, and requires measurement cutoff +4200 seconds before finish. Do not guess IDs or times. Root records the binding hash and actual external shutdown receipt before launch.

Root launches one systemd unit: RuntimeMaxSec=4200, AllowedCPUs=0-7, CPUQuota=800%, MemoryMax=8G, MemorySwapMax=0, TasksMax=1024. Guest shutdown within90 minutes of host launch; external shutdown within100. Preserve the original SSM/unit handle, no autoretry or replacement after interruption. Incomplete cells invalidate the entire paired attempt.

Both arms use qualified runner/reader/source/inputs/scratch/flags from authorities.json and frozeninputs, leaf64/cache0/Q1/Offload2/FP32 shortlist16. A128 versus B64 is the sole intervention. Admission diagnostic8 [0,14,15,31,46,62,78,93] and separate real-S3 canary2 [109,113] run BOTH on the fresh host first; measured cells A1 B1 B2 A2 execute Full1000. Gold for BOTH is cold-source-overlap-full1000-a0002/B1.jsonl, 50434430 bytes, SHA256 69b56a0464d9db152625b32051438fdc644785f6dc18564f68e0980a06e6b8b1. Every full cell requires9902/10000 with all1000 IDs/f64bits/physicalordinals/plans/trace/shortlist/ALL charges equal gold, plus independent f64 oracle.

Full caller7010842 both; model A471434981 B471043301, no diagnostic delta. Prelude adds524288+8*n to caller/model; scheduler/auth/scratch pins stay exact. No rebuild or framework changes.

All11 gates remain: BOTH SOURCEp95 <=.90; BOTH queryp95 <=.90; pooled query wall <=.90; pooledp90 <=1; queryCPU <=1; external wall <=.95; externalCPU <=1.05; GET/bytes <=1. Valid gains survive a strictFAIL. Frozen1000 regression only: no untouched holdout, vendor, generalization, readiness or default claim. Provider cache eviction and download occupancy remain unproven.

Closed screen is VALID_FETCH64_NEGATIVE_ON_FIXED68:674/680 each cell, strictFAIL3/11, wall .7664266774; paired queryp95 .6847862115/.9117608572; SOURCEp95 .6216012532/.9007826516; queryCPU1.0184312026. Preserve the latency/RAM/CPU Pareto: lower modeled RAM391680 bytes, improved latency, regressed queryCPU. closed-screen.json preserves root verification of292 exact outputs/plans/charges and10 checksumHEADs. Earlier leaf40 full1000 favored128 (protocol historical_counterexample); this packet makes no reversal claim.

Budget proof: full4*(600+120)=2880; prelude4*(60+30)=360; native/reader kill grace160; AWS340 =20*(15+2); other460; total4200. AWS wrapper timeout15/kill2/retry1. Exactly8 create-only completed-cell checkpoints + archive + terminal, each checksumHEAD. Full per-cell output64MiB; campaign512MiB. Query bound4020*176 GETs and4020*(67108864+33554432+65664) verified bytes; native retry costs remain in all tails/CPU/transport evidence. No slow-cell exclusion.

TraceON remains BLOCKED at unchanged1MiB diagnostic cap (lower bound1189457), unrun without waiver. Static checks only: CPU1/512MiB/swap0/tasks128/120s. Preserve inherited missing Git blobs; bundle only the new commit delta.
