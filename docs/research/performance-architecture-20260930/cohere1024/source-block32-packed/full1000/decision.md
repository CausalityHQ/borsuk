# Full1000 packed SOURCE scorer decision

ADOPT_USEFUL_LIBRARY_INCREMENT. The parent approved the packed independent-row
scorer in candidate 7acbbb82d48078f46c0a163614db39eeee15afc7 against the d014 control.
This evidence supports default library adoption; code integration remains with
the parent. Product goal: NOT_COMPLETE. Matched vendor win: UNPROVEN.

The closed Cohere 1M, D1024, cosine k10 panel ran the same fixed 1000 queries
twice per arm in A1 B1 B2 A2 order. These are repeated measurements, not 2000
independent queries. All four native cells, four strict readers and the runner
exited 0. All four semantic files have SHA-256
17ecb612b4df5991018d334e2ebad78616a04a20f17b167d49464a0e8a937312.
The parent verified exact primitive score bits, result IDs/order/score bits and
ALL logical charges. Each cell returned 9783/10000 hits (97.83% recall).

Candidate/control ratios are CPU 0.9472385896273348, wall 0.958274122960616
and planning 0.8184030920694314: about 5.28%, 4.17% and 18.16% reductions.
Both orders improve CPU and wall. Both improve p90 and p95, although reverse
p95 is nearly flat (B2 214.569157 ms versus A2 214.814009 ms). No arbitrary
20% adoption floor was applied.

| Cell | p90 ms | p95 ms | p99 ms | Peak RSS bytes |
| --- | ---: | ---: | ---: | ---: |
| A1 | 203.771550 | 217.658951 | 261.657969 | 96550912 |
| B1 | 191.358850 | 204.740873 | 240.466682 | 94752768 |
| B2 | 194.127893 | 214.569157 | 263.745736 | 95232000 |
| A2 | 199.059542 | 214.814009 | 262.551094 | 91258880 |

Pooled descriptive control/candidate p90 is 200.245449/192.500856 ms,
p95 217.328068/210.009066 ms, p99 262.551094/258.044750 ms and serial
QPS 6.249580573871469/6.521704410177755. Pooling repeats does not add
independent queries. Per-cell p99 is mixed. RSS does not establish a memory
improvement; the prospective modeled scratch increase is 447 bytes, not RSS.

Each cell has unchanged SOURCE 109872 logical GETs/28967583744 bytes,
SQ8 30041 GETs/10722616576 bytes, and router zero charges, with zero failed
logical GETs. The closed physical parity check is true: each cell records
139913 physical GETs/39690200320 consumed payload bytes, with zero transport
and stream failures. These counters do not establish wire bytes or billed usage.

The runs used Spot m6i.4xlarge i-0f168289eda791b07 in eu-central-1 with
retained EBS vol-025..., native CPU4/cpus0-3, 16 GiB RAM, no swap and pids1024.
SOURCE128/64 MiB, SQ8 32 GETs/32 MiB, query memory 2 GiB,
fetch parallelism64 and source cache off were bound by strict reader headers.
The control ELF is a3678661701d97541c66632bd4b7bb73d5fa0fcd0d392266c523b3b60d9de415;
the candidate ELF is 9a5134dcd9f9e4b6544de501c937a1973c0e6a09b2015af3b7abc764a47deec3.
Before/after artifact hashes match. Correctness, source/release and runtime
gate metadata pass for the candidate; receipt paths and source/config/header
hashes are retained in receipts.json. No native or correctness tests were rerun.

summary.json is a literal copy of the closed summary.v1.json, SHA-256
6cacdda88db3f15ce5f2181eb9825329fee9adae7f240982e713a2884ffcabc4.
This full1000 result supersedes only the original fixed32 decision's pending
default-adoption status. The original decision, summary and receipts remain
unchanged historical records.
