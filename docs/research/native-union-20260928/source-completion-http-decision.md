# Bounded source completion: verified BOTH100k development GO

ReLAION and CoHere FIRST100k, D768 cosine k100, consumed external development0–63.
Paired control/candidate use identical v4 root, all nine source components, SQ8 scorer,
GT, graph discovery traces and fixed work/GET/byte gates. No fresh qualification.

|Dataset|Control recall@100|Candidate|Actual delta|Flat SQ8|Control p90/p95 ms; serial QPS|Candidate p90/p95 ms; serial QPS|
|---|---|---|---|---|---|---|
|ReLAION|6366/6400 = 99.468750%|6369 = 99.515625%|+3 hits / +0.046875 pp|6369 = 99.515625%|111.997/114.190; 9.17254|117.485/125.202; 8.81918|
|CoHere|6349 = 99.203125%|6351 = 99.234375%|+2 hits / +0.031250 pp|6351 = 99.234375%|107.288/109.490; 9.40491|109.296/110.768; 9.24361|

All table values verified CLOSED. p05 R99%, C98% both arms. HTTP median two
repetitions/arm, ABBA64 each, serial loopback keepalive, fresh server/resident
router; S3 server cache uncontrolled. Candidate slower; no vendor win.
Candidate source-row evaluations R4,666,752/C3,583,232 over64 queries, versus
control3,062,016/2,873,792; each query <=81,408. Actual candidate dataGETs
R1712/C1983 and bytes1,073,105,280/1,072,880,640 per64; no failedGETs.
Candidate process max RSS R80,592–83,956 KiB/C78,268–80,568 KiB.
ReLAION candidate walk/roster/nomination/physical/final losses0/0/0/0/31;
CoHere0/0/3/0/46; flat-present final ranking loss0 both. Discovery unchanged.

130 artifacts,395 native files independently authenticated; ordered native and
HTTP IDs, root/generation/epoch, boundaries, GETs/bytes, cancellation and stops
verified. Original15490 exit0; independent51775 exposed report-only variable
shadowing after HTTP references. Fixed source-work reduction to explicitly use
paired control/candidate plans; independent95459 exit0. No measurement edits,
cloud rerun or Rust changes. Exact discovery parity independently asserted.
Terminal bb027f633bce5293ba09b7568506b6f7d53cdb8a85480fcd98669644a8d7bbcd.
i-026727531d5b42289 actually terminated426s; compute $0.0212 ESTIMATE excludes
EBS/S3 and is not an invoice or lifecycle cost. Large immutable bodies S3-only.

Next: fixed same-root ReLAION FIRST1M consumeddev0–63, control6288/6400,
flat6363. Candidate requires >=6331 hits,p05>=95%, nonregression and unchanged
source/scorer/discovery/work. Scientific KILL ends arm before HTTP/CoHere1M.
GO proceeds immediately to native and incoming HTTP in that same bounded job.
Reuse2696-pass authority; controller-only parity assertion requires no full gate.
Fresh identity,1M/10M/100M scale, maintenance/pins/recovery/lifecycle and BOTH
matched vendor comparisons remain unqualified. No operator decision needed.
