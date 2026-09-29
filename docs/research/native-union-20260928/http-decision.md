# Incoming native HTTP: both development envelopes GO

Actual Rust HTTP boundary plus authenticated object-native S3 search completed
512 queries with exact ordered-ID/plan/physical-counter parity to the closed native
serving authority. Both first100k/D768/cosine/k100 corpora, **consumed development
ordinals0–63**, four64-query repetitions/corpus, ABBA nearest control /flat-layout
union /union /nearest. Samebinary/root/source/scorer/caps. Medians of two reps/arm.

| Dataset /split | nearest→union recall@100 | actual quality delta | exhaustive SQ8 | incomingHTTP p90/p95 ms nearest→union | serial observed QPS nearest→union |
|---|---:|---:|---:|---:|---:|
| ReLAION dev0–63 |99.15625%→99.421875%|+0.265625pp /17hits|99.515625%|105.276956/106.590849→106.305615/108.067463|9.435453→9.552674|
| CoHere dev0–63 |99.09375%→99.171875%|+0.078125pp /5hits|99.234375%|104.639611/105.840154→105.364193/106.248342|9.595077→9.564621|

All are verified measurements. Dev p05 nearest→union97→99% ReLAION,98→98% CoHere.
Returned totals6346→6363/6400 R and6342→6347 C, exhaustive6369/6351. Fixedmean98%,
p0595%,flatdeficit<=.5pp,nonregression,32GET/16773120B and250/400ms median tail
limits passed. Union slightly slower at p90/p95; serial QPS mixed direction.
A single serial development cell does not establish a sustained throughput or
speed win. Earlier library-only timings are separate closed cells, not estimates
of HTTP overhead or matched timing controls for this one.

## Timing boundary and topology

Client perf_counter from sending prepared JSON through complete response parsing:
request transport, serverJSON parsing, native routing/nomination/authenticated S3
reads/ranking, response serialization/transport/clientparse. RequestJSON preparation
excluded from latency, included with validation/tracelogging in total serial QPS
loopwall. Samehost **loopback** on Causalityc7g2xlarge/eu-central-1c/CPU0–3,
4Tokio workers; not remote-client WAN/TLS deployment measurement. New nativeprocess
per repetition, residentrouter, no clientSQ8cache/querywarmup, keepalive reused,
S3servercacheuncontrolled. Existing immutable heads read/authenticated/opened at
startup; no publication timing in this cell. Root/generation1/control epoch1 pinned.

| Dataset /arm /rep | p50 ms | p90 ms | p95 ms | p99 ms | serial QPS | native process maxRSS KiB |
|---|---:|---:|---:|---:|---:|---:|
| ReLAION nearest0 |103.115353|105.925980|107.159509|169.429182|9.448321|79268|
| ReLAION union1 |103.770484|106.225966|108.052151|115.608170|9.560406|78192|
| ReLAION union2 |103.951406|106.385265|108.082774|113.943866|9.544943|76780|
| ReLAION nearest3 |102.805464|104.627931|106.022190|172.544464|9.422586|81936|
| CoHere nearest0 |102.757703|105.223224|106.790478|157.696429|9.493009|75908|
| CoHere union1 |103.191583|104.654585|105.284913|113.218891|9.613164|78636|
| CoHere union2 |104.002733|106.073801|107.211771|114.369523|9.516079|78800|
| CoHere nearest3 |102.113301|104.055998|104.889829|113.272459|9.697146|79384|

Per64-query rep Rnearest1615GET/1072206720B versusunion1663/1072281600B;
Cnearest1922GET/1072955520B versusunion1934/samebytes. All512queries obeyed32GET/
16773120B, zero failures. No discarded/trimmed/warmed-up/replaced query. Every
response ordered100 signedIDs equals its frozen native record; independent GT-set
counts equal expected perquery/aggregate/p05. No new exhaustive SQ8 kernel.

Actual HTTP boundary checks every repetition: wrongroot/generation/epoch409,
zero400,unknownfield422,bodycap413,slowbody occupies admission/second request503,
disconnect releases permit/next zero400. No validANN warmup. Wrongepoch startup
rejected before ready on both corpora. Native processes stopped via timeoutchild
and GNUtime retained RSS/exit; completed rep syncedS3 before next. No active compute.

## Current validation and remaining convergence gap

Separate **consumed method-validation256–999 /744queries**, unchanged quality
already verified: ReLAION matchednearest98.850806%→union99.463710%,
+0.612903pp /456hits, exhaustive99.568548%,p0597→98%; CoHere99.059140%→99.215054%,
+0.155914pp /116hits, exhaustive99.315860%,p0598→98%. That split has no measured
HTTP tails/QPS. Do not transfer dev64 timings or call old reference timing matched.

Both-vendor recall/HTTP/concurrent QPS/total lifecycle-dollar comparison missing;
no1M/10M/100M serving/build/pinnedRSS/maintenance/recovery quality result. Native
100M resident arithmetic remains a projection, not admission/RSS. Scale blockers:
flat1M fitter120s KILL and hierarchical-order discovery reachable-frontier KILL;
end those arms, no work-budget/roster weakening/retry. The next engineering slice
must make existing source graph construction support bounded complete discovery
without changing source/scorer or query caps, then a single preregistered paired
falsifier. A source-only connectivity repair within existing graph degree is a
concrete candidate; establish its property and quality before further scale work,
not an architecture/reviewer cycle. Existing successful100kflat union retained.

Prospective ReLAION1000 panel fails novelty atquery903 (prior V189sourcequery).
No new sealed vectors/GT/ANNquality opened. Preservewholepanel; no filtering one
witness and relabeling999. Entire replacement needs identityaudit before quality.
CoHere novelty still uncertified. Vendoraccess dependencies remain: Turbopuffer
credentials absent, S3Vectors create/query permissions unverified. No operator
choice blocks the authorized native construction/identity work.

## Frozen authority and closure

Native session14755 CLOSED0; i-0bf1ce00d49a100a4 **terminated158s**, compute estimate
**$0.0079**, excludesEBS/S3/notinvoice/lifecycle dollars. Controller71.52s,
maxRSS146900KiB; nativeprocess75908–81936KiB. Wholecgroup peak223793152B inclclient/
cache/kernel, actual8GiBmax/4GiBAS/CPU0–3/zeroSwap/OOM0.80GiB encrypted disposable
EBS. Reused verified12.523896MB nativeHTTPbinary SHA
`9202a5358e080454404cef15e2ef1f0605c29b6a5c6f73d3f015b7ed257707ab`;
no build/full gate/reviewer rerun. Its repaired-code full assurance2690/0/26 plus
separateHTTP1/0 remains current, not convergence evidence by itself.

Source archive`400ccf28a4aae978942dcd15f10391323f519bcb4db4450288283e350fe2b34f`,
base0e96983c; terminal
`6e7c26cffeddf53ec5bb2f23ab29995bafcb8f1a1c2f0f5fa318e2bed9f3effe`.
Independent verifier authenticated63artifacts/396sources/393Rust-Cargo, exact
root/ID/range/GTset/counter checks, all512latency reductions/QPS/RSS/probes/limits,
source archive and actualtermination. Closedverification.json is authority.
Product goal ACTIVE, BOTH vendors mandatory, no production/default freeze.
