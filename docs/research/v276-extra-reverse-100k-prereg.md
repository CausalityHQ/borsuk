# V276 one-shot extra reverse-edge falsifier

Status: preregistered before candidate measurement. V272's authenticated 10M
CoHere graph had median in-degree 7 among 701 missed GT rows versus 99 among
hit GT rows. V275 tried to repair incoming connectivity without more graph
bytes, but changed only 437 edges across 357/100,000 source rows and did not
recover any misses. The no-extra-slot restriction is the identified blocker.

## Fixed candidate and inputs

Baseline is V271's authenticated first100k root
`440beefd321dfeeae25ba6277a2e2f1f938c389a0c4a9007177f4bacc4b1d6e3`.
Candidate builds from the same CoHere-large-10M canonical train rows0–99,999,
D768 cosine, with M32/M0=64, efConstruction128, worker cap8, same PQ and FP16.
After the existing diverse build and reachability repair, process rows with
in-degree <16 in increasing original in-degree order. For each, consider its
16 nearest geometric outgoing base neighbors. Append missing reverse edges
until its in-degree reaches16, subject to at most16 **extra** edges per source
row and total source out-degree <96. No baseline edge is displaced. This
spends memory only for incoming coverage; there is one fixed cap and no sweep.
The graph's authenticated binary layout is unchanged and its root SHA changes.

Source first100k raw FP32 SHA-256:
`0f3631d71c105e5ea3d701c96033b362c2f84bd43002a9c8a5c70040801be06e`;
ID-plus-vector source identity:
`d638878523cfdd349cb28e214d59010709d6451f3eeca2a70da88c0c9d9ea753`.
Fresh excluded queries are train rows103,000–103,999, distinct from prior
panels; query raw SHA-256:
`81d455e2b251f6244e5e6092b0880fc7014e5cb88c93c29d8f57685f2d51056b`.
Truth is FAISS `IndexFlatIP` on FP32 unit-normalized source/query vectors,
k100. Authenticate baseline against V271 terminal and candidate by its new
trusted root before serving.

## Measurement and decision

One `causality` c7i.4xlarge Spot host in eu-central-1, two-hour hard stop,
one terminal attempt. Build candidate once. Search each arm in a separate
process after one warmup, same 1,000 queries sequentially, one worker, zero
query-time GETs. Primary diagnostic widths are PQ ef256/shortlist256/exact
ef128; record default PQ4096/shortlist4096/exact ef2048 separately. Record
per-query IDs/visits/latency, p50/p90/p95/p99 from the same raw samples,
recall, p05 hits, graph in-degree and byte growth, subset of baseline edges,
reachability, build/search RSS and time, instance/quote/cost and all SHA-256
identities. Upload terminal artifacts before terminating Spot; discard an
interrupted cell.

Inconclusive if the baseline has <100 GT100 misses among rows whose baseline
graph in-degree is ≤8. Otherwise pass only if candidate misses in that fixed
subgroup fall ≥30%, total diagnostic misses do not rise, p05 hits do not fall,
mean visits and p95/p99 are each ≤120% of paired baseline, search RSS is
≤115% baseline, and the all-row share with in-degree≤8 falls ≥50%. Every
baseline edge must remain, max out-degree≤96, graph bytes≤115% baseline,
min in-degree≥4, full reachability and authenticated open/search must hold.
Candidate build time/RSS must be ≤150% of V271's same-class 100k receipt
(201.80 s, 840,720 KiB). At default widths, candidate hits may fall by at
most10/100,000 and p95/p99 must be ≤120% baseline. Failure rejects this
format; a pass permits one fresh 1M gate, not 10M directly. These in-process
measurements are not HTTP product latency or vendor evidence.
