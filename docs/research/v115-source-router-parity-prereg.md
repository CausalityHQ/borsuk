# V115 source-only router parity cell

Status: preregistered before launch. This is a ReLAION-1M **development-1000**
parity cell; it is not a recall or live S3 performance measurement.

The frozen V114 paired control is 99,234/100,000 returned GT100 hits
(99.234% Recall@100), versus its same-run V109 capped baseline of
98,803/100,000 (98.803%). That is historical evidence at source commit
`87881c71e048d557c5c1285ffa0ac1d8c381f764`, not an inherited V115
quality result. The V115 route may use that quality evidence only if its
source-trained planes match V77 bitwise and its 1,000 nominee **sets** match
the frozen V114 requests. Ordered mismatches are recorded. Any plane or set
failure stops promotion and requires diagnosis and a fresh paired quality cell.

One Causality `c7i.12xlarge` Spot instance builds the source-only router from
the pinned V36 ReLAION source and V63 layout. It seals the five sectioned
planes and generation manifest before downloading any development query,
ground truth or V114 request artifact. It then independently regenerates the
V77 manifest, runs Rust nomination for the frozen 1,000 requests, and validates
source-plane and roster parity. No threshold or training rule depends on the
dataset name or GT. The cell reports build phase time/RSS as observations;
these are not serving latency, throughput or steady placement RAM.

The attempt has one immutable S3 reservation and terminal, a 7,200-second
wall limit, Spot interruption monitoring, and per-artifact SHA-256/size. An
interrupted or failed attempt is discarded and may be restarted only under a
fresh attempt prefix. Monitor incomplete work by terminal and infrastructure
health only. Terminate the instance immediately after terminal. The source
archive and output prefix are bound to the committed SHA before launch.

The 2026-09-23 EC2 API Spot observations were $0.9821/h in `eu-central-1c`
at 22:00 UTC, $1.0053/h in `1b` at 20:00, and $1.0251/h in `1a` at 20:00.
These are variable prices, not a cost measurement for this cell. The current
research cap is one attempt of at most two hours, with no On-Demand fallback.

Passing this gate authorizes implementing the Rust returned scorer and S3
transport in the live V115 plan. Untouched validation, another corpus, actual
RAM/NVMe resource measurements, and 10M/100M scale remain separate gates; the
memory ceiling may increase with recall and corpus size along the measured
frontier, with no vector-count knee.
