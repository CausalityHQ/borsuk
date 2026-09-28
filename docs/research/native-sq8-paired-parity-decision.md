# Native SQ8 creation: GO for encoding and paired offline quality

Frozen first100k, D768 cosine k100. Same physical order, codes, IDs, calibration
and 744 validation queries256–999 for each dataset. No layout refitting or tuning.
The actual public Rust writer produces codes/IDs/low/step identical to the
historical NumPy bodies. Sequential reconstructed f32 norm accumulation changes
90,059 ReLAION and90,208 CoHere records; maximum absolute norm change is
1.847744e-6 in both. The body SHA therefore changes and requires a new authenticated
root before serving. Historical bodies/roots remain immutable.

| Dataset / split | Native returned mean R@100 % / p05 hits | Native flat mean % / p05 | Original returned mean % / p05 | Decision |
|---|---:|---:|---:|---|
| CoHere first100k validation256–999 |99.131720 /98|99.315860 /98|99.131720 /98|GO|
| ReLAION first100k validation256–999 |99.163978 /97|99.568548 /99|99.162634 /97|GO|

Both meet unchanged98% mean/p05>=95/<=0.5pp flat deficit, with at most32
**planned** GETs /16,773,120bytes. This reuses frozen native range plans and the
established sequential-f32 scoring mirror. Codes/calibration determine unit
centroids, and original raw rows/physical IDs determine two-bit nomination;
stored SQ8 norms do not enter those routing inputs. These are offline final
scoring checks, not a regenerated-root or end-to-end serving benchmark.
ReLAION gains one total returned GT hit; CoHere and both flat hit totals are
unchanged. Per-query changes, hashes, configs and raw samples are archived.

Native SQ8 creation on existing Spark took30.77s ReLAION /30.41s CoHere,
2,852KiB peak process RSS each, zero swaps. These are unoptimized debug **writer**
measurements, excluding source normalization/layout fitting, metadata building,
OS cache, serving and cloud cost. Source-only Parquet export used Python to
supply the already-frozen normalized vectors/order; the writer itself streams
one row at a time. The example also rejects wrong order identity/low cap before
output. Early missing Cargo registration/PyArrow failures occurred before their
respective build/scoring and are retained.

## Product decision and next gate

Retain native `build_sq8_source` as the Rust SQ8 creation boundary and the
runnable example. No algorithm/format retuning or paid scale run is authorized
by this result. Native semantic layout fitting, arbitrary IDs, incremental
maintenance/GC, packaging/CI, live AWS/root serving, scale and matched vendor
comparison remain open. Next close native semantic source construction; qualify
its approved-method implementation before the frozen cold HTTP product gate.
