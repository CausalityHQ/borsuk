# On-Demand Cargo pilot: infrastructure GO

Original controller session16182 completed0. One c7i.2xlarge On-Demand instance `i-0bc4f4139a6d42c01` terminated and waited before collection. All17 terminal artifact bodies authenticate; exact399 native source before/after remains addf62. The one requested fragmented-source-cover test actually executed and passed. No ANN query, quality measurement, full suite or automatic retry occurred.

| Measurement | Result | Scope |
|---|---:|---|
| Frozen source → authenticated result |474.813s|Measured root/worker timestamp intervals; clocks separate|
| Controller start → authenticated result |395.504s|Measured local clock|
| Cargo narrow test/build |264.333s|Measured worker clock, cold registry/target|
| Boot → Cargo start |68.603s|Measured worker clock|
| Source download |7.746s|Measured worker clock|
| Cache mount/preparation |4.748s|Measured worker clock|
| Kernel memory peak |6,308,208,640B|Measured service cgroup, under8GiB|
| Swap/OOM |0/0|Measured cgroup|
| Max sampled host/cgroup PSI some/full avg10 |0.00%|266 timed samples; not a continuous zero guarantee|
| Compute estimate |$0.0431|380.593s request→termination at verified$0.4074/h; NOT billing|
| Elapsed root/cache storage estimate |$0.00150|Declared gp3$0.0952/GiB-month/30days; excludes S3/transfer/tax|
| Maximum80GiB24h cache estimate |$0.2539|Projection, NOT spent bill|

The local same-source/same-test attempt was pressure-stopped before test execution. Historical Spot1885s qualified a different source and five-stage command sequence. Neither provides a successful matched timing comparison: no speedup ratio or On-Demand-vs-Spot superiority claim is valid.

**Decision:** use remote bounded build resources rather than the pressured shared host. This pilot establishes a usable cold build lane at small observed rate-based compute cost; warm cache value remains unmeasured. Retain only the encrypted detached cache `vol-0b559583b4b92951c` until the preregistered expiry1791070064959738265ns. Root-owned timer `borsuk-cargo-pilot-cache-expiry-20261002.timer` verifies exact ownership and absence of attachments before deletion. No idle EC2 retained. Any reuse needs a separate prospective source/command/config/cost reservation; no automatic warm rerun.

Next is the architecture decision requested by the operator, reusing prior Fable/Sol evidence and closed recall/CPU/stage receipts. SOURCE32 candidate remains source-only/unverified off main. No new format rewrite or paid ANN launch is authorized by this infrastructure GO.
