# CoHere FIRST1M fresh64 metadata decision

**Metadata authority complete; root freeze pending.** This slice creates only
the authority and metadata selector. `root_pending: true` propagates to the
provisional locator output. It does not authorize preparation or establish
fresh-panel quality, exhaustive truth, corpus body authentication, or complete
historical freshness. The parent owns integration, final freeze, preparation,
and the paid gate.

The original 135,298-byte `STAGING_COMPLETE.json` is committed at
`84c08fd6bb687fdd8e0b648d6e5afcdd26e48669`, directly above assigned evidence base
`c33fd046ceee627ccf9002b96be765a97a71c433`. Its authenticated body SHA256 is
`0965aa0241199822dfac3410bba4edad5536ac0eb0aaa8ab83c216e8c5749a87`.
[The receipt](../cohere-source-receipt.json) has 461 objects, including 458 train
shards covering exactly 10,000,000 original source ordinals. The authority
records their full URI-sorted roster with keys, byte counts, SHA256, row counts,
and cumulative source intervals. These intervals come from receipt row counts.
No shard body was opened or downloaded by this worker.

[The authority](metadata-authority.json) binds original FIRST1M raw
`6c82a340e3e1b4226640e593efa9c4000c6a5962d4b13063093a1dab689a9005`, SQ8
`b2f2f7dbec79c8646495b4a1500c363c5ccbf51690cf1e936d1264e7ee3839e1`, and
LEu64 order `25672572b8e36eafea6ba856068f33aca5064e3ce02544340f682cd39881ea02`
to the committed source construction and verification proofs. FIRST1M contains
all rows of shards0–44 and the first16,975 rows of shard45. The selector separately
authenticates those proof bodies and checks raw/SQ8/order/root crossbindings.
This authenticates metadata provenance; preparation must authenticate actual
corpus/vector bodies before use.

All intervals below are half-open. The parent's prospective population is train
`[1000000,10000000)`, excluding known consumed `[1000000,1002000)` and
`[1005000,1006000)`. The eligible union is `[1002000,1005000)` plus
`[1006000,10000000)`, totaling **8,997,000** ranks. The ledger also retains earlier
within-index exclusions `[100000,104000)` and `[104000,105000)`, and registered
test ordinals `[0,1000)` in their separate test namespace. The entire old1000
panel is consumed: development ordinals `[0,64)` and confirmation `[64,1000)`.
The old panel's raw, requests, GT100, identity, seal, and both consumption
configurations remain pinned. Its exhaustive GT100 applies only to that panel.
Historical coverage remains explicitly incomplete.

Selection uses **CPython 3.14.4** and exactly:

```python
seed_integer = int.from_bytes(
    hashlib.sha256(b"borsuk-cohere-first1m-fresh64-v1").digest(), "big")
ranks = random.Random(seed_integer).sample(range(8_997_000), 64)
```

The seed SHA256 is
`1224e8dade2fdfab239ea10c6b6d648a368194133e3cac7156c1b2887a704bfd`; its
integer is
`8206844874650668526632870969860184178411035331402446395253641667409609772029`.
Map each rank through the ascending eligible interval union and preserve the
sampled order. There is no sorting, ANN/GT/vector-based selection, retry, or
replacement. Output has exactly64 zero-based source ordinals, shard ordinals,
keys, hashes, byte counts, local row locators, and CoHere column `emb`.
Any detected duplicate or exclusion failure stops the whole fixed panel;
preparation must also stop on any raw or normalized-vector duplicate, without
replacing a selected row.

Canonical JSON uses sorted keys and compact separators for hash pins. The
ordered roster SHA256 is
`7b2607c50a79c1da6fb91ed351c98b75b0a27b628af3458e30382cc4ee327df7`, and the
population geometry SHA256 is
`7788c8228aafea809d1f24d4ebdf98e4a7da905b6ce30c26223102021de1da9f`. The latter
binds candidate interval, consumed intervals, eligible intervals, population
size, and roster hash. Canonical ordered selected-locator SHA256 is
`379b6421d794fb2d0956e7ed787478b019abfd599e0510be157cdcaa53205abf`; ordered
source-ordinal SHA256 is
`f58957a9a7cfb445298a6a6a48c139a5dc8f3df1d9f7714602d4a55141ee321c`.

CLI: `python3 scripts/select_cohere_1m_fresh64.py AUTHORITY SHA256 NEW_OUTPUT`.
The caller supplies the exact authority body SHA256. Metadata identity,
population, seed, rule, Python version, complete roster, source bindings, and
ledger must all validate before output. A same-directory temporary file is
flushed and atomically linked to the requested new path; existing files and
symlinks are never overwritten, and failures leave no partial output.

The runnable check is
`python3 scripts/select_cohere_1m_fresh64.py --self-check`. It uses metadata and
synthetic shard boundaries only. It checks deterministic repetition against an
independently generated golden panel, exact64 uniqueness, population and interval
boundaries, consumed and duplicate rejection, changed/missing authority fields,
raw/SQ8/order crossbindings, missing proof identity, and atomic no-overwrite
behavior. Run it within60 seconds, 128MiB, and one core; no SDK, network, corpus,
vector, truth, native binary, or Rust build is needed. Actual commands and exit
statuses accompany the handoff in
`/tmp/borsuk-cohere-fresh64-metadata-contract.json`.
