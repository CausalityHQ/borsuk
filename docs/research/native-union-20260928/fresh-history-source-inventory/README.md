# Historical query metadata inventory, incomplete exclusion audit

2026-09-29, metadata/source reads only. No query embeddings, GT, measurement
CSV or sealed corpus bodies opened. No ANN, numerical kernels or builds here.

Root discovery found341 research prefixes,279 versioned. Collection authenticated
490 versioned terminal metadata bodies by SHA256 and identified101 terminal
records with named query/preparation/population metadata locators (87 prefixes).
This locates further lineage work; it does NOT certify complete history/novelty.
Nonversioned campaigns, older producer dependencies and source-ID mappings remain.

Nine selected CLOSED campaign archives independently matched their terminal
source hashes. Saved code-file hashes and relevant source lines for V151,
V152(two revisions),V154(failed and complete),V272,V278,V279,V281. Conservatively
retain failed-terminal producer lineage too. Initial older archive lookup used
wrong `sources/<sha>.tar.gz` layout; S3 metadata located original
`source/<sha>/source.tar.gz`, then all nine archives authenticated. Locator files
remain discovery-only; separate per-archive records carry authenticated hashes.

NEW CLOSED V278 exclusion proof: CoHere train rows1001000–1001999,1000 queries,
prepare SHA952d365015ab37cd285a89132fa187fb74b1d6faacd9054e7bd4278055b6df2a,
query identity6e3505fdfc9d6a6c101cba2b7d4c16d5c07eee6ec704fa836385a810e8ca17d4.
Exclude this whole panel in addition to V2711000000–1000999 and V277104000–104999.
Archive-bound V272 uses registered CoHere test.parquet, not a new train panel;
V279 ReLAION requests derive from V116 validation. Bind their actual metadata
identities and all earlier source dependencies before selecting a fresh panel.

`terminal-collection.py` and `source-collection.py` preserve the bounded
collection commands. Source selection is fixed to nine archived experiments;
no new experiment/query selection or product quality claim. Full prior-query
audit remains FALSE, novel cohort not selected, old rejected seal stays closed.

Further CLOSED lineage, 2026-09-29:

- `v114-v116-query-producers.json` authenticates five original source archives
  using same-attempt reservations plus matching terminal schema/source commit.
  Those old terminals do not contain the archive SHA themselves. Includes failed
  attempts conservatively. Query bodies and quality outputs were not read.
- `../fresh-relaion-validation-family.json` binds the original V36 validation1000
  through V116 requests to V154/V198/V279. Different encoded query hashes belong
  to the same prior query identities; exclude the entire bank.
- `../fresh-cohere-stress-query-families.json` authenticates V273–V276 byte ranges,
  archived mandatory input SHA checks, CLOSED `fresh_queries.raw: OK` receipts
  and preparation metadata. V274–V276 add previously omitted train-order raw rows
  101000–103999. With V273/V277 exclude100000–104999 inclusive. The22-byte check
  receipt's own SHA is not the vector SHA. Preparation metadata contains only
  truth/version fields; expected vector SHA comes from the archived check.
- `../fresh-history-source-bindings.json` reauthenticates all697 inventoried
  terminal bodies and preserves nested archive/input identities missed by the
  first collector:371 direct SHA,28 nested identity,298 without terminal archive
  SHA. This metadata normalization does not authenticate each archive or prove
  complete query-bank closure.

Remaining gate: authenticate remaining producer banks/source-ID mappings, or
prove a conservative new source bank excludes every historical query bank.
No fresh cohort selected, no query/GT/sealed bodies opened, no ANN/numerical
work, native build/test, paid job or full assurance rerun occurred in this slice.

Further source proof, 2026-09-29:

- `../fresh-history-archive-reconstruction.json` checks356 terminal-bound
  commit/archive pairs against exact `git archive` bytes and its deterministic
  gzip form. 149 pairs covering158 terminal references match exactly. The207
  mismatches remain unverified by this method; a commit alone does not prove
  archive bytes. This proves source content only, not executed query inputs.
- `v130-v155-collection-status.json` indexes28 selected original CLOSED
  terminal source archives, all independently SHA-verified against their
  terminal identities. Five prior proofs were reused and23 original archives
  were newly collected. These attempts include Deep Image and ReLAION sources,
  and failed attempts remain in the exclusion inventory.
- `v85-metadata-locators.json` and `../fresh-relaion-development-format-family.json`
  bind the older V114 100k formatted query SHA to the original V36 development
  parquet via the archived V85 converter and CLOSED input-check receipt. V85's
  terminal lacks an archive SHA; its source binding is same-attempt plus matching
  Git code and the closed input checks, not terminal-SHA binding.
- `../fresh-relaion-physical-bank-envelope.json` verifies all16 original V36
  ranked source objects against the authenticated population authority and
  records ranked source objects16–31 as a *candidate physical bank only*.
  The 5,483,342,562 encoded bytes have not been read here. Feature-ID
  disjointness and historical query identity remain unverified; no cohort was
  selected or quality measured.
