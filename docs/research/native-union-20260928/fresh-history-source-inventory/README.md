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
  The ID-only follow-up `../fresh-relaion-physical-id-overlap.json` then
  full-SHA-verified all32 pinned objects and decoded only `feature_row_id`.
  Original rank0–15 has3,583,054 physical rows/3,579,759 unique IDs; candidate
  rank16–31 has3,581,781 physical rows/3,578,530 unique IDs. **5,724 IDs
  overlap**. The whole candidate bank is therefore ineligible as a fresh
  panel. Exclude shared IDs before any frozen query selection; historical
  query lineage still needs closure. No cohort was selected or quality measured.
- `../fresh-history-direct-source-scan.json` authenticates35 original source
  archives (including both V126 attempts, V114–V116 and V130–V155) and reuses
  149 exact Git archive matches. Within code files under `scripts/` and
  `crates/`, literal raw ReLAION source references occur only in the V36
  builder/tests. This narrows direct-import risk; indirect runtime inputs and
  query transformations still require separate lineage checks.

`../fresh-rank16-selection-decision.md` records a subsequently preregistered,
quality-blind 1,000-ID **provisional** selection. Original-bank exclusion and
all selected physical locators passed an independent ID-only check. Historical
selected-ID query lineage remained open at that checkpoint; no vector/GT seal,
ANN quality or serving result existed for that panel.

Selected-ID follow-up: `../fresh-nonversioned-direct-source-scan.json` verifies
33 more terminal-bound original source archives; direct ReLAION raw references
remain confined to V36's builder/tests. `../fresh-native-union-runtime-inputs.json`
authenticates50 CLOSED native-union terminal/reservation pairs and binds23
exact current/historical Git configs; the one unmatched historical fresh config
has a terminal-authenticated source-mismatch decision with no seal. A separate
SHA check of701 distinct closed terminal JSON bodies found zero direct candidate
shard path/name/SHA references. The combined selected-ID inference and its
limits are stated in `../fresh-rank16-identity-decision.md`. This permits
sealed source/GT construction; it is not an ANN quality result or a claim that
every historical query artifact has been individually decoded.
