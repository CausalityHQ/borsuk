# Provisional ReLAION 1M fresh query panel: identity-first freeze

Frozen 2026-09-29 before selecting IDs or decoding candidate vectors. This is
source/query identity preparation, not ANN quality, an accepted cohort, or a
vendor comparison. The old rejected ReLAION1000 seal remains closed and invalid
for fresh qualification.

## Source and blind selection

- Indexed corpus stays the authenticated original V36 first1M ReLAION D768
  cosine source parquet, SHA256 `2796b579f37afe99ca4aff57e282335a6a79ad30596645957d26326a0560cf86`.
  Current native candidate, source, scorer, root and control identity must remain
  exact at any later quality gate.
- External candidate queries come only from the pinned original Hugging Face
  revision `bfc7465dcf1245bd605d35dcaf5d2177bbc2025a`, ranked physical
  objects16–31 of the V36 source registry SHA256
  `b9a19e2f142fd54983ed1db9f09862f2c5623b6b2105d66e538664f8adda9180`.
  This 16-object window was fixed before looking at candidate IDs. All32
  source objects must match their registered full SHA256 before selection.
- Exclude every feature ID in *all* original rank0–15 physical objects, not
  merely the indexed first1M. The ID-only report SHA256
  `888748ef1125416bfa47c6dc791280e8be1885eb7ea68f3145872985fe5801c6`
  measured 5,724 shared IDs and 3,572,806 distinct candidate IDs after that
  exclusion. Candidate duplicates use the first physical occurrence by source
  rank then row offset. Null/negative IDs invalidate construction.
- Among eligible IDs, choose the 1,000 smallest SHA256 values of
  `b"borsuk-v36-rank16-fresh-v1" + feature_row_id.to_bytes(8,"little")`,
  breaking digest ties by numeric ID. Assign query ordinals in that order.
  Ordinals0–63 are development,64–999 are sealed prospective confirmation.
  Selection reads only the ID column, no embeddings, GT, ANN results or quality.
  Do not replace a selected ID or salvage999 if any later identity check fails;
  reject the **whole** panel before quality access.

## Remaining identity and execution gates

The 35 authenticated original source archives and149 exact Git source trees in
`fresh-history-direct-source-scan.json` reference direct raw ReLAION physical
shards only in the V36 builder/tests, whose frozen population uses rank0–15.
This code scan alone does not prove every historical runtime input or indirect
query transformation. Before unsealing any ANN quality, bind the selected
IDs to their archived source positions and audit all prior ReLAION external and
pseudoquery lineages, including nonversioned attempts. If any selected ID is
found or provenance remains unresolved, stop and retain this panel only as
construction evidence. Do not inspect old sealed query bodies to decide.

After identity PASS, an AWS Spot construction cell may decode only selected
candidate embeddings, authenticate source and query bytes, and seal exact f64
cosine GT10/GT100 against the fixed original1M corpus. Validate source/scorer
and GT parity independently. Keep quality sealed until a separate qualified
native gate. Practical fixed representative target: mean recall@10 >=95%,
report recall@100 separately; cold incoming HTTP p90 <444ms for the published
1M D768 context and offered8 successful QPS, with hydration, physical GET/bytes,
RSS, retries and cost disclosed. The published comparison is contextual until
matched vendor measurements. No architectural tuning or threshold change may
use the prospective IDs, vectors or GT before its outcome is fixed.
