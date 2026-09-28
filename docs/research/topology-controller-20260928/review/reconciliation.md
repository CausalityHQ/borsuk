# Narrow helper review reconciliation

Opus consultation `cd67ed69c7a54cf0` is complete. The unedited result is retained
in result.json. This was a bounded code review, not a replacement architecture
critique or an approval of corpus launch. No review executed tests or queries.

## Fixes selected before another corpus result

- Compare sorted JSON representations after removing only the two graph fields.
  Python dict equality conflates signed zero and numeric/bool representations;
  sorted JSON distinguishes those representations for the canonical Rust roots.
- Require distinct lowercase 64-hex root hashes in complete and partial rosters.
- Require the exact Rust candidate-geometry error with exit1, rather than a
  substring. Extra stderr remains an invalid runtime/control result.
- Reject duplicate, fractional or out-of-range GT/result IDs and fractional
  nominated pages; reject noninteger resource counts, mismatched sample ordinals
  and returned hits exceeding physically fetched hits.
- Delete the redundant 6272 returned-hit floor; the registered 6346/6342 floors
  already imply mean recall >=98 percent.

All twelve concrete negative cases are added to the same standalone check.
Observe their failures on AWS before changing the helpers, then verify the
complete helper check on one subsequent short green worker. No duplicate Cargo
gate or second review is needed for these fixes.

## Gate ruling

Keep no actual paired-control mean returned recall regression. The existing
falsifier-protocol-draft.md explicitly registers it, and controller-next.md item6
is an abbreviated list rather than a change in protocol. The counterexample
with control6360 violates the mandatory frozen control6346 reproduction first;
it cannot authorize this scientific comparison. The retained condition makes
the requirement explicit and harmless under the mandatory identity preflight.
Cost if this ruling is wrong: a candidate is stopped more strictly, never
accepted with worse measured control recall. No gate is weakened or retuned.

## Wrapper obligations retained

The caller must hash-check and normally load both current-v3 roots, compare the
historical fingerprint, assert exact100k/D768/page geometry and 159 candidate
pages, validate exactly100 unique GT IDs, derive GETs/bytes from authenticated
ranges and require planned_bytes equality. The stage-set helper also accepts
smaller synthetic ID panels for its check; it does not authenticate a corpus.
The wrapper must build ordinal-aligned samples from the validated roster,
compare every prefix control plan against preflight before kill classification,
and reject an unparsable final output line. None of these duties is discharged
by synthetic helper success.

Nested duplicate JSON fields do not describe normal Rust-emitted roots and
remain outside this digest helper's claim; normal root loading and a recorded
body SHA precede use. There is no new compatibility reader or published old
root. Physical HTTP, Rust serving scoring and vendor quality/cost remain open.
