# Prospective three-arm work admission

Source inspection on 2026-10-05 found a deterministic resource mismatch in the
uncommitted corrected-codec diagnostic. This is an implementation/configuration
issue, not a scientific rejection. No native experiment was run.

The authenticated `closed-populations.json` contains 1,013,616 fetched rows for
ReLAION and 1,371,936 for CoHere: 2,385,552 rows over the consumed 128 queries.
At D768 the inspected implementation charges `8D` per row for each of corrected
scoring and decoded-cosine scoring, plus `4D+128` for unchanged SQ8 scoring.
Thus these row charges alone are:

```
2,385,552 * (20 * 768 + 128) = 36,947,429,376
```

This exceeds the inherited 20,000,000,000 query/authentication limit before
preparation, sorting, authentication, and closure charges. These are source
accounting units, not measured CPU instructions or elapsed time.

Required repair: admit the complete authenticated population and all reference
control work before construction; test that insufficient admission refuses
before encoding or large input reads. Report candidate query work and additional
reference-control work separately. Any distinct prospective diagnostic budget
must be explicit in the new method's frozen configuration. Historical limits
and recall, payload GET, and byte gates remain immutable. Repair the admission
and run correctness gates before launching the scientific falsifier.

The same implementation worker received this finding as message
`1791234340807046857-3297541`. Its final source and resource contract are pending.
