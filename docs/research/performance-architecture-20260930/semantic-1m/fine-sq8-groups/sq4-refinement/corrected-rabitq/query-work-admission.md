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

## Source repair checkpoint

The same worker subsequently added mandatory `query_auth_operations` and a
full-batch admission before construction. Root independently recomputed its
current formulas from the authenticated 128 row counts:

| Accounting component | Conservative units |
| --- | ---: |
| Corrected candidate query scoring/preparation | 15,733,844,912 |
| Decoded-cosine reference scoring/preparation | 16,649,896,880 |
| Unchanged SQ8 reference scoring | 11,326,600,896 |
| Three-arm subtotal, before authentication | 43,710,342,688 |
| Separate construction admission | 159,001,952,256 |

For `E(D)=D²+128D+7D*bit_length(7D)`, candidate work is
`128E(D)+sum T*(12+ceil(D/2)+8D+bit_length(T))`; decoded-cosine work is
`128E(D)+sum T*(12+9D+bit_length(T))`; unchanged SQ8 work is
`sum T*(12+6D+128)`. Construction is
`2*(8D³+128D²)+2N*E(D)` at N100,000/D768.

These are provisional source accounting bounds, independently reproduced by
integer arithmetic, not a native execution or timing result. The full query
bound also includes admitted authentication and output passes. The new explicit
allowance still needs a final source contract and prospective experiment freeze;
no default increase or paid launch has occurred. Historical `caps.operations`
validation remains separate. Native correctness tests remain unrun.
