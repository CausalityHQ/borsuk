# Independent source-probe count audit

Prospective arithmetic only; no native source probe has executed. Fixed geometry: two panels, N=100000, D=768, cohort4096, anchors64, 16 centers and256 bins. These are component counts, not a complete operations admission or elapsed-time estimate.

| Component | Count |
|---|---:|
| Three SQ8 source passes, two panels, 780 bytes/row | 468000000 bytes |
| Source coordinate visits across those passes | 460800000 |
| Worst DP predecessor candidates per axis | 496176 |
| Worst DP predecessor candidates across both panels | 762126336 |
| Nearest16-center comparisons for cohort encoding | 100663296 |
| Same comparisons if all source rows were encoded | 2457600000 |
| Anchor/nonself-cohort coordinate terms per scorer | 402554880 |
| Two residual cohort bodies, 396 bytes/row | 3244032 bytes |
| Two native SQ8 cohort bodies, 780 bytes/row | 6389760 bytes |

DP count is sum over t=1..16 of (257-t)*(258-t)/2. Scoring terms are 2*64*(4096-1)*768. Source reads are 2*3*100000*780. Cohort encoding is 2*4096*768*16; it does not require encoding discarded source rows.

Before any corresponding body read, the implementation must admit the complete aggregate: initial authentication and all exact-EOF rechecks, PQ/order/group bodies, predictor reconstruction, binning and DP interval costs, histogram hashing, query normalization and both numerical scorers, score sorting/serialization, output authentication and terminal closure. Actual container capacities and concurrent lifetimes remain separately charged. None of the component counts above proves the complete attempt fits20B operations,1GiB RAM,64MiB output or600 seconds.

Full-source input identities and cohort residual output identities are distinct. The cohort restriction belongs only to this no-request/no-GT mechanism probe; a later scientific arm must retain every fetched row, including incidental rows.
