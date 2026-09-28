# Narrow assurance fixture repairs

Original full gate:1630 passed,57 failures,6 ignored; full names/details under
../callable-gc-review-20260928/workspace-failures.txt. Five failing checks now
pass in the frozen causality Spot terminal, sourcec57a294917cd3d4bbf4d4a89e5b6f219f2b57f0a2bb473785c44a9572e22a2a7.
Original instancei-0216ea425de3b87f1 terminated; compute estimate$0.0193, excludes
EBS/S3 and not invoice. No production behavior/default/layout/benchmark changed.

| Root cause | Fixture correction | Passed checks |
|---|---|---|
| Current schema has native_bounded_ann_ref_json, missing nullable array | Match current27-column schema | 2 format validation checks |
| Negative backend literal equals actual AWS x86 backend | Unsupported backend string, preserve worker-count check | 1 authority test |
| Capacity-aware owner selection no longer permits old1/3 occupancy | Source49/50 yields actual cross-supercell owners1,0,1,0 at bounded2/2; assert both | 1 global-assignment test |
| Current one-page response cap requires separate GETs | Assert one physical GET per selected page; retain output-page cap | 1 native fetch test |

Remaining52 original failures have NOT been re-run or certified. Many request
retired default authorities: d94711a0 intentionally restricts finish_bulk_load to
squared-EuclideanD>=64 and publishes native_bounded_ann_ref, not oldnative_ann_ref
or global_cell_card_ann_ref. Existing replacement bounded tests passed in full
gate. Do not restore unqualified retired routes just to satisfy stale expectations.
Security/admission/corruption coverage requires an explicit current-contract
fixture before retiring a stale test; actual paged-manifest error still needs
independent diagnosis. No clean-base full57-failure measurement yet.

Operator Ask operator-immediate-1790599542963968292-808897 was replied exactly
once via devbox-tell --reply-to; successful queue receipt observed. Never repeat.
Active full goal unchanged. These five fixture repairs are delivered separately; delayed DELETE production repair remains under qualification.
