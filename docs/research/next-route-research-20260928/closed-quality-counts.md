# Closed quality counts: coverage remains the first failing layer

This is post-hoc reconciliation of completed, consumed offline panels, not a new query run or a fresh validation result. Inputs and SHA identities are in closed-quality-counts.json; reproduce with python3 closed_quality_counts.py (stdlib only, no fit/query/score execution).

ReLAION first100k D768 cosine k100, validation256–999 (744queries):623queries fetched all100GT. Returned below paired native-flatSQ8 on105queries, equal on638, above on1. All105 deficits occur among the121queries whose fetched pages miss GT; none occurs when all100GT were fetched. There are577GT hits absent from fetched pages and278fetchedGT hits absent from returned top100, versus321missing under paired flatSQ8:577+278-321=534net lost hits, or0.717742pp mean deficit.

The278fetched-but-unreturned count is a descriptive difference; it is not an additive estimate of the flat quantization floor, and the577missing count still merges candidate discovery and physical page selection. These counts support diagnosing coverage first; they do not authorize changing nomination from GT, choosing a candidate with consumed validation, removing the ranking plane, or declaring a quality win. Development counts for both corpora are retained in the same JSON. No cold latency, QPS, physical requests, RSS or lifecycle cost was measured here.
