# Exact SQ8 scorer decision

Native source 9d540e527f3a6e7ce22009302fa1187498476d3c is correctness/compiler qualified on the original terminated instance. Seven serial native gates passed, including debug/release scorer tests, returned-SQ8 regression, release binaries, one-CPU kernel probe, workspace correctness/suspicious Clippy and actual unshimmed workspace test compilation. Full-workspace tests were compiled, not executed.

| Closed synthetic kernel, D1024 | Old scalar median | Blocked median | Ratio |
| --- | ---: | ---: | ---: |
| 257 rows | 177.151 us | 95.524 us | 0.539 |
| 16,192 rows | 10.913126 ms | 6.263345 ms | 0.574 |

All 18 shapes preserved exact IDs, ordinals and score bits against the independent scalar oracle. Portable x86_64 build; AVX2 compile-time flag false. Timings include the scorer allocation path and ranking input construction, not object fetch/authentication or complete ANN queries. Candidate adds a validated temporary roster; matched end-to-end RAM must be measured.

Decision: integrate the native scorer and require one matched retained Cohere100k/D1024/cosine/k10 replay before an end-to-end benefit claim. Both arms must use one newly authenticated transport envelope, identical unchanged fitted/code/source payloads and query/truth bytes. Existing local baseline recall97.23%, p9582.411ms is historical comparison evidence, not a newly measured candidate result. Physical S3 Vectors/Turbopuffer comparison remains pending the agreed single-dataset methodology. No projected runtime or vendor win is admitted.
