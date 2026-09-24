# V139 centroid-page threshold preflight closeout

**Decision: do not launch the preregistered threshold policy on Spot.** An
independent read-only review executed the exact V139 evaluator on completed,
authenticated historical inputs before launch. This was an exploratory
**local** dry run, contrary to the intended remote measurement placement. No
V139 Spot instance was launched. The result is useful for an early stop and
the next design, but is not claim-eligible returned recall, live S3 latency,
or a frozen remote benchmark. Its raw outputs remain in
`/tmp/v139-critic/`; the D96 and D768 raw SHA-256 values are
`22be98fe478f165ad85c7234376dc849f731b71ab27daea4baabc5015ce736b8`
and `f1cf3bdff58e53e1b54944b0cb5975e6f8d4a53cd54eee3b13d5e40975eafd94`.
I independently recounted all 2,000 raw rows, every GET and byte charge,
caps and summaries. The reviewer authenticated the frozen input hashes and
verified that V116 primary values index physical SQ8 rows. There is no active
local V139 calculation or remote worker.

| Used cohort and split | Fits 16 MiB and 32 GETs | Mean minimum bytes/query | p50 / p95 bytes/query | Decision |
| --- | ---: | ---: | ---: | --- |
| deep-image-96-angular random100k train subset, publication-test 9000–9999 | 1,000/1,000 | 1,272,409.344 | 874,368 / 3,777,408 | Passes the 4,714,063 B screen |
| ReLAION-1M, validation-1000 | 636/1,000 | 72,544,992 | 10,183,680 / 397,612,800 | Rejects the 5% cap-violation screen |

The D96 threshold's physical GT coverage, counted **after** range selection
against authenticated sealed V122 truth and layout, was **99,297/100,000**
positions. The same-layout fixed capped control covered **98,827/100,000**;
the stronger V135 full-object candidate returned **99,942/100,000** exact
source neighbors. V139's 99,297 is only a physical ceiling, so its returned
quality can be lower. It does not meet a 99.5% physical ceiling. The used
publication-test split cannot provide a fresh validation claim.

The root cause is a mismatch between a row-distance witness threshold and
distances to physical unit centroids. The centroid score alone misses some
units containing the 100 primary rows; the forced primary-page union repairs
that local inconsistency. On D768, the score has a large admission tail: 364
of 1,000 queries exceed the 16-MiB cap because threshold admission has no
budget. A failure at this threshold does **not** prove centroid ranking is
useless. The next method must rank pages, preserve the best primary pages,
and enforce bytes and GETs during selection under one policy for both corpora.
The selected page count and resident summary budget should be explicit
functions of requested recall and corpus geometry, with no corpus-name or
vector-count branch. Measure the new policy on Spot, then test returned
quality and live S3 only if the source-only geometry passes.

The V139 launcher review also found missing artifact authentication and an
unbounded setup path. Commit `67f5ae3c` added terminal artifact checks,
exception cleanup, a whole-instance shutdown timer, and evaluator log
preservation. A separate claim that botocore double-encodes the already
base64-encoded user data was refuted by inspecting the installed serializer:
it transmits the supplied string unchanged; V138 had completed through the
same path. Those hardening changes remain available for the next remote gate.
