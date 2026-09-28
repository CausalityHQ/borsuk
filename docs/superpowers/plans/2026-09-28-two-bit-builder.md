# Source-only generation metadata builder

Use existing source codec, SQ8 centroid builder, graph builder and hashing
reader. Accept authenticated ordinal raw source, physical-order SQ8, approved
calibration and remote SQ8 key/ETag. Preserve physical layout/codec arithmetic;
no query or GT inputs. This replaces manual metadata/root assembly, not frozen
source layout fitting or SQ8 creation. No new paid/full dataset run.

- [x] Focused red: builder output equals the manually sealed eight-file fixture;
  no overwrite, tiny-budget/no-publication, singleton open/plan.
- [x] Admit source/graph/buffer payload before allocations; stream page digests
  and SQ8 hash, verify bytes used for centroids; root manifest written last.
- [x] Reuse graph serialization, including a valid single-node adjacency for
  tiny indexes. Existing multi-node graph arithmetic unchanged.
- [x] Focused check, source/test receipt, docs and coherent push to main.

Missing API red59483 observed. First integration88489 passed eight-file
parity but failed singleton reload at the graph decoder. Fixed empty-base
neighbors only for node_count1; final44671 exit0,1test/0.10s. Reused hashing
reader and existing graph/centroid/source builders. Zero SQ8 cosine norm is
rejected at the shared source guard. No full-data/paid/cloud run or full suite.
Receipt: docs/research/native-two-bit-builder-check.json.
