# Prospective 1M resource inventory — not a frozen envelope

Geometry N=1,000,000,D=1024,Q=1000,k=10. Original f32 source, normalized source, SQ8, canonical and packed records have separate identities. Do not delete original vectors to fund serving measurements or reuse 100k truth.

Known principal file-size terms (bytes): original corpus 4,096,000,000; normalized corpus 4,096,000,000; canonical 4,104,000,000; SQ8 1,036,000,000; packed records 264,000,000; physical order 8,000,000; queries 4,096,000; truth 80,000. Sum 13,608,176,000 before manifests, metadata, application IDs, publication copies, temporary files, source shards and retained generation pins.

Publisher-declared eleven shard bodies add 2,382,253,857 bytes, subject to actual whole-body authentication. Principal subtotal with shards is 15,990,429,857. Worst encoded IDs, ID uniqueness state and serialized ID sidecars need separate explicit models; do not substitute an average ID length. Build-time RAM and disk models have different scope.

Each phase must declare which previous files remain live, whether publication copies or links, whether a temporary output duplicates its final body, caller-owned resident data, and old/new generation ownership. Account for staging, sync and failure paths too. An estimated 32GiB scratch limit is not admitted until this complete ledger fits with a reserve and native evidence.

For truth, 32 queries entail 32,768,000,000 coordinate products; 1000 queries entail 1,024,000,000,000. This is arithmetic, not a runtime estimate. Pin an explicit work/deadline envelope from a bounded native measurement before the full pass. Streaming blocks must include decoder, vector block, query, top-k and ID states in peak admission.

Serving profile payload model approximately 506,729,928 bytes is not process RSS. Publication/maintenance coexistence, caller/query buffers, fetched bodies and old-generation pins require independent models and measurements. No 1M cold latency, recall, QPS, memory, cost or maintenance qualification is established by this inventory.

Next decision: after current library gate closure, implement authenticated scale geometry and bounded streaming tools, qualify exact source remotely, then perform actual-input admission and a small recall/resource falsifier. Freeze the measured resource envelope only afterward; no new paid launch is authorized here.
