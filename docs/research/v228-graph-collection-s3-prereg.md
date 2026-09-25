# V228 graph collection S3 gate

One Causality c7i.4xlarge Spot attempt on `eu-central-1c`, after the
V227 1M source-format attempt has closed and its instance is terminated.
Source archive, instance identity, Spot quote, terminal status and artifact
hashes are recorded under an attempt-specific immutable S3 prefix. An
interrupted cell is discarded and restarted under a new attempt; the worker
uploads a terminal marker and shuts down immediately.

This is a four-row storage and concurrency correctness gate, not a
performance or recall benchmark. The deterministic base contains public
ID 42. A collection head first pins the unmodified graph at revision 1.
Revision 2 atomically deletes 42 and puts 99 with its vector in an
authenticated mutation snapshot. A held reader still returns 42, while a
new reader returns 99. A newly staged generation then compacts that exact
four-row live state and revision 3 switches the collection head to its
root with an empty mutation snapshot. Its query IDs must equal revision
2's query IDs; the old reader must remain unchanged. A stale revision-1
CAS must fail and leave revision 3 current.

Cold graph hydration must use five blob GETs, the mutation-only revision
must reuse the same cached graph with zero blob GETs, and the switched
base must use five blob GETs. The gate also records bytes, wall time,
peak RSS and compute estimate. It does not establish the general
compaction builder, mutation throughput, or large-scale quality.
