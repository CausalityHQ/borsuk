# Indexed source ID admission repair

Exact historical V36 writer revision 31445a91 materializes UInt64 source IDs; registered candidate shards remain Int64. The shared source reader now requires UInt64. Synthetic RED reproduced the admission failure; GREEN preserves row order/raw SHA and rejects duplicate/null/wrong-schema IDs. Root independently reran the existing helper self-check: exit 0. No Rust or dependency changes, corpus/query/GT or ANN measurement.

Freeze a distinct a0002 using the same source, selected IDs, resources and methodology; change only the source-type admission and resulting code pins. Original a0001 remains FAIL. New source-id-v2 draft authority leaves the original draft immutable. Root owns this small authority integration and frozen campaign decision; the bounded implementation was delegated.
