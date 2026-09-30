# Source unit authority prerequisite

The source-plane writer now streams SHA256 for each 32-row encoded unit,
including a partial tail. Records and the digest table are flushed and synced
before manifest publication. The extra 16 KiB writer buffer is charged before
allocation. Source format v3 and generation format v5 reject older layouts.

The generation publication/GC roster retains records and adds the digest table.
Resident reference reopening authenticates both the table and its correspondence
to records, charging two table copies during validation. Remote startup still
hydrates records at this checkpoint; this is not the completed paging arm.

Local focused verification on 2026-09-30:

- Source writer red: missing page_digests.bin, exit 101, as expected.
- Source/application IDs/generation/delayed-delete GC: 13 passes, one AWS-only
  smoke test ignored, exit 0. Partial tail and a rebound-but-inconsistent digest
  table are covered, as are publication/reopening/conditional fences and GC.
- SQ8 authority isolation in prerequisite 87cd403d: four focused passes.

No full-workspace assurance, cold latency, throughput, scale or cost result is
claimed for these changed sources. Closed historical binaries remain immutable.
Next: authenticated source range transport and startup-only roster separation,
with source caps selected from the independently authenticated closed-trace replay.
