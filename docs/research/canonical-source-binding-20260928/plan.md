# Authenticated canonical source for self-contained compaction

Base824b61f1. Generation schema v2 requires a canonical source descriptor:
rows/dimensions/bytes/SHA/approved content-addressed key. Stream normalized FP32
rows plus signed logical IDs in physical order to canonical.bin during build.
Use explicit source permutation for application IDs, ordinal convenience for
ordinal inputs. Authenticate raw and SQ8 inputs; no new encoder/ID map/hydration.
Existing caller immutability contract applies throughout construction.

Publisher streams/hash-validates canonical.bin through existing bounded multipart
uploader before metadata/head CAS. Source object is outside METADATA_FILES:
open/query do not fetch or allocate it. Reader validates descriptor geometry and
binding; v1 generations reject. Historical artifacts unchanged, no legacy reader.
Canonical source is durable maintenance storage, not resident query cache.

Expose explicit recovery to a new local file, streaming one pinned source GET,
checking length/SHA before atomic no-clobber rename. Bounded source chunks and
caller disk cap; stage scratch removed on error/cancel. Preserve submitted SDK GET
and delivered-byte charges on errors. Transport retries/overhead are separately
owned by caller and must be measured in lifecycle receipts. No queries invoke it.

Extend signed-ID fixture: v2/file bytes, normalized rows/IDs in correct order,
publish/recover exact, cap/corrupt-source rejection without visible output;
metadata-only open makes zero canonical/SQ8 payload GETs. Update exact generated
fixture to v2 and preserve source/graph parity. Original focused Spark tests,
then existing HTTP mutation checks. No cloud/new consultation or full suite.
Next atomic base/delta compaction handoff and GC; canonical lifecycle storage and
maintenance I/O cost remain unmeasured release gates against both vendors.
