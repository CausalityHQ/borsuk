# Experimental status

BORSUK is unreleased. Public APIs, defaults, and stored formats can change.
Current native code includes authenticated build/publication/open/search,
logical IDs, durable mutations and recovery, callable compaction, and garbage
collection. Local synthetic fixtures and S3 evidence cover specific operations.

That coverage is not a production-readiness certification. The current
maintenance lifetime locks cover cooperating callers on one host or shared
local filesystem. Python and TypeScript bindings are separate experimental
surfaces.

Read the [native API guide](api.md), [coordination boundary](consistency.md),
and [development measurement scope](benchmarks.md).
