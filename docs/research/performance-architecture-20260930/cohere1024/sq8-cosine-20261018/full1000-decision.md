# SQ8 cosine: full1,000 quality confirmation

Retain a research quality gain: 9,737 -> 9,746 / 10,000 truth hits
(97.37% -> 97.46%), 19 gained and 10 displaced, no underfill.
Exact authenticated fetched rows, generation, requests, truth and roster unchanged.
All1,000 native rankings replay exactly; first32 rankings match the verified prior run.
This is development data, not an untouched holdout. All gains/losses retained.

Independent reconstructed-f64 cosine oracle also has9,746 hits. Query289 has a
ranking-order disagreement: no bit-identical f32/f64 arithmetic claim.
Maximum checked modeled workspace315,964,352 bytes below512MiB is not RSS.
Qualified source05737 includes full-cohort metadata accounting and schema-only
16MiB output cap. Other report caps unchanged. Affected native tests, release,
workspace Clippy correctness+suspicious and actual shim-unset testbuild passed;
unchanged scorer gates reused.

Original full1000 attempt INVALID: serializer hit old2MiB global cap. Its partial
output is not science. Fresh attempt uses identical configuration SHA
759ccf9c52e680a93976dfccde7d24e4a7c205af5ceb42931ffb713a353be49f,
only repairing preregistered output envelope after native qualification.

Next: consistent cosine immutable/mutable serving integration, ranking/error/drain/
memory and mutation checks, then actual-input admission and disposable canary before
paired runtime measurement. Not serving recall/latency/QPS, production readiness,
fully cold evidence or S3 Vectors/Turbopuffer parity. Retain the quality gain.
