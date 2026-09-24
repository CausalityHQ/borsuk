# V129 authenticated source-block reuse

Status: verified production scorer increment; **no live latency or 100k
quality claim**. The exact source plane is still a candidate architecture.

The source scorer now visits a fixed candidate roster in source-ordinal order
and retains one authenticated verification block for consecutive rows in that
block. It reuses the bytes and digest result only within one rank call. Each
new block is read and SHA-256 checked against the digest table derived at
authenticated open. The result is still sorted by exact cosine score and
source ID, so candidate membership and deterministic ties are unchanged.
The block buffer stays at the configured 4 KiB to 1 MiB verification size;
it does not grow with the source corpus or candidate count. The sorted roster
does use O(candidate count) memory. This assumes the published local
generation remains immutable while a query runs.

The focused test-first Causality Spot check used a source archive over commit
`16a96e8c1e8ea24c2379ea382768d0e31b1c2af2`. In the red archive, only
the expected local read cost changed. The test failed as intended: the old
implementation made two authenticated reads for two rows in one block,
versus one expected. Eight other focused tests passed. Red archive SHA-256
`da4b3d306cfc5ae7bb6bcf1cddd198dc2b5c325af81bccdf9156cb3c70f1f5c1`;
terminal SHA-256
`f568d2ac091d771d630c95d4326db64d44cc7a1612d635d622f4d5fc438fbf65`.
The complete green archive SHA-256 was
`6d83debe01960d6590a1e08a47adcb17a41075b3ac5380473a674309236372c2`;
its terminal SHA-256 was
`f03f9b93fc84263c2e9d1294aa73b1b46bc1d15f78e986778c1a1a524f9f395c`.
All nine focused tests passed. Both workers were `c7i.8xlarge` Spot in
`eu-central-1` and were observed terminated. An earlier red setup attempt
failed to compile because the slim archive omitted two test fixtures; its
terminal SHA-256 was
`e18eea5259a4070e4030bf8dce2553d06fa656de3b78687b3cecc71ad8898f5b`.
It is excluded from the red/green result.

The immutable red and green receipts and test logs are under
`s3://borsuk-bench-453182569524-euc1/research/v129-source-cache-codecheck/16a96e8c1e8ea24c2379ea382768d0e31b1c2af2/runs/`
at `red-v129-codecheck-20260924T045333Z/a0001` and
`green-v129-codecheck-20260924T045607Z/a0001`, respectively.

The tiny measured test changed the source scorer's logical local read cost
from 2 block reads and 192 bytes to 1 block read and 96 bytes for two rows
in one block. A second test checks that two rows spanning three blocks need
exactly three reads and 8,272 local bytes after reuse. These are test
geometry measurements, not serving latency, physical SSD I/O, or a corpus
scale estimate.

Next gate: replay the sealed V122 deep-image-96 100k development cohort with
the production source writer, ID map, returned SQ8 scorer, and exact source
scorer. Authenticate the input artifacts, preserve the same candidate and
capped-control ranges, and require the returned SQ8 top-100 to match V122
before interpreting source recall. Measure local source read bytes, rank
latency, charged memory, and paired Recall@100. Then run a separate live S3
serving gate; the existing offline screen cannot establish its GET or latency
cost, because its 10.8 MB SQ8 object was often fetched in full.
