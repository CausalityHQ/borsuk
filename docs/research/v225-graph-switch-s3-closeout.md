# V225 real S3 graph switch closeout

**Decision:** retain the conditional S3 head and pinned resident-reader
handoff. A changed, authenticated graph advanced the real S3 head; a
stale writer could not move it. This is a four-row transactional gate,
not a 100k mutation-quality or service-performance result.

The frozen source commit was
`47339e2a4eb8b1c1a84568c48da7c32efed765f1`; source archive SHA-256
`4b53996d4fb7676abbdadf2030dc5b7d3378c0d1db88f2254d0d6b555a5206f6`.
The original attempt was `a0001` under
`s3://borsuk-bench-453182569524-euc1/research/v225-graph-switch-s3/47339e2a4eb8b1c1a84568c48da7c32efed765f1/runs/a0001/`.
Spot c7i.4xlarge instance `i-04325955d4768784b` in eu-central-1c is
confirmed terminated. Terminal SHA-256 is
`bf7aa38ccef393e7f62d140c83756bc489f90ad66cd390c34ddbe2462eef97ff`;
closeout SHA-256 is
`9ac29c0293c03c3571020eb9fa2dbcf1b27b4ea7bc983c8564073d9c8ab97af2`.
Independent S3 readback verified all five terminal artifacts by length
and SHA-256.

Generation 7 had root SHA-256
`aca53cec8db4556f8aa41715bada8886dc45929c37d7835e500b6a04a6c26c2d`.
Its pinned reader returned IDs `[42,7,19,33]`. Generation 8 changed the
first vector and ID; its root SHA-256 was
`391396e15ddaf8782089b5d71f654c713958352dee0859d36f1a9f55cca78a41`.
After the S3 versioned head update and resident slot replacement, a new
reader returned `[7,19,99,33]`, while the held reader still returned its
generation 7 IDs. Both cold hydrations fetched five blob objects. A valid
generation 9 published with the stale generation 7 token received an S3
precondition error; head readback remained at generation 8. `gate_pass=true`.

The example's wall time was **2.96 seconds** and peak RSS **22,695,936
bytes**. These include two tiny resident graphs and local S3 transfers;
they do not predict 100k or 1M mutation latency or RAM. The Spot quote
was $0.3631/hour and compute to the terminal marker is estimated at
**$0.023355**, excluding S3, EBS, network and billing adjustments.

The next design gate is a bounded mutation delta with exact visibility,
delete/upsert rules and compaction back to the selected graph format.
Falsify it at 100k against the strongest frozen BORSUK baseline before a
new 1M run. Provision overlap memory for pinned old generations; V224
measured a 3.93 GB peak when two 1M graphs were intentionally held.
