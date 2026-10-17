# Packed 32-row scorer: valid screen, full-1000 pending

Decision: **VALID_SCREEN_RETAIN_PENDING_FULL1000**.

Retain candidate `7acbbb82d48078f46c0a163614db39eeee15afc7` as a valid screening result. This documentation commit is based on `a6663b4ea4b8bada6195f4575da86a7203b1513e`; it adopts no candidate code. The parent retains integration, push, native execution, paid work, and full-1000 authority.

The closed fixed-32 ABBA screen ran A1/B1/B2/A2, repeating the same 32 queries twice per arm. These are neither 64 independent queries nor a full-1000 run. The unchanged original summary bytes are retained in [panel32-summary.json](panel32-summary.json); exact source paths, terminal labels, exits, and hashes are in [receipts.json](receipts.json).

| Candidate/control ratio | Pooled | Forward | Reverse |
| --- | ---: | ---: | ---: |
| CPU | 0.9410655286018675 | 0.9400817833351558 | 0.9420484182964055 |
| Wall | 0.9659037156032563 | 0.9707676090568746 | 0.9610300052611919 |

Pooled planning-wall ratio is 0.8066546844530529. CPU improves about 5.89%, wall about 3.41%, and planning wall about 19.33%; CPU and wall improve in both orders. Source-wall ratio is 1.0021233370395284. There is no arbitrary 20% gain gate. Panel tails remain descriptive and do not qualify population tails or establish a vendor win.

Every arm returns 309/320 hits (96.5625% recall). All four semantic hashes are exactly `fd7eb4eefd3844640e3207d55f2aadec580a065f33caaf7c2af2f46bae0b99bb`; result semantics and ALL logical charges are equal. Each cell charges router 0 GET/0 bytes, source 3,474 GET/914,411,520 verified bytes, and SQ8 1,004 GET/290,875,648 verified bytes, with no failed GETs. All four native and four strict-reader exits and runner.exit are 0. The preserved terminal label says native/readers passed and parity analysis pending; the equality and ratios above are the closed evidence analysis recorded here.

Existing source correctness evidence has fmt, rotated, generation, integration, scalar-control, Clippy, and workspace test-build exits 0, with affected passing counts 5/51/2/7 respectively. These are prior completed checks, not checks rerun for this documentation change. Existing release evidence has all four exits 0 and artifact hashes; actual score.asm contains 16 SSE2 addpd instructions in the serving loop and frame 0x208. Conservative scratch accounting adds a modeled 447 bytes; that modeled delta is not a measured RSS delta. Per-cell observed RSS is retained in the exact summary without attributing it to scratch.

Actual authenticated-input admission and the separate two-query real-service canary passed all native and strict-reader gates, runner exit 0, with exact control/candidate canary semantics. The prospective protocol is preserved by path and hash in receipts.json. Independent review `028dc62f9db547f5` was already read by the parent with no blocker, per the assignment; no new review is claimed.

**Candidate full-1000: UNRUN.** The incumbent's historical full-1000 recall of 97.83% is a separate historical result and cannot be assigned to this candidate or substituted for candidate qualification. The historical four-row rejection remains intact. This evidence slice changes no format, authentication, quality, or release gates.
