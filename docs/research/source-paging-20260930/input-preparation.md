# Pinned 1M input conversion

The query-blind converter is research preparation outside Rust. The production
reader still rejects old layouts; it has no legacy reader or migration path.
It accepts an authenticated v4 root/v2 source receipt, checks all nine old input
bodies, preserves the seven payload files, streams 32-row source SHA256 entries
(including a partial tail), and publishes the v3 receipt/v6 root last.

The source-only snapshot is bounded to at most 1M rows, dimensions <=8192 and
300 MB per input object. Conversion uses the existing 4 MiB file-hash helper,
streaming copies and a 16 KiB sidecar writer. These are preparation bounds, not
measured RSS or native query-memory claims.

`python3 -m scripts.prepare_native_paged_source` passes the runnable synthetic
33-row/tail check and independent tampering of each of the nine inputs. The
frozen real input config is metadata-geometry-config.json, SHA256
410ce351d764bccccafcfe6e34697dcc961856288c67f8519c451fa4266143c3.

| Dataset / corpus | Original root | Converted v6 root |
| --- | --- | --- |
| ReLAION FIRST1M, D768 | d8e7ccf090f322bbb39600f096e8d64b7dc15ed8f5d7dfd6344b1c076dc99cce | b089766bb08cd0fc091162abf4353ca631595ef771aa27294d27f606d5f8fe8b |
| CoHere FIRST1M, D768 | a4eb4851c545e828f3d08181a0c3e9cca09ed9341032ee3e69c511b9b7caf67e | 73610189db28eee8cacdec1867fab6ee18e197ddecbda9ba4cfa2cd0684a9ccf |

The per-dataset conversion receipts authenticate every original/converted body.
Each source records object remains 200,000,000 bytes; each new digest table is
1,000,000 bytes. Mean, source order, source identity, source/scorer/SQ8 binding,
centroids, both graphs, SQ8 authority and canonical reference are preserved.
Referenced canonical/SQ8 payloads are reused from their prior qualification;
they were not downloaded or requalified by this conversion.

The current native `two_bit_plan_demo` binary (SHA256
649f14183d026941c6e7c3b464c810f48d730193495284119d2befc1efbbf4ba,
Rust sources at 2b082736) successfully opened each converted resident generation.
The next request-roster guard rejected an authenticated empty file, exactly
`Error: "request roster size"`, exit 1. Source code places this guard after
generation opening and before request parsing/output creation. This deliberate
negative gate verifies native input decoding with no query/truth body or ANN
execution; it is not a successful process qualification, ARM assurance, remote
startup validation or performance measurement. Exact receipts are retained.

The whole native source manifest freezes all 395 Rust/Cargo files. New Python
preparation/controller work does not change that Rust identity. Existing full
assurance is not claimed for this changed native implementation.

Next: publish ten authenticated bodies to a fresh immutable research prefix,
HEAD last, preserve the original namespace, then qualify the changed ARM binary
and freeze the new runtime/controller/config/artifact identities. Only then run
the ReLAION-first cold development panel, followed by CoHere. Old reference
header authority may be rebound in a new file only after source/scorer parity;
keep its original receipt and all reference query rows byte-identical.
No matched-control latency, vendor win, recall, cold latency, QPS, total dollars
or 100M result follows from these input checks.
