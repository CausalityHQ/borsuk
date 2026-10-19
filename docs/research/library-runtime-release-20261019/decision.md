# Library fixture-runtime repair

The original broad native library run completed with 2049 passing, 28 failing and 9 ignored tests. Its immutable executable and logs are retained. Failures came from stale canonical-row-digest fixture metadata; related binary fixtures also declared generation v12 rather than current v13.

Commit 5cee36af5a11b1dd80352cce39e8c58dc6af9f61 repairs only three cfg(test) files. Actual per-record SHA256 sidecars and matching metadata replace incomplete fixtures. Production source outside test modules and existing assertions are unchanged. Full-source diagnostic fingerprints change; historical receipts retain their original bindings.

On exact frozen source: 31 fine-group library tests, 42 hierarchical library tests and 20 binary tests pass. All 28 original failures pass. The 45 previously passing affected library tests are superseded by fresh checks; 2004 unaffected original passes are reused and 9 ignored tests remain ignored. This is composite evidence, not a new full-suite execution. Workspace locked all-targets Clippy correctness/suspicious and the real override-unset workspace test-build pass. Commands and exits are in qualification.json.

No production benchmark binary was replaced and no performance result was rebound. This clears the observed fixture failures; it does not establish production readiness or a vendor win.

Original logs, failed repair attempt, final native receipts and executable are preserved under research/semantic-router/20261019/fp32-concurrent-full1000-a0001/library-runtime-closure-a0001.tar.gz in borsuk-bench-453182569524-euc1.
