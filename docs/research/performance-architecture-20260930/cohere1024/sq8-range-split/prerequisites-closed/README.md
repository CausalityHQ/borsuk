# SQ8 split runtime prerequisites — closed

Native build source: `8e7076951936b0434b7947afd5b23e192c98fe70`.
Prerequisite runner: `6c05d7f764bcbb4bfdd6801b67e5144ec8eb7b22`, parent `63e63dfd2288fd7c8d125d1c1702c57ec47edae1`.
The retained producer/reducer release binaries were reused without rebuilding.

| Attempt | Result | Native work | Compute closure |
| --- | --- | --- | --- |
| a0001 | Execution INVALID: setup assembler inserted one LF and failed the source hash guard | None | Instance terminated, volume absent, timer inactive |
| a0002 | Prerequisites independently verified | Real candidate 32-query admission; distinct null 32-query smoke; whole sealed native validation of each; 15 mocked fault cases; actual historical/null canonical parity | Instance terminated, volume absent, timer inactive |

The complete a0002 prerequisite command took 26.166 seconds. This is **not query latency**. All six unique owned units completed and drained under CPU100%, CPU0, 2GiB, swap0, tasks128. OOM and swap peaks were zero. Charged cgroup memory peaks were 86,028,288 bytes for C1 and 78,540,800 bytes for NULL_SMOKE; these are **not RSS**. Pids peaks were 31 and 37.

## Independent verification

The original SSM command, exact instance, all four native/publication/shutdown/wrapper exits, master hash, actual unit ownership/invocations, raw controls, resource events, drain witnesses, independent expected headers, full native report metadata, original-file identity snapshots and infrastructure closure were checked. Both parity native validators exited zero before the ordered canonical comparison. The mocked cases are synthetic correctness witnesses, not scientific evidence. Metadata mutations for an empty receipt and a wrong invocation were each rejected with exit 1.

The first local metadata extraction mishandled the checker's intentional atomic hard-link publication of `parity.json`. Its type check used an unsafe `test && size_check` pattern, and it extracted an empty alias. This was detected before acceptance. The corrected extractor requires a regular selected member and resolves only the exact `parity.json` alias to the authenticated regular `.receipt.pending.json`. Remote execution and immutable archive bytes were unchanged. The independent verifier requires exactly one JSON document, so an empty receipt is refused. First extraction artifacts remain in the root's durable attempt directory.

No scientific result JSONL, vectors or ground truth were extracted or parsed locally. This directory contains selected closed metadata. Complete immutable archives remain in S3:

- Bucket: `borsuk-bench-453182569524-euc1`
- Key: `research/semantic-router/20261016/sq8-split-prerequisites-a0002/terminal/evidence.tar.gz`
- Bytes: `1462842`
- SHA256: `3e147a76b75c8a82ea9ce772fd8c954854f3029196581371411c3c2c8a267fc9`

The failed a0001 original receipts and source assembly are retained separately. A0002 embedded source checks prove the two scripts and asset manifest are byte-identical before launch. A later local command-assembly quoting failure created no target file or native run; structured source assembly and an embedded-master byte comparison replaced it before submission.

## Scope and next gate

This closes real-input admission, disposable SDK/assets/CLI/exit/cleanup smoke, mocked glue falsifiers, and disabled/historical parity for this exact experiment. The 184 native and 17 manager compiler gates and separate builder-removal mutation proof are retained in sibling directories; no Rust/Cargo/test-build bytes changed. It does not qualify the full product or establish a performance/recall/vendor win.

The scientific six-cell screen remains unrun: B0,C0 untraced then B1,C1,C2,B2 traced, same frozen binaries/inputs, baseline 32MiB versus candidate 2,121,728-byte physical cap, physical GET cap46, download64/compute1. A fresh Spot instance and final scientific receipt/manifest freeze are still required. Preserve the selected-panel limits and prior S3 access disclosure; no population p95/QPS or matched competitor claim follows from this panel.

Campaign compute upper so far: 537 seconds at the $0.06/hour bid cap = $0.00895. Including one remaining 3840-second allowance ($0.064) and the unchanged $0.15 ancillary reserve, the reconciled upper is $0.22295 under the unchanged $0.30 ceiling. These are conservative upper bounds, not actual billing or product cost.
