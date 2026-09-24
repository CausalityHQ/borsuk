# V154 ReLAION-1M attempt ledger

## a0001: premeasurement infrastructure failure

Source commit `6513bc4c18dbadd1f570c01083086e71066fa290`, archive
SHA-256 `11cf3183069547d66945356f6a27a10172dd93f47106c1fbaf25d67e29b86aa9`.
Spot `c7i.8xlarge` instance `i-0119be88579b95cb5` terminated after
its failed terminal, SHA-256
`d631429c5cd0085d18376b5c91b190a40c46359359beaf0854995f68f998bd19`.
Terminal URI:
`s3://borsuk-bench-453182569524-euc1/research/v154-relaion-graph/6513bc4c18dbadd1f570c01083086e71066fa290/runs/v154-20260924T142813Z/a0001/terminal.json`.
It reports phase `prepare`, exit 1, elapsed 295 seconds, uninterrupted.
The launcher authenticated every terminal-listed artifact. The closed
`prepare.log` reports `TypeError: zip() takes no keyword arguments`:
the remote system Python lacks `zip(..., strict=True)`. The checked
V146 terminal, centroid plane and V116 requests had downloaded, and
remote Rust check, focused tests and release build had completed.
There is no science artifact or measurement. This attempt contributes
no performance or quality number.

The only correction removes `strict=True` from the input converter's
`zip`; its two 1,000-row inputs are already length checked before the
loop. The V154 dataset, algorithm, budgets, paired-arm order, gate and
independent recount remain frozen. Retry the entire cell from a new
source archive under `a0002`; never overwrite a0001.
