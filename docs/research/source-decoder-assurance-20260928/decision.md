# Decoder fixture gate: repaired, workspace remains red

The physical one-row decoder cap correctly rejects a two-row Parquet row group. The fixture now checks that rejection, then writes one-row groups using existing WriterProperties and retains scalar parity/replay assertions. No production admission changed.

Frozen source d954ba1b2dac830caaf83e7e4104481c826f3484a272a84476d5b8b79c2369b5: V36 target63 passed; full workspace library1687 passed,0failed,6ignored; V36 rerun63passed. The full gate subsequently failed in WAL integration:25passed,4failed. Two fixtures expect retired automaticV20 authority; two claim-free finalization checks expose a guard that omits native_bounded_ann_ref. Later targets were not reached. No workspace-pass or performance claim.

Independently checked all three terminal artifact hashes/lengths, changed fixture against frozen S3 archive, and EC2 terminated state. Worker i-0ee7b6aa43593ab11 terminated after780seconds; estimated compute $.0461 excludes EBS/S3 and is not an invoice. Prior visibility-failed launch was terminated and discarded before serial retry. No active worker remains.

Next: supported native fixtures and shared finalization guard; narrow WAL target before another full workspace gate. Existing production quality KILL and vendor gates remain open.
