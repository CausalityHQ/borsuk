# a0001 CLOSED: native fixture failure, not algorithm rejection

Source103f49c7f6384b8acd27271134a8cc86c7877247; driver371754c0.
Original SSM4600854f-8a64-4283-ac86-2422b1fb436a Failed101. Debug target
compiled successfully (5m18s), inventory24. First exact selected1 failed at
two_bit_gc_delayed_delete.rs:1123: expected injected DELETE failure but got
Ok(report scanned2, unknown1, delete_attempts0, EOFtrue). Remaining gates UNRUN.

Evidence archive358869 bytes; SHA256
99b3f6a7378a617bb1b56eadaebb4e63dfe09d998fc126a33ae7458d057c13eb
verified against terminal. Raw archive preserved. Selected completed compiler/
test metadata retained in failure-metadata, no scientific data parsed.
Before/after source check logs byte-identical,483 OK =478 source +5 runtime
admission identities. Peak memory5,431,443,456B, pids25, OOM0, swap0.
Owned cgroup drained without forced cleanup. Instancei-045302239debe2bfe
terminated, volumevol-0e1596f136dfbf227 absent254 with exactID,
external timerinactive; watcher64053 exit0, BGoff.

Root source diagnosis: Fixture::object calls object_store::path::Path::join
with a multi-segment maintenance/... name. The exact0.14.1 primary source
path/mod.rs:368 describes ONE segment; path/parts.rs includes '/' in INVALID
and percent-encodes it. GC correctly ignores the resulting non-hierarchical
key. The large filesystem test uses the same incorrect spelling in its
independent absence lookup. The ordinary unknown-key fixtures are unaffected.
The old std::path::PathBuf join at line251 is filesystem API and is correct.

Root TEST-ONLY repair5d8a87e03b8bd5173ca2c227cd408d04c53eb84a parent103f,
onefile7+/5-, repairs shared fixture key and matching absence lookup with
ObjectPath::from(format!(...)). No production/assertion/name/cap change.
Rustfmt/diff static0; repaired native remains UNVERIFIED. No retry launched.
Independent specialist launch for this slice was unavailable at four-child
limit; root made the minimal demonstrated fixture correction using private
Git index, preserving physical root index and original candidate/attempt.

Next: package repaired exactsource, new a0002 namespace and one native attempt;
compile then FIRST same exact failing test, then unchanged remaining gates.
No algorithm/performance KILL and no quality/vendor claim.
