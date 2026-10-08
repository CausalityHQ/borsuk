# Four-row qualification a0004: correctness passed; control build invalid

Exact candidate `fd2cb50ee5856a5c43131ca23d38c7be8c8bc73b`, frozen protocol `d3c9830b4ce4f30aea38ad05eac59957bebc8610`. Original watcher85898 closed0. Same instance `i-05426de087af47c5a` was terminated and waited before collection.

Independent verification authenticates all161 artifact bodies,415 native and2454 support files before/after, all13 serial stage native/tee exits0, required debug/release/scalar tests, locked workspace Clippy and actual shim-unset test compilation. Capped local verification exited0 (CPU1/256MiB/swap0/120s;231ms,42.4MiB peak). This proves compiler/synthetic correctness only.

The paired control serving build is INVALID: shared candidate target output was reused, control-release finished in0.18s without compiling borsuk, and both retained control serving binaries have exactly the candidate byte counts and hashes. Frozen control source c49a2e6d lacks score_four, whereas authenticated candidate binary contains score_four with two packed addpd loop instructions. Source hashes alone do not prove control binary provenance.

Preserve this attempt. Build unchanged control with isolated output, authenticate its actual compiler artifacts, then verify ordered arithmetic and matched production nested stack before the unchanged primitive timing gate. No algorithm rejection, no primitive run, no cold result and no performance claim. Candidate source is not integrated.
