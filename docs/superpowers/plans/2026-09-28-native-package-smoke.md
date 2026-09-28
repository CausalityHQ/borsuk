# Native package smoke repair

Scope: make existing fixture and metadata tests independent of an enclosing workspace after Cargo packaging. Preserve fixture bytes and algorithms. Add required matching versions to local dependencies; do not change private research crate publication policy.

1. Reproduce Cargo packaging failures and inspect external test inputs.
2. Place the two existing immutable fixtures inside the crate and read standard Cargo metadata/license paths.
3. Run the two existing fixture tests and package metadata test on the existing Spark machine; check package file inclusion and fixture SHA equality locally.
4. Record remaining package resolution blocker, commit and fast-forward push. No registry publication, paid jobs or architecture change.
