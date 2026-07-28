# Laptop-reset archive — 2026-07-28

This branch preserves the ignored local research state that is intentionally
absent from `main`. Large archive parts are stored with Git LFS.

## Source snapshot

- Git base: `49286c14731f1d36b0b53e5fe725e5ecee9feb37`
- Source paths:
  - `.borsuk-scratch/`
  - `crates/borsuk/.borsuk-scratch/`
  - `docs/web/assets/benchmarks/raw/`
- Regular files: 126,426
- Regular-file bytes: 11,857,997,161
- Symlinks: 25
- Credential review: no live credential file or strict-format private key/token
  was found. Text matches were dependency source, metadata, documentation, and
  example credentials.

## Archive layout

Each source path is an independent `tar.zst` stream split into parts below
2 GB. The seven parts occupy approximately 8.6 GiB. `SHA256SUMS`
authenticates every part.

The verified archive entry counts are:

- `root-scratch`: 14,351
- `crate-scratch`: 68
- `raw-benchmarks`: 133,994

## Restore

From the repository root after checking out this branch and fetching LFS
objects:

```sh
git lfs pull
(
  cd backups/laptop-reset-2026-07-28
  sha256sum -c SHA256SUMS
)
cat backups/laptop-reset-2026-07-28/root-scratch.tar.zst.part-* \
  | zstd -d \
  | tar -xpf -
cat backups/laptop-reset-2026-07-28/crate-scratch.tar.zst.part-* \
  | zstd -d \
  | tar -xpf -
cat backups/laptop-reset-2026-07-28/raw-benchmarks.tar.zst.part-* \
  | zstd -d \
  | tar -xpf -
```

The archive paths are repository-relative, so extraction recreates the
original directory layout.
