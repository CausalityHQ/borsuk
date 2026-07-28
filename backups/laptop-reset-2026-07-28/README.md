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
2 GB. The seven repository-artifact parts occupy approximately 8.6 GiB.
`SHA256SUMS` authenticates every part.

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

## Encrypted Codex and Claude history

Two additional LFS parts preserve the client-side-encrypted conversation and
edit-history snapshot:

- original ciphertext:
  `codex-claude-history-20260728T193759Z.tar.zst.gpg`
- bytes: `3,202,851,760`
- ciphertext SHA-256:
  `05df39316c10d873f12cd963e8819418c496cb1822a6085d0e0b8b11870d737f`
- contents: 19,593 archive entries and 19,152 regular files, including 11,493
  Codex session transcripts/indexed threads, 4,280 Claude project/history
  files, and 3,360 Claude file-history snapshots
- encryption: GnuPG symmetric AES-256
- key: AWS SSM SecureString
  `/borsuk/private/laptop-reset-2026-07-28/history-archive-key`, version 1, in
  account `453182569524`, region `eu-central-1`

Authentication, OAuth, API keys, AWS state, settings, environment/shell
snapshots, MCP auth caches, downloaded binaries, logs, plugins/skills,
temporary files, and application caches were deliberately excluded. SQLite
databases were captured with SQLite's online backup API. A full
decrypt/decompress/tar traversal succeeded before splitting.

Restore after authenticating to the Causality AWS account:

```sh
cat backups/laptop-reset-2026-07-28/codex-claude-history-*.part-* \
  > ./codex-claude-history-20260728T193759Z.tar.zst.gpg
sha256sum ./codex-claude-history-20260728T193759Z.tar.zst.gpg

history_archive_key=$(AWS_PROFILE=causality aws ssm get-parameter \
  --region eu-central-1 \
  --name /borsuk/private/laptop-reset-2026-07-28/history-archive-key \
  --with-decryption \
  --query 'Parameter.Value' \
  --output text)

gpg --batch --quiet --pinentry-mode loopback \
  --passphrase "$history_archive_key" \
  --decrypt ./codex-claude-history-20260728T193759Z.tar.zst.gpg \
  | zstd -d \
  | tar -xpf - -C /Users/romanbartusiak

unset history_archive_key
```

The non-secret restore manifest is also in
`s3://borsuk-bench-453182569524-euc1/private/laptop-reset-2026-07-28/`.
The second S3 copy of the ciphertext was not completed because AWS SSO expired;
Git LFS is the durable ciphertext copy.
