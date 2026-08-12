#!/usr/bin/env bash
set -euo pipefail
export PYTHONDONTWRITEBYTECODE=1

if [[ "$#" -ne 1 || "$1" != "--dry-run" ]]; then
  printf 'Publication V3 paid launch is unavailable until the AWS execution plan is implemented and reviewed\n' >&2
  exit 2
fi

cd "$(dirname "$0")/.."

if ! git diff --quiet || ! git diff --cached --quiet || [[ -n "$(git ls-files --others --exclude-standard)" ]]; then
  printf 'Publication V3 preflight requires a clean worktree including untracked files\n' >&2
  exit 2
fi

git fetch --quiet origin main
if ! git merge-base --is-ancestor origin/main HEAD; then
  printf 'origin/main must be an ancestor of the frozen source commit\n' >&2
  exit 2
fi

temporary="$(mktemp -d)"
trap 'rm -rf "$temporary"' EXIT
archive="$temporary/source-archive.tar.gz"
manifest="$temporary/manifest.json"
schedule="$temporary/schedule.json"
replay="$temporary/replay"
commit="$(git rev-parse HEAD)"

git archive --format=tar HEAD -- \
  Cargo.toml Cargo.lock crates scripts docs/research \
  python/uv.lock packages/borsuk/package-lock.json \
  | gzip -n >"$archive"

python3 - \
  docs/research/publication-v3-manifest.json \
  "$manifest" "$archive" "$commit" <<'PY'
import hashlib
import json
import pathlib
import sys

from scripts.publication_v3_protocol import canonical_json_bytes, validate_manifest


def digest(path: pathlib.Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


source, output, archive = map(pathlib.Path, sys.argv[1:4])
commit = sys.argv[4]
value = json.loads(source.read_text(encoding="utf-8"))
value["source"] = {
    "state": "frozen",
    "git_commit": commit,
    "archive_sha256": digest(archive),
    "cargo_lock_sha256": digest(pathlib.Path("Cargo.lock")),
    "python_lock_sha256": digest(pathlib.Path("python/uv.lock")),
    "node_lock_sha256": digest(pathlib.Path("packages/borsuk/package-lock.json")),
}
validated = validate_manifest(value)
output.write_bytes(canonical_json_bytes(validated) + b"\n")
PY

validation="$(python3 scripts/publication_v3_protocol.py validate "$manifest")"
paid_ready="$(python3 -c 'import json,sys; print(str(json.loads(sys.argv[1])["paid_ready"]).lower())' "$validation")"
structural_replay="blocked-until-paid-ready"
schedule_sha256=""
if [[ "$paid_ready" == "true" ]]; then
  python3 scripts/publication_v3_protocol.py schedule "$manifest" --output "$schedule" >/dev/null
  python3 scripts/publication_v3_protocol.py replay \
    "$manifest" --source-archive "$archive" --output "$replay" >/dev/null
  python3 scripts/validate_publication_v3_results.py \
    "$replay" --structural-only >/dev/null
  schedule_sha256="$(sha256sum "$schedule" | cut -d' ' -f1)"
  structural_replay="structurally-valid"
fi

python3 - "$manifest" "$archive" "$validation" "$structural_replay" "$schedule_sha256" <<'PY'
import hashlib
import json
import pathlib
import sys

from scripts.publication_v3_protocol import canonical_json_bytes

manifest_path = pathlib.Path(sys.argv[1])
archive_path = pathlib.Path(sys.argv[2])
validation = json.loads(sys.argv[3])
manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
report = {
    "schema_version": 1,
    "source_commit": manifest["source"]["git_commit"],
    "source_archive_sha256": hashlib.sha256(archive_path.read_bytes()).hexdigest(),
    "manifest_sha256": hashlib.sha256(canonical_json_bytes(manifest)).hexdigest(),
    "schedule_sha256": sys.argv[5] or None,
    "cargo_lock_sha256": manifest["source"]["cargo_lock_sha256"],
    "python_lock_sha256": manifest["source"]["python_lock_sha256"],
    "node_lock_sha256": manifest["source"]["node_lock_sha256"],
    "result_prefix": manifest["prefixes"]["result"],
    "index_prefix": manifest["prefixes"]["index"],
    "paid_ready": validation["paid_ready"],
    "unstaged_datasets": validation["unstaged_datasets"],
    "structural_replay": sys.argv[4],
}
print(json.dumps(report, sort_keys=True, separators=(",", ":")))
PY
