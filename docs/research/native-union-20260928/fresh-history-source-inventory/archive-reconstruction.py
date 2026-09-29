"""Prove exact historical source bytes from Git; mismatches stay unverified.

Source archive work only. No builds, vector/GT bodies, or numerical execution.
"""
import concurrent.futures
import gzip
import hashlib
import io
import json
import subprocess
from pathlib import Path

root = Path(__file__).resolve().parent.parent
bindings = json.loads((root / "fresh-history-source-bindings.json").read_text())["bindings"]
groups = {}
for binding in bindings:
    archive = binding["source_archive"]
    if archive and binding["source_commit"]:
        groups.setdefault((binding["source_commit"], archive["sha256"]), []).append(binding)


def reconstruct(pair):
    commit, expected = pair
    result = subprocess.run(["git", "archive", "--format=tar", commit], capture_output=True)
    report = dict(source_commit=commit, expected_archive_sha256=expected,
                  terminal_keys=[row["terminal_key"] for row in groups[pair]], exact_archive_match=False)
    if result.returncode:
        report["reason"] = result.stderr.decode().strip()
        return report
    raw = result.stdout
    raw_sha = hashlib.sha256(raw).hexdigest()
    if raw_sha == expected:
        report.update(exact_archive_match=True, archive_format="git-tar", archive_bytes=len(raw))
    else:
        buffer = io.BytesIO()
        with gzip.GzipFile(filename="", mode="wb", fileobj=buffer, mtime=0, compresslevel=9) as zipper:
            zipper.write(raw)
        data = buffer.getvalue()
        digest = hashlib.sha256(data).hexdigest()
        report.update(reconstructed_tar_sha256=raw_sha, reconstructed_gzip_sha256=digest)
        if digest == expected:
            report.update(exact_archive_match=True, archive_format="git-tar-gzip9-mtime0", archive_bytes=len(data))
        else:
            report["reason"] = "Git archive bytes differ; requires original archive/producer proof. No source or query identity inferred."
    if report["exact_archive_match"]:
        for row in groups[pair]:
            ident = row["source_archive"]
            size = ident.get("bytes", ident.get("encoded_bytes"))
            assert size is None or size == report["archive_bytes"]
    return report


with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
    proofs = list(pool.map(reconstruct, groups))
assert len(proofs) == len(groups)
positive = next(p for p in proofs if p["source_commit"] == "f5cc5a01a7184112adb609c3fff4e59508da5ecb")
assert positive["exact_archive_match"] and positive["archive_format"] == "git-tar-gzip9-mtime0"
matched = [p for p in proofs if p["exact_archive_match"]]
report = dict(metadata_only=True, query_gt_or_sealed_bodies_opened=False, complete_prior_query_audit=False,
              method="Exact SHA256 equality to independently collected terminal archive identity; Git source content only on match.",
              unique_source_pairs=len(proofs), matched_source_pairs=len(matched),
              matched_terminal_count=sum(len(p["terminal_keys"]) for p in matched), proofs=proofs)
(root / "fresh-history-archive-reconstruction.json").write_text(json.dumps(report, indent=2) + "\n")
print(json.dumps({k: report[k] for k in ("unique_source_pairs", "matched_source_pairs", "matched_terminal_count")}))
