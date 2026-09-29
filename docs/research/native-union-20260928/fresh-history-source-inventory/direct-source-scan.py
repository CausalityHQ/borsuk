"""Find direct ReLAION physical-shard references in authenticated source code.

Only source archives and Git source trees are read; no query, GT, or results.
"""

import hashlib
import io
import json
import os
from pathlib import Path
import subprocess
import tarfile


ROOT = Path(__file__).resolve().parent.parent
INVENTORY = ROOT / "fresh-history-source-inventory"
BUCKET = "borsuk-bench-453182569524-euc1"
PATTERNS = (b"relaion2b_features_", b"andropar/relaion2b")


def original_archives():
    selected = json.loads((INVENTORY / "v130-v155-collection-status.json").read_text())
    rows = []
    for entry in selected:
        proof = json.loads((ROOT / entry["proof"]).read_text())
        rows.append((proof["archive_key"], proof["archive_sha256"], proof.get("archive_bytes")))
    older = json.loads((INVENTORY / "v114-v116-query-producers.json").read_text())
    rows.extend((p["archive_key"], p["archive_sha256"], p["archive_bytes"])
                for p in older["producer_proofs"])
    v126 = json.loads((ROOT / "fresh-history-source-bindings.json").read_text())
    terminal_shas = {row["source_archive"]["sha256"] for row in v126["bindings"]
                     if "/v126-relaion-expansion-source/" in row["terminal_key"]}
    assert terminal_shas == {
        "db3a4133d6340a54a00a582e7812ddbe62260a818969bca964f74229ee47af43",
        "497b3f8cdb1110d0bbff918b0726672cf71bb9fe9218520e7a5523280ee4b9db",
    }
    base = "research/v126-relaion-expansion-source/c334ec638932f579f2ee62766d24c722c7c9b07c/source/"
    rows.extend([(base + "source.tar.gz", "db3a4133d6340a54a00a582e7812ddbe62260a818969bca964f74229ee47af43", 22067),
                 (base + "source-full.tar.gz", "497b3f8cdb1110d0bbff918b0726672cf71bb9fe9218520e7a5523280ee4b9db", 11615121)])
    assert len(rows) == 35
    return sorted(set(rows))


def inspect_archive(key, expected_sha, expected_bytes):
    proc = subprocess.run(["aws", "s3", "cp", f"s3://{BUCKET}/{key}", "-", "--only-show-errors"],
                          env={**os.environ, "AWS_PROFILE": "causality"}, capture_output=True)
    if proc.returncode:
        raise RuntimeError(f"source archive fetch failed: {key}: {proc.stderr.decode()}")
    blob = proc.stdout
    assert hashlib.sha256(blob).hexdigest() == expected_sha
    assert expected_bytes is None or len(blob) == expected_bytes
    matches = []
    with tarfile.open(fileobj=io.BytesIO(blob), mode="r:gz") as archive:
        for member in archive:
            name = member.name.removeprefix("./")
            if not member.isfile() or not name.startswith(("scripts/", "crates/")):
                continue
            if not name.endswith((".py", ".rs", ".sh")):
                continue
            if member.size > 16_000_000:
                raise ValueError(f"source code file unexpectedly large: {name}")
            body = archive.extractfile(member).read()
            if any(pattern in body for pattern in PATTERNS):
                matches.append(name)
    return {"archive_key": key, "archive_sha256": expected_sha, "archive_bytes": len(blob),
            "direct_source_reference_files": sorted(matches)}


def main():
    archive_results = [inspect_archive(*row) for row in original_archives()]
    reconstruction = json.loads((ROOT / "fresh-history-archive-reconstruction.json").read_text())
    commits = sorted({p["source_commit"] for p in reconstruction["proofs"]
                      if p["exact_archive_match"]})
    assert len(commits) == 149
    git_results = []
    for commit in commits:
        proc = subprocess.run(["git", "grep", "-l", "-e", "relaion2b_features_",
                               "-e", "andropar/relaion2b", commit, "--", "scripts", "crates"],
                              capture_output=True, text=True)
        if proc.returncode not in (0, 1):
            raise RuntimeError(proc.stderr)
        git_results.append({"commit": commit,
                            "direct_source_reference_files": sorted(
                                row.split(":", 1)[1] for row in proc.stdout.splitlines())})
    result = {"metadata_only": True, "query_gt_or_sealed_bodies_opened": False,
              "complete_prior_query_audit": False,
              "method": "Exact Git archive source matches or original terminal/reservation-bound archive SHA, then literal direct physical-source references in scripts/ and crates/ code only.",
              "original_archives_scanned": len(archive_results),
              "exact_git_source_commits_scanned": len(git_results),
              "original_archives": archive_results, "exact_git_source_commits": git_results,
              "qualification": False}
    (ROOT / "fresh-history-direct-source-scan.json").write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps({"original_archives": len(archive_results), "git_commits": len(git_results),
                      "archive_reference_files": sorted({f for row in archive_results for f in row["direct_source_reference_files"]}),
                      "git_reference_files": sorted({f for row in git_results for f in row["direct_source_reference_files"]})}))


if __name__ == "__main__":
    main()
