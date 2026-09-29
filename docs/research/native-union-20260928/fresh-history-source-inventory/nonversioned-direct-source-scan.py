"""Inspect original nonversioned source archives with terminal SHA authority."""

import hashlib
import io
import json
from pathlib import Path
import tarfile

import boto3


ROOT = Path(__file__).resolve().parent.parent
BUCKET = "borsuk-bench-453182569524-euc1"
PATTERNS = (b"relaion2b_features_", b"andropar/relaion2b")


def main():
    bindings = json.loads((ROOT / "fresh-history-source-bindings.json").read_text())["bindings"]
    prior = json.loads((ROOT / "fresh-history-direct-source-scan.json").read_text())
    covered = {row["archive_sha256"] for row in prior["original_archives"]}
    grouped = {}
    for row in bindings:
        if not row["terminal_key"].startswith("research/native-") or not row["source_archive"]:
            continue
        sha = row["source_archive"]["sha256"]
        if sha in covered or "/runs/" not in row["terminal_key"]:
            continue
        base = row["terminal_key"].split("/runs/", 1)[0]
        key = base + "/source/source.tar.gz"
        grouped.setdefault((key, sha), []).append(row["terminal_key"])
    s3 = boto3.Session(profile_name="causality").client("s3", region_name="eu-central-1")
    records = []

    def inspect(key, expected, terminals, blob):
        actual = hashlib.sha256(blob).hexdigest()
        if actual != expected:
            raise ValueError(f"terminal-bound source archive differs: {key}")
        files = []
        with tarfile.open(fileobj=io.BytesIO(blob), mode="r:gz") as archive:
            for member in archive:
                name = member.name.removeprefix("./")
                if (member.isfile() and name.startswith(("scripts/", "crates/"))
                        and name.endswith((".py", ".rs", ".sh"))):
                    if member.size > 16_000_000:
                        raise ValueError(f"source file too large: {name}")
                    body = archive.extractfile(member).read()
                    if any(pattern in body for pattern in PATTERNS):
                        files.append(name)
        records.append({"source_key": key, "source_sha256": expected,
                        "source_bytes": len(blob), "terminal_keys": sorted(terminals),
                        "direct_source_reference_files": sorted(files)})

    for (key, expected), terminals in sorted(grouped.items()):
        try:
            blob = s3.get_object(Bucket=BUCKET, Key=key)["Body"].read()
        except s3.exceptions.NoSuchKey:
            continue
        inspect(key, expected, terminals, blob)
    scanned = {row["source_sha256"] for row in records} | covered
    for row in bindings:
        if not row["terminal_key"].startswith("research/native-") or not row["source_archive"]:
            continue
        expected = row["source_archive"]["sha256"]
        if expected in scanned or "/runs/" not in row["terminal_key"]:
            continue
        terminal = row["terminal_key"]
        attempt = terminal.split("/evidence/terminal.json", 1)[0].split("/terminal.json", 1)[0]
        key = attempt + "/source.tar.gz"
        try:
            blob = s3.get_object(Bucket=BUCKET, Key=key)["Body"].read()
        except s3.exceptions.NoSuchKey:
            continue
        inspect(key, expected, [terminal], blob)
        scanned.add(expected)
    records.sort(key=lambda row: row["source_key"])
    report = {"metadata_only": True, "query_gt_or_sealed_bodies_opened": False,
              "complete_prior_query_audit": False, "original_source_archives_scanned": len(records),
              "records": records}
    (ROOT / "fresh-nonversioned-direct-source-scan.json").write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps({"archives": len(records),
                      "reference_files": sorted({name for row in records
                                                 for name in row["direct_source_reference_files"]})}))


if __name__ == "__main__":
    main()
