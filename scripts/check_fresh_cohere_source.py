"""Pin one unused-candidate CoHere query source without reading vectors or GT."""

import hashlib
import json
from pathlib import Path
import subprocess

import boto3


ROOT = Path("docs/research/native-union-20260928")
BUCKET = "borsuk-bench-453182569524-euc1"
RECEIPT = "publication/v3/20260812/datasets/cohere-large-10m-768/attempts/0001/STAGING_COMPLETE.json"
RECEIPT_SHA = "0965aa0241199822dfac3410bba4edad5536ac0eb0aaa8ab83c216e8c5749a87"
SHARD = "train-00000046.parquet"
SHARD_SHA = "13195bd1a643b659679156745e45dfdf3f2be7d8e340da1dd97bf84352cbf0c4"
FIRST, COUNT = 1_005_000, 1_000


def sha(body):
    return hashlib.sha256(body).hexdigest()


def overlaps(a, b):
    return a[0] <= b[1] and b[0] <= a[1]


def main():
    assert not overlaps((FIRST, FIRST + COUNT - 1), (1_000_000, 1_001_999))
    assert overlaps((FIRST, FIRST + COUNT - 1), (1_005_500, 1_006_000))
    s3 = boto3.Session(profile_name="causality", region_name="eu-central-1").client("s3")
    body = s3.get_object(Bucket=BUCKET, Key=RECEIPT)["Body"].read()
    if sha(body) != RECEIPT_SHA:
        raise ValueError("closed CoHere publication receipt differs")
    receipt = json.loads(body)
    if receipt["dataset_id"] != "cohere-large-10m-768" or len(receipt["objects"]) != 461:
        raise ValueError("CoHere publication source roster differs")
    start = 0
    selected = None
    for item in receipt["objects"]:
        if item["role"] != "train":
            continue
        end = start + item["rows"]
        if item["uri"].endswith("/" + SHARD):
            selected = (start, end, item)
        start = end
    if start != 10_000_000 or selected is None:
        raise ValueError("CoHere train geometry differs")
    start, end, item = selected
    if not (start <= FIRST and FIRST + COUNT <= end and item["sha256"] == SHARD_SHA):
        raise ValueError("candidate panel is outside pinned source shard")
    key = item["uri"].removeprefix("s3://" + BUCKET + "/")
    head = s3.head_object(Bucket=BUCKET, Key=key)
    if (head["ContentLength"] != item["bytes"]
            or head["Metadata"].get("borsuk-sha256") != SHARD_SHA):
        raise ValueError("pinned source shard HEAD differs")

    proof_paths = {
        "registered_test": ROOT / "fresh-cohere-10m-test-family.json",
        "stress_train": ROOT / "fresh-cohere-stress-query-families.json",
        "v271_train": ROOT / "fresh-cohere-prior-identity/verification.json",
        "v277_train": ROOT / "fresh-cohere-prior-identity/v277-verification.json",
        "v278_train": ROOT / "fresh-history-source-inventory/v278-exclusion-proof.json",
    }
    proofs = {name: json.loads(path.read_text()) for name, path in proof_paths.items()}
    excluded = [proofs["stress_train"]["excluded_raw_source_rows_inclusive"],
                proofs["v277_train"]["prior_query_rows_inclusive"],
                proofs["v271_train"]["prior_query_rows_inclusive"],
                proofs["v278_train"]["prior_query_source_rows"]]
    if (excluded != [[100000, 103999], [104000, 104999],
                     [1000000, 1000999], [1001000, 1001999]]
            or not proofs["registered_test"]["metadata_verified"]
            or any(overlaps((FIRST, FIRST + COUNT - 1), pair) for pair in excluded)):
        raise ValueError("authenticated prior CoHere query exclusion differs")

    history = json.loads((ROOT / "fresh-history-archive-reconstruction.json").read_text())
    commits = sorted({p["source_commit"] for p in history["proofs"] if p["exact_archive_match"]})
    if len(commits) != 149:
        raise ValueError("authenticated Git source roster differs")
    pattern = r"(^|[^0-9])1_?005_?000([^0-9]|$)|(^|[^0-9])1_?005_?999([^0-9]|$)|cohere-large-10m-768/.*train-00000046"
    matches = []
    for commit in commits:
        found = subprocess.run(["git", "grep", "-l", "-E", pattern, commit, "--",
                                "scripts/*.py", "scripts/*.sh", "crates/**/*.rs"],
                               capture_output=True, text=True)
        if found.returncode not in (0, 1):
            raise RuntimeError(found.stderr)
        matches.extend(found.stdout.splitlines())
    if matches:
        raise ValueError("candidate query source has a historical direct code reference")
    report = {
        "schema": "borsuk-fresh-cohere-source-candidate-v1",
        "metadata_only": True, "vector_gt_or_query_bodies_opened": False,
        "complete_prior_query_audit": False, "query_selection_status": "provisional",
        "dataset": "CoHere-large-10M D768 cosine", "indexed_source_rows": [0, 999999],
        "candidate_query_rows_inclusive": [FIRST, FIRST + COUNT - 1],
        "source_receipt": {"key": RECEIPT, "sha256": RECEIPT_SHA},
        "source_shard": {"key": key, "bytes": item["bytes"], "sha256": SHARD_SHA,
                         "etag": head["ETag"], "row_start": start, "row_end_exclusive": end},
        "known_consumed_train_ranges_inclusive": excluded,
        "registered_test_rows_inclusive": [0, 999],
        "known_query_proof_sha256": {name: sha(path.read_bytes()) for name, path in proof_paths.items()},
        "exact_archive_git_source_commits_scanned": len(commits),
        "literal_query_source_references": matches,
        "remaining_gate": "Audit indirect prior query inputs and source/vector duplicate identity before sealing GT; then build current CoHere1M generation with source/scorer parity.",
    }
    path = ROOT / "fresh-cohere-source-candidate.json"
    path.write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps({"status": report["query_selection_status"],
                      "rows": report["candidate_query_rows_inclusive"],
                      "shard_sha256": SHARD_SHA, "historical_source_commits": len(commits),
                      "literal_references": len(matches)}))


if __name__ == "__main__":
    main()
