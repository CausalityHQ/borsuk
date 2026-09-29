"""Supplement frozen terminal inventory with nested archive/input identities."""
import concurrent.futures
import hashlib
import json
from collections import Counter
from pathlib import Path

import boto3

root = Path(__file__).resolve().parent.parent
s3 = boto3.Session(profile_name="causality", region_name="eu-central-1").client("s3")
rows = []
for filename in ("fresh-history-terminal-inventory.json", "fresh-history-nonversioned-terminal-inventory.json"):
    rows.extend(json.loads((root / filename).read_text())["terminal_metadata"])
assert len(rows) == len({row["key"] for row in rows}) == 697


def collect(row):
    body = s3.get_object(Bucket="borsuk-bench-453182569524-euc1", Key=row["key"])["Body"].read()
    assert len(body) == row["bytes"] and hashlib.sha256(body).hexdigest() == row["sha256"]
    value = json.loads(body)
    archive = value.get("source_archive")
    direct = value.get("source_archive_sha256")
    if isinstance(archive, dict):
        assert direct is None or direct == archive["sha256"]
        binding = "terminal-nested"
    elif direct:
        archive = dict(sha256=direct)
        binding = "terminal-sha-only"
    else:
        archive = None
        binding = "not-in-terminal"
    return dict(
        terminal_key=row["key"], terminal_sha256=row["sha256"],
        source_commit=value.get("source_commit", value.get("source_base_commit")),
        source_archive=archive, archive_binding=binding,
        input_metadata={k: v for k, v in value.items() if k in ("inputs", "input_objects", "source", "queries", "source_identity", "execution_authority_sha256", "freeze_authority_sha256")},
    )


with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
    bindings = list(pool.map(collect, rows))
counts = dict(Counter(row["archive_binding"] for row in bindings))
report = dict(metadata_only=True, query_gt_or_sealed_bodies_opened=False, complete_prior_query_audit=False, terminal_count=len(bindings), binding_counts=counts, bindings=bindings)
(root / "fresh-history-source-bindings.json").write_text(json.dumps(report, indent=2) + "\n")
print(json.dumps(dict(terminal_count=len(bindings), binding_counts=counts, terminals_with_input_metadata=sum(bool(row["input_metadata"]) for row in bindings))))
