"""Authenticate pre-Git-archive historical producers using CLOSED terminals."""
import concurrent.futures
import hashlib
import io
import json
import re
import tarfile
from pathlib import Path

import boto3

root = Path(__file__).resolve().parent
reconstruction = json.loads((root.parent / "fresh-history-archive-reconstruction.json").read_text())["proofs"]
inventory = {x["key"]: x for x in json.loads((root.parent / "fresh-history-terminal-inventory.json").read_text())["terminal_metadata"]}
selected = [p for p in reconstruction if not p["exact_archive_match"] and any(
    (m := re.search(r"/v(\d+)-", key)) and 130 <= int(m.group(1)) <= 155
    for key in p["terminal_keys"])]
assert len(selected) == 28
s3 = boto3.Session(profile_name="causality", region_name="eu-central-1").client("s3")
bucket = "borsuk-bench-453182569524-euc1"


def collect(item):
    terminal_key = item["terminal_keys"][0]
    terminal = inventory[terminal_key]
    campaign = terminal_key.split("/")[1]
    version = campaign.split("-")[0]
    expected = item["expected_archive_sha256"]
    assert terminal["source_archive_sha256"] == expected and terminal["source_commit"] == item["source_commit"]
    existing = root / (campaign + "-" + item["source_commit"][:8] + ".json")
    if existing.exists():
        previous = json.loads(existing.read_text())
        if previous.get("archive_sha256") == expected and previous.get("terminal_sha256") == terminal["sha256"]:
            return dict(terminal_key=terminal_key, proof=str(existing.relative_to(root.parent)), already_authenticated=True)
    run_prefix = terminal_key.removesuffix("/terminal.json") + "/"
    branch_prefix = terminal_key.split("/runs/")[0] + "/"
    candidates = []
    for prefix in (run_prefix, branch_prefix + "source/", branch_prefix + "sources/"):
        for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix):
            candidates.extend(o for o in page.get("Contents", []) if o["Key"].endswith(".tar.gz") and o["Size"] < 128 * 1024 * 1024)
    candidates = list({o["Key"]: o for o in candidates}.values())
    candidates.sort(key=lambda o: (expected not in o["Key"], not o["Key"].startswith(run_prefix)))
    assert candidates and len(candidates) <= 12, terminal_key
    matching = None
    for o in candidates:
        body = s3.get_object(Bucket=bucket, Key=o["Key"])["Body"].read()
        if hashlib.sha256(body).hexdigest() == expected:
            matching = (o, body)
            break
    assert matching is not None, terminal_key
    archive, body = matching
    code = {}
    with tarfile.open(fileobj=io.BytesIO(body), mode="r:gz") as source:
        for member in source.getmembers():
            name = member.name.removeprefix("./")
            if (member.isfile() and name.startswith("scripts/") and name.endswith((".py", ".sh"))
                    and Path(name).name.startswith((version + "_", "launch_" + version + "_", "run_" + version + "_"))):
                data = source.extractfile(member).read()
                code[name] = dict(sha256=hashlib.sha256(data).hexdigest(), lines=[dict(line=n, text=line)
                    for n, line in enumerate(data.decode().splitlines(), 1)
                    if re.search(r"query|queries|request|V36|V114|V116|source|test\.parquet|deep-image|relaion", line, re.I)])
    assert code, terminal_key
    proof = dict(metadata_only=True, query_gt_or_sealed_bodies_opened=False, complete_prior_query_audit=False,
                 terminal_key=terminal_key, terminal_sha256=terminal["sha256"], terminal_status=terminal["status"],
                 source_commit=item["source_commit"], archive_key=archive["Key"], archive_sha256=expected,
                 archive_bytes=archive["Size"], source_code=code)
    if existing.exists():
        # Original proof files remain immutable. Put additional source binds elsewhere.
        path = root / (campaign + "-" + item["source_commit"][:8] + "-additional-" + expected[:8] + ".json")
    else:
        path = existing
    path.write_text(json.dumps(proof, indent=2) + "\n")
    return dict(terminal_key=terminal_key, proof=str(path.relative_to(root.parent)), source_code_files=len(code), already_authenticated=False)


with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
    results = list(pool.map(collect, selected))
assert len(results) == len(selected)
(root / "v130-v155-collection-status.json").write_text(json.dumps(results, indent=2) + "\n")
for row in results:
    print(json.dumps(row), flush=True)
