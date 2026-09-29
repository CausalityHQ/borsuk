"""Authenticate CLOSED CoHere stress-query ranges without vector/GT reads."""
import concurrent.futures
import hashlib
import io
import json
import re
import tarfile
from pathlib import Path

import boto3

root = Path(__file__).resolve().parent.parent
s3 = boto3.Session(profile_name="causality", region_name="eu-central-1").client("s3")
bucket = "borsuk-bench-453182569524-euc1"
rows = json.loads((root / "fresh-history-terminal-inventory.json").read_text())["terminal_metadata"]
selected = [row for row in rows if re.search(r"/v27[3-6]-", row["key"])]
assert len(selected) == 5


def read(key, digest, size=None):
    body = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
    assert hashlib.sha256(body).hexdigest() == digest, key
    assert size is None or len(body) == size, key
    return body


def collect(row):
    terminal = json.loads(read(row["key"], row["sha256"], row["bytes"]))
    assert terminal["status"] in ("complete", "failed")
    assert terminal["source_archive_sha256"] == row["source_archive_sha256"]
    campaign = row["key"].split("/")[1]
    prefix = row["key"].split("/runs/")[0] + "/"
    objects = [o for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix)
               for o in page.get("Contents", []) if o["Key"].endswith(".tar.gz") and row["source_archive_sha256"] in o["Key"]]
    assert len(objects) == 1 and objects[0]["Size"] < 128 * 1024 * 1024
    archive = objects[0]
    data = read(archive["Key"], row["source_archive_sha256"], archive["Size"])
    version = campaign.split("-")[0]
    code = {}
    with tarfile.open(fileobj=io.BytesIO(data), mode="r:gz") as source:
        for member in source.getmembers():
            if member.isfile() and member.name.startswith("scripts/") and (Path(member.name).name.startswith((version + "_", "run_" + version + "_", "launch_" + version + "_")) or member.name in ("scripts/launch_v273_construction_quality_spot.py", "scripts/v273_construction_quality.py")):
                body = source.extractfile(member).read()
                code[member.name] = dict(sha256=hashlib.sha256(body).hexdigest(), lines=[dict(line=n, text=line) for n, line in enumerate(body.decode().splitlines(), 1) if re.search(r"quer|train|source|V261|range|sha256|prepare", line, re.I)])
        runner = next(member for member in source.getmembers() if member.name.startswith("scripts/run_" + version + "_") and member.name.endswith(".sh"))
        text = source.extractfile(runner).read().decode()
    match = re.search(r"--range bytes=(\d+)-(\d+) fresh_queries\.raw[^\n]*\n[^\n]*'([0-9a-f]{64})'", text)
    assert match, version
    first, last = map(int, match.group(1, 2))
    query_sha = match.group(3)
    assert first % 3072 == 0 and last - first + 1 == 1000 * 3072
    assert first // 3072 == 100000 + (int(version[1:]) - 273) * 1000
    metadata = None
    check = None
    if "prepare.json" in terminal["artifacts"]:
        ident = terminal["artifacts"]["prepare.json"]
        metadata = json.loads(read(row["key"].removesuffix("/terminal.json") + "/artifacts/prepare.json", ident["sha256"], ident["bytes"]))
        # Old preparation metadata records only the truth digest. The archived
        # mandatory SHA check and its CLOSED receipt bind the query input.
        check_ident = terminal["artifacts"]["fresh_queries.raw.sha256"]
        check_key = row["key"].removesuffix("/terminal.json") + "/artifacts/fresh_queries.raw.sha256"
        check_body = read(check_key, check_ident["sha256"], check_ident["bytes"])
        assert check_body == b"fresh_queries.raw: OK\n"
        check = dict(identity=check_ident, text=check_body.decode(), expected_query_sha256=query_sha)
    return dict(terminal_key=row["key"], terminal_sha256=row["sha256"], terminal_status=terminal["status"],
                archive_key=archive["Key"], archive_sha256=row["source_archive_sha256"],
                query_source_byte_range=[first, last], query_source_rows_inclusive=[first // 3072, last // 3072],
                query_count=1000, query_sha256=query_sha, preparation_metadata=metadata,
                preparation_identity=terminal["artifacts"].get("prepare.json"), query_input_check=check, source_code=code,
                executed_preparation_authenticated=metadata is not None,
                exclusion="Conservatively exclude whole producer range, including failed attempts.")


parent = next(row for row in rows if "/v261-cohere-dual-graph-1m/" in row["key"])
parent_body = read(parent["key"], parent["sha256"], parent["bytes"])
parent_terminal = json.loads(parent_body)
source = parent_terminal["artifacts"]["vectors.raw"]
assert parent_terminal["status"] == "complete" and source == dict(bytes=1000000 * 768 * 4, sha256="6c82a340e3e1b4226640e593efa9c4000c6a5962d4b13063093a1dab689a9005")
with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
    proofs = list(pool.map(collect, selected))
report = dict(metadata_only=True, query_gt_or_sealed_bodies_opened=False, complete_prior_query_audit=False,
              dataset="CoHere D768", source_terminal_key=parent["key"], source_terminal_sha256=parent["sha256"], source_raw_identity=source,
              excluded_raw_source_rows_inclusive=[100000, 103999], additional_rows_beyond_previously_named_v273=3000,
              producer_proofs=proofs, qualification=False)
(root / "fresh-cohere-stress-query-families.json").write_text(json.dumps(report, indent=2) + "\n")
for proof in proofs:
    print(json.dumps({k: proof[k] for k in ("terminal_status", "query_source_rows_inclusive", "query_sha256", "executed_preparation_authenticated")}))
