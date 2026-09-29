"""Read CLOSED query-producer metadata and source; never query/GT bodies."""
import concurrent.futures
import hashlib
import io
import json
import re
import tarfile
from pathlib import Path

import boto3

root = Path(__file__).resolve().parent
s3 = boto3.Session(profile_name="causality", region_name="eu-central-1").client("s3")
bucket = "borsuk-bench-453182569524-euc1"
locators = json.loads((root / "v114-v116-source-locators.json").read_text())


def read(key, digest, size=None):
    body = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
    assert hashlib.sha256(body).hexdigest() == digest, key
    assert size is None or len(body) == size, key
    return body


def collect(row):
    terminal = json.loads(read(row["terminal_key"], row["terminal_sha256"]))
    prefix = row["terminal_key"].removesuffix("/terminal.json")
    reservation = next(o for o in row["objects"] if o["key"] == prefix + "/reservation.json")
    binding = json.loads(read(reservation["key"], reservation["sha256"], reservation["bytes"]))
    assert binding["source_commit"] == terminal["source_commit"] == row["source_commit"]
    assert binding["schema"] == terminal["schema"]
    assert terminal["status"] in ("complete", "failed")
    archive = next(o for o in row["objects"] if o["key"].endswith("/source.tar.gz"))
    body = read(archive["key"], binding["archive_sha256"], archive["bytes"])
    campaign = row["terminal_key"].split("/")[1]
    if campaign == "v114-exact-local":
        files = ["launch_v114_exact_local_100k_spot.py", "run_v114_exact_local_100k_remote.sh", "v114_exact_local_100k.py"]
    elif campaign == "v114-1m-paired":
        files = ["launch_v114_1m_paired_spot.py", "launch_v112_precise_nominee_spot.py", "run_v114_1m_paired_remote.sh", "v114_1m_paired.py", "v114_exact_local_100k.py"]
    elif campaign == "v115-returned-replay":
        files = ["launch_v115_returned_replay_spot.py", "run_v115_returned_replay_remote.sh", "v115_compose_replay_requests.py", "validate_v115_router_parity.py"]
    else:
        assert campaign == "v116-validation-paired"
        files = ["launch_v116_validation_paired_spot.py", "run_v116_validation_paired_remote.sh", "v116_validation_paired.py"]
    code = {}
    with tarfile.open(fileobj=io.BytesIO(body), mode="r:gz") as source:
        for filename in files:
            name = "scripts/" + filename
            data = source.extractfile(name).read()
            lines = data.decode().splitlines()
            selected = set()
            for n, line in enumerate(lines):
                if re.search(r"query|queries|request|INPUTS|ARTIFACTS|V36|ObjectIdentity|sha256|prepare|compose", line, re.I):
                    selected.update(range(max(0, n - 2), min(len(lines), n + 3)))
            code[name] = dict(sha256=hashlib.sha256(data).hexdigest(), lines=[dict(line=n + 1, text=lines[n]) for n in sorted(selected)])
    return dict(
        terminal_key=row["terminal_key"], terminal_sha256=row["terminal_sha256"],
        terminal_status=terminal["status"], source_commit=row["source_commit"],
        terminal_archive_sha256=terminal.get("source_archive_sha256"),
        reservation_key=reservation["key"], reservation_sha256=reservation["sha256"],
        archive_key=archive["key"], archive_sha256=binding["archive_sha256"], archive_bytes=archive["bytes"],
        archive_binding="Original same-attempt reservation, matched terminal schema/source commit; terminal does not itself contain archive SHA.",
        query_artifact_identities={k: v for k, v in terminal.get("artifacts", {}).items() if re.search(r"query|queries|request", k, re.I)},
        source_code=code,
    )


with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
    proofs = list(pool.map(collect, locators))
assert len(proofs) == 5
report = dict(metadata_only=True, query_gt_or_sealed_bodies_opened=False, complete_prior_query_audit=False, producer_proofs=proofs)
(root / "v114-v116-query-producers.json").write_text(json.dumps(report, indent=2) + "\n")
for proof in proofs:
    print(json.dumps({k: proof[k] for k in ("terminal_key", "terminal_status", "archive_sha256", "query_artifact_identities")}))
