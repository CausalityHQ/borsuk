"""Bind old formatted ReLAION queries to their original development bank."""
import ast
import hashlib
import io
import json
import subprocess
import tarfile
from pathlib import Path

import boto3

root = Path(__file__).resolve().parent.parent
s3 = boto3.Session(profile_name="causality", region_name="eu-central-1").client("s3")
bucket = "borsuk-bench-453182569524-euc1"
commit = "fb976932ecd4076e2f76a7cb7e7aa7efe01e9a2d"
prefix = "research/v85-competitive-rescore/" + commit + "/runs/v85-100k-dev1000-20260920T094401Z-fb976932/a0001"
terminal_key = prefix + "/terminal.json"
terminal_body = s3.get_object(Bucket=bucket, Key=terminal_key)["Body"].read()
assert hashlib.sha256(terminal_body).hexdigest() == "ea50eb119407b7b235bf47c7f096b47937c69570920935b5c2c15c98db277b19"
terminal = json.loads(terminal_body)
assert terminal["source_commit"] == commit and terminal["exit_code"] == 104
archive_key = prefix + "/source/source.tar.zst"
archive = s3.get_object(Bucket=bucket, Key=archive_key)["Body"].read()
assert len(archive) == 9660806
raw = subprocess.run(["zstd", "--decompress", "--stdout", "--quiet"], input=archive, check=True, capture_output=True).stdout
code = {}
with tarfile.open(fileobj=io.BytesIO(raw), mode="r:") as source:
    for name in ("scripts/v85_pq16_100k_rescore_run_remote.sh", "scripts/v85_build_delta.py"):
        body = source.extractfile(name).read()
        git_body = subprocess.run(["git", "show", commit + ":" + name], check=True, capture_output=True).stdout
        assert body == git_body, name
        code[name] = body
script = code["scripts/v85_pq16_100k_rescore_run_remote.sh"].decode()
assert '310bb54f79f2e79d09fe63aa4f6b5c6e9e7ffb31101964f816be978dadb2db54  query-source.parquet' in script
assert '"$evaluation_prefix/development-query.parquet" query-source.parquet' in script
assert 'Path("query-source.parquet"), Path("truth-source.parquet")' in script
assert 'dimensions=768, neighbors=100, query_limit=1_000' in script
assert 'aws s3 cp queries.parquet "$queries_uri"' in script
helper = code["scripts/v85_build_delta.py"].decode()
node = next(n for n in ast.parse(helper).body if isinstance(n, ast.FunctionDef) and n.name == "canonicalize_evaluation")
function = "\n".join(helper.splitlines()[node.lineno - 1:node.end_lineno])
hash_key = prefix + "/evidence/hashes.log"
checks = s3.get_object(Bucket=bucket, Key=hash_key)["Body"].read()
assert len(checks) == 147 and b"source.tar.zst: OK\n" in checks and b"query-source.parquet: OK\n" in checks
v114 = next(p for p in json.loads((root / "fresh-history-source-inventory/v114-v116-query-producers.json").read_text())["producer_proofs"] if p["terminal_status"] == "complete" and "/v114-exact-local/" in p["terminal_key"])
reader = v114["source_code"]["scripts/v114_exact_local_100k.py"]
query_sha = "4834cf63a50971b7d605c00f91b5142f67b049e91ea2c62c220271b50bffa6ac"
assert any(query_sha in line["text"] for line in reader["lines"])
report = dict(
    metadata_only=True, query_gt_or_sealed_bodies_opened=False, complete_prior_query_audit=False,
    terminal_key=terminal_key, terminal_sha256=hashlib.sha256(terminal_body).hexdigest(), terminal=terminal,
    archive_key=archive_key, archive_sha256=hashlib.sha256(archive).hexdigest(), archive_bytes=len(archive),
    archive_binding="Original same-attempt archived input; relevant source files exactly match terminal's declared Git commit. CLOSED input-check log confirms archive/query input checks; terminal does not record archive digest.",
    checks=dict(key=hash_key, sha256=hashlib.sha256(checks).hexdigest(), text=checks.decode()),
    original_query_bank_sha256="310bb54f79f2e79d09fe63aa4f6b5c6e9e7ffb31101964f816be978dadb2db54",
    formatted_query_sha256=query_sha, query_count=1000, dimensions=768,
    downstream_closed_v114_terminal_key=v114["terminal_key"], downstream_closed_v114_terminal_sha256=v114["terminal_sha256"],
    source_code={name: dict(sha256=hashlib.sha256(body).hexdigest(), git_commit_match=True) for name, body in code.items()},
    producer_function=dict(line=node.lineno, text=function),
    runner_bank_lines=[dict(line=n, text=line) for n, line in enumerate(script.splitlines(), 1) if 65 <= n <= 106],
    decision="Same original development1000 bank after schema conversion; exclude its whole feature-ID bank. No fresh cohort or quality claim.",
)
(root / "fresh-relaion-development-format-family.json").write_text(json.dumps(report, indent=2) + "\n")
print(json.dumps(dict(original_bank=report["original_query_bank_sha256"], formatted_bank=query_sha, relevant_archived_files_match_terminal_commit=True, terminal_phase=terminal["phase"])))
