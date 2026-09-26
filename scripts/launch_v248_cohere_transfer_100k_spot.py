#!/usr/bin/env python3
"""One immutable CoHere-100k source-only graph transfer gate on Causality Spot."""

import argparse
import base64
import fcntl
import hashlib
import json
import subprocess
import time

import boto3
from botocore.exceptions import ClientError

from scripts.launch_v157_primary_feasibility_spot import (
    BUCKET, PROFILE_ARN, REGION, SECURITY_GROUP, SUBNET,
    archive_source, missing, put_if_absent,
)

SCHEMA = "borsuk-v248-cohere-transfer-100k-spot-v1"
PQ_SCHEMA = "borsuk-v249-cohere-pq-aligned-100k-spot-v1"
DIVERSE_SCHEMA = "borsuk-v250-cohere-diverse-100k-spot-v1"
EXACT_SCHEMA = "borsuk-v251-cohere-fp16-navigation-100k-spot-v1"
ANCHOR_SCHEMA = "borsuk-v252-cohere-strided-anchor-100k-spot-v1"
GLOBAL_SCHEMA = "borsuk-v253-cohere-global-pq-100k-spot-v1"
COARSE_SCHEMA = "borsuk-v254-cohere-coarse-pq-100k-spot-v1"
MILLION_SCHEMA = "borsuk-v255-cohere-diverse-graph-1m-spot-v1"
HYBRID_SCHEMA = "borsuk-v256-cohere-hybrid-100k-spot-v1"
MILLION_HYBRID_SCHEMA = "borsuk-v257-cohere-hybrid-1m-spot-v1"
MILLION_EXACT_SCHEMA = "borsuk-v258-cohere-fp16-navigation-1m-spot-v1"
DUAL_SCHEMA = "borsuk-v259-cohere-dual-navigation-100k-spot-v1"
IMAGE = "ami-06121aa3085b6f918"
WALL_SECONDS = 10_800
SOURCE = "publication/v3/20260812/datasets/cohere-large-10m-768/attempts/0001/"
RECEIPT_SHA = "0965aa0241199822dfac3410bba4edad5536ac0eb0aaa8ab83c216e8c5749a87"
TEST_SHA = "5e0123f163df0e53a7e329fd92fbfd49f079756acfb47387ee6664c267b6f94e"
PARENT_PREFIX = ("research/v248-cohere-transfer-100k/"
                 "7b66f9f7d306a0f1febc2091783de68a6ece72c7/runs/a0001")
PARENT_TERMINAL_SHA = "9e45f55216518fec13bba3c40002aedb8d356ce8fe1fa943d6ac4caf9ac817f6"
V249_PREFIX = ("research/v249-cohere-pq-aligned-100k/"
               "6b7884a6a24e6467d82c9ced1a00ca3706cf0aca/runs/a0001")
V249_TERMINAL_SHA = "1d51817ca1556c011fad29bc0476a9b36103fb60e5b9d6a632766d48fa7bc280"
V250_PREFIX = ("research/v250-cohere-diverse-100k/"
               "34a38d6427405ade5931e0cfc8e2f19e5436a6d8/runs/a0001")
V250_TERMINAL_SHA = "811dfda87424380926c359d8d0c02c3db153986d7f48a31b249c2ea0316fa689"
V251_PREFIX = ("research/v251-cohere-fp16-navigation-100k/"
               "0168b667c32907ce2dd62e3648d03f220bc98479/runs/a0001")
V251_TERMINAL_SHA = "a92bd99b3c5739819894bed011ad9b766914476892c64b2137987f1f9f940d65"
V252_PREFIX = ("research/v252-cohere-strided-anchor-100k/"
               "136e6ed61e64a5a9ee8b18ab86fd8637b3f2d771/runs/a0001")
V252_TERMINAL_SHA = "3e87061f17a06266244e3192cd5849a013ff86eef1cd34753a216ec63ee43294"
V253_PREFIX = ("research/v253-cohere-global-pq-100k/"
               "bd14239fbc22156fc7fe53bfbe0e9a7bdcb6879a/runs/a0001")
V253_TERMINAL_SHA = "eaed2fd2640846cc7326616030eec0843314a3b0b926f01d83c54a76492b7d2d"
V254_PREFIX = ("research/v254-cohere-coarse-pq-100k/"
               "a7def2219cb4cc402637e8bbfd905d1d300840f2/runs/a0001")
V254_TERMINAL_SHA = "97a91fd937d61f804e17fcce047685e9c11df12c07ffbb2077000ec3519ec16e"
V255_PREFIX = ("research/v255-cohere-diverse-graph-1m/"
               "68c35e24a648e2036c3d7f568a3b5d6a27fcaa18/runs/a0001")
V255_TERMINAL_SHA = "253f787ba8fe6efa4334c259d55ecaae557d1f8e302063291e0e108aefe1e99b"
V256_PREFIX = ("research/v256-cohere-hybrid-100k/"
               "1ba78eaf8322ad47b9553ede6f1d9f8848277678/runs/a0001")
V256_TERMINAL_SHA = "b4ce612b6bf3a4b636b23586fd795c7d418dd669adf3078d9f3e4eedcc3243f6"
V257_PREFIX = ("research/v257-cohere-hybrid-1m/"
               "80f9d35c1283e0161983e578b3d779c58aebd62e/runs/a0001")
V257_TERMINAL_SHA = "e1cef23d26f9d3fb96276541084de06ec6ba09bd6b75206267518fc958530f06"
ARTIFACTS = ("prep.json", "build.json", "serving.json", "quality.json",
             "requests.json", "requests.jsonl", "raw.jsonl", "truth.u32", "graph.bin",
             "vectors.raw", "plane.bin", "books.bin", "codes.bin", "map.u32",
             "prep-resources.txt", "graph-resources.txt", "serving-resources.txt",
             "truth-resources.txt", "build.log", "install.log", "run-closed.log")
COARSE_ARTIFACTS = ("centroids.f32", "offsets.u32", "postings.u32",
                    "coarse.json", "coarse-resources.txt")
HYBRID_ARTIFACTS = COARSE_ARTIFACTS + ("loaded-raw.jsonl",)


def worker(commit, archive_sha, archive_key, prefix, pq_topology=False,
           diverse=False, exact_nav=False, anchored=False, global_pq=False,
           coarse_pq=False, million=False, hybrid=False, million_hybrid=False,
           reuse_v257=False, million_exact=False, dual=False):
    big = million or million_hybrid or million_exact
    hybrid_mode = hybrid or million_hybrid or dual
    script = r'''#!/bin/bash
set -euo pipefail
systemd-run --unit=v248-hard-stop --on-active=10800s /usr/sbin/shutdown -h now
root=/mnt/v248-cohere-100k
mkdir -p "$root" && cd "$root"
phase=bootstrap
finish() {
  code=$?
  trap - EXIT TERM
  set +e
  cd "$root"
  cp run.log run-closed.log || code=96
  for name in @@ARTIFACTS@@; do
    [ ! -f "$name" ] || aws s3 cp "$name" "s3://@@BUCKET@@/@@PREFIX@@/artifacts/$name" --only-show-errors || code=96
  done
  token=$(curl -fsS -X PUT -H 'X-aws-ec2-metadata-token-ttl-seconds: 60' http://169.254.169.254/latest/api/token)
  instance_id=$(curl -fsS -H "X-aws-ec2-metadata-token: $token" http://169.254.169.254/latest/meta-data/instance-id)
  INSTANCE_ID="$instance_id" EXIT_CODE="$code" PHASE="$phase" python3 - <<'PY' >terminal.json
import hashlib,json,os
from pathlib import Path
artifacts={}
for name in @@ARTIFACTS_PY@@:
    path=Path(name)
    if path.is_file():
        value=hashlib.sha256()
        with path.open('rb') as source:
            for block in iter(lambda:source.read(1024*1024),b''):
                value.update(block)
        artifacts[name]={'bytes':path.stat().st_size,
                         'sha256':value.hexdigest()}
code=int(os.environ['EXIT_CODE']);phase=os.environ['PHASE']
print(json.dumps({'schema':'@@SCHEMA@@','source_commit':'@@COMMIT@@',
    'source_archive_sha256':'@@ARCHIVE_SHA@@','instance_id':os.environ['INSTANCE_ID'],
    'exit_code':code,'phase':phase,
    'status':'complete' if code==0 and phase=='complete' else 'failed',
    'artifacts':artifacts},sort_keys=True,separators=(',',':')))
PY
  aws s3 cp terminal.json "s3://@@BUCKET@@/@@PREFIX@@/terminal.json" --only-show-errors
  shutdown -h now || true
  exit "$code"
}
trap finish EXIT
trap 'exit 97' TERM
exec >run.log 2>&1
aws s3 cp 's3://@@BUCKET@@/@@ARCHIVE_KEY@@' source.tar.gz --only-show-errors
printf '%s  source.tar.gz\n' '@@ARCHIVE_SHA@@' | sha256sum -c -
mkdir repo && tar -xzf source.tar.gz -C repo
phase=install
dnf install -y -q python3.12 python3.12-pip gcc gcc-c++ cmake perl tar gzip time >install.log 2>&1
python3.12 -m venv .venv
.venv/bin/pip install -q numpy==2.3.3 pyarrow==24.0.0 >>install.log 2>&1
export RUSTUP_HOME="$root/.rustup" CARGO_HOME="$root/.cargo" CARGO_TARGET_DIR="$root/target" CARGO_BUILD_JOBS=6
curl -fsSL https://sh.rustup.rs | sh -s -- -y --profile minimal --default-toolchain 1.98.0 >>install.log 2>&1
export PYTHONPATH="$root/repo" OPENBLAS_NUM_THREADS=8 OMP_NUM_THREADS=8 MKL_NUM_THREADS=8
if [ '@@REUSE_V257@@' = 1 ]; then
  phase=reuse
  aws s3 cp 's3://@@BUCKET@@/@@REUSE_PREFIX@@/terminal.json' prior-terminal.json --only-show-errors
  printf '%s  prior-terminal.json\n' '@@REUSE_TERMINAL_SHA@@' | sha256sum -c -
  for name in prep.json build.json vectors.raw plane.bin books.bin codes.bin map.u32 \
              graph.bin centroids.f32 offsets.u32 postings.u32 coarse.json \
              requests.json requests.jsonl prep-resources.txt graph-resources.txt \
              coarse-resources.txt; do
    aws s3 cp "s3://@@BUCKET@@/@@REUSE_PREFIX@@/artifacts/$name" "$name" --only-show-errors
  done
  python3 - <<'PY'
import hashlib,json
from pathlib import Path
prior=json.loads(Path('prior-terminal.json').read_text())
assert prior['status']=='failed' and prior['phase']=='serve'
for name in ('prep.json','build.json','vectors.raw','plane.bin','books.bin',
             'codes.bin','map.u32','graph.bin','centroids.f32','offsets.u32',
             'postings.u32','coarse.json','requests.json','requests.jsonl',
             'prep-resources.txt','graph-resources.txt','coarse-resources.txt'):
    path=Path(name); h=hashlib.sha256()
    with path.open('rb') as source:
        for block in iter(lambda:source.read(1024*1024),b''):
            h.update(block)
    assert path.stat().st_size==prior['artifacts'][name]['bytes']
    assert h.hexdigest()==prior['artifacts'][name]['sha256']
PY
else
phase=inputs
aws s3 cp 's3://@@BUCKET@@/@@SOURCE@@/STAGING_COMPLETE.json' receipt.json --only-show-errors
printf '%s  receipt.json\n' '@@RECEIPT_SHA@@' | sha256sum -c -
for n in @@SHARDS@@; do
  name=$(printf 'train-%08d.parquet' "$n")
  aws s3 cp "s3://@@BUCKET@@/@@SOURCE@@/materialized/$name" "$name" --only-show-errors
done
train_args=()
for n in @@SHARDS@@; do
  train_args+=("$(printf 'train-%08d.parquet' "$n")")
done
phase=prepare
/usr/bin/time -v -o prep-resources.txt .venv/bin/python -m scripts.v248_prepare_cohere_graph \
  --receipt receipt.json --receipt-sha256 '@@RECEIPT_SHA@@' \
  --train "${train_args[@]}" --rows @@ROWS@@ --generation @@GENERATION@@ --output prepared
cp prepared/* .
@@MATCHED_PREP@@
@@COARSE_PREP@@
fi
phase=compile
cd repo
"$CARGO_HOME/bin/cargo" build --release --locked -p borsuk \
  --bin v248_build_cohere_graph_100k --bin v248_serve_cohere_graph_100k \
  --jobs 6 >"$root/build.log" 2>&1
@@EXACT_TEST@@
cd "$root"
if [ '@@REUSE_V257@@' != 1 ]; then
phase=graph
/usr/bin/time -v -o graph-resources.txt "$CARGO_TARGET_DIR/release/v248_build_cohere_graph_100k" \
  prep.json plane.bin vectors.raw graph.bin build.json @@PQ_ARGS@@
@@MATCHED_GRAPH@@
phase=queries
aws s3 cp 's3://@@BUCKET@@/@@SOURCE@@/materialized/test.parquet' test.parquet --only-show-errors
printf '%s  test.parquet\n' '@@TEST_SHA@@' | sha256sum -c -
.venv/bin/python -m scripts.v248_score_cohere_graph requests \
  --test test.parquet --request-file requests.jsonl --output requests.json
@@MATCHED_REQUESTS@@
fi
phase=serve
/usr/bin/time -v -o serving-resources.txt "$CARGO_TARGET_DIR/release/v248_serve_cohere_graph_100k" \
  prep.json build.json plane.bin graph.bin map.u32 books.bin codes.bin \
  requests.jsonl raw.jsonl serving.json @@SERVE_ARGS@@
aws s3api put-object --bucket '@@BUCKET@@' --key '@@PREFIX@@/sealed/raw.jsonl' \
  --body raw.jsonl --if-none-match '*' --no-cli-pager >/dev/null
raw_sha=$(sha256sum raw.jsonl | cut -d ' ' -f1)
remote_sha=$(aws s3 cp 's3://@@BUCKET@@/@@PREFIX@@/sealed/raw.jsonl' - \
  --only-show-errors | sha256sum | cut -d ' ' -f1)
[ "$raw_sha" = "$remote_sha" ]
phase=truth
/usr/bin/time -v -o truth-resources.txt .venv/bin/python -m scripts.v248_score_cohere_graph score \
  --source vectors.raw --prep prep.json --request-file requests.jsonl \
  --raw-file raw.jsonl --serving-file serving.json --truth-output truth.u32 \
  --output quality.json
@@MATCHED_TRUTH@@
phase=complete
'''
    matched_prep = '''printf '%s  %s\\n' \\
  '0f3631d71c105e5ea3d701c96033b362c2f84bd43002a9c8a5c70040801be06e' vectors.raw \\
  '4e164997e53e2e59c0930403386de863ceb17eaa000cd904b699370eca0d9de7' plane.bin \\
  'd6d36b22f66ecb5cfc750c117392ab1c0f264dc5bac3e9785cdab92dc6a75b19' books.bin \\
  'a645cf66c8fd92d1f248438807f97cfd676ca8fb4233efd23d7adc299ff5e9e3' codes.bin \\
  '20ff50e632cc575386b15d7fcd9c3842ef435388ed29ae8c30617158ee907dc5' map.u32 | sha256sum -c -'''
    matched_million_prep = '''printf '%s  %s\\n' \\
  '6c82a340e3e1b4226640e593efa9c4000c6a5962d4b13063093a1dab689a9005' vectors.raw \\
  '223879129b10559fbb1ca122b624fc0f058d4a4c2a98159046bc8b72cd96ee00' plane.bin \\
  '13ed1ebcb37e132da2eb58dae957b4cfceff8ea018ce25e7da24e1ea58c938b9' books.bin \\
  'b019a7ab11ee0a378ea91a54dac3b5d557231d218e14c1800d62f9325b7c966e' codes.bin \\
  '02e21fa3c89fa7d7b61826918a8bd35d3127827b4ef3f3ee47ade5e64e3c2a80' map.u32 | sha256sum -c -'''
    artifacts = ARTIFACTS + HYBRID_ARTIFACTS if hybrid_mode else ARTIFACTS + ("loaded-raw.jsonl",) if million_exact else ARTIFACTS + COARSE_ARTIFACTS if coarse_pq else ARTIFACTS
    coarse_prep = '''phase=coarse
/usr/bin/time -v -o coarse-resources.txt env OPENBLAS_NUM_THREADS=1 OMP_NUM_THREADS=1 MKL_NUM_THREADS=1 \\
  .venv/bin/python -m scripts.v254_prepare_coarse_pq --source vectors.raw \\
  --prep prep.json --codes codes.bin --output coarse
cp coarse/* .'''
    for key, value in {"ARTIFACTS": " ".join(artifacts), "ARTIFACTS_PY": repr(artifacts),
                       "SCHEMA": DUAL_SCHEMA if dual else MILLION_EXACT_SCHEMA if million_exact else MILLION_HYBRID_SCHEMA if million_hybrid else HYBRID_SCHEMA if hybrid else MILLION_SCHEMA if million else COARSE_SCHEMA if coarse_pq else GLOBAL_SCHEMA if global_pq else ANCHOR_SCHEMA if anchored else EXACT_SCHEMA if exact_nav else DIVERSE_SCHEMA if diverse else PQ_SCHEMA if pq_topology else SCHEMA,
                       "BUCKET": BUCKET, "PREFIX": prefix,
                       "COMMIT": commit, "ARCHIVE_SHA": archive_sha,
                       "ARCHIVE_KEY": archive_key, "SOURCE": SOURCE.rstrip("/"),
                       "RECEIPT_SHA": RECEIPT_SHA, "TEST_SHA": TEST_SHA,
                       "REUSE_V257": "1" if reuse_v257 else "0",
                       "REUSE_PREFIX": V257_PREFIX,
                       "REUSE_TERMINAL_SHA": V257_TERMINAL_SHA,
                       "SHARDS": "$(seq 0 45)" if big else "0 1 2 3 4",
                       "ROWS": "1000000" if big else "100000",
                       "GENERATION": "255" if big else "248",
                       "PQ_ARGS": "--diverse" if diverse or exact_nav or anchored or global_pq or coarse_pq or big or hybrid_mode else "--pq-topology books.bin codes.bin" if pq_topology else "",
                       "SERVE_ARGS": "--million-exact" if million_exact else "--dual centroids.f32 offsets.u32 postings.u32 coarse.json" if dual else "--hybrid centroids.f32 offsets.u32 postings.u32 coarse.json" if hybrid_mode else "--coarse-pq centroids.f32 offsets.u32 postings.u32 coarse.json" if coarse_pq else "--global-pq" if global_pq else "--strided-anchors" if anchored else "--exact-nav" if exact_nav else "",
                       "EXACT_TEST": ('"$CARGO_HOME/bin/cargo" test --release --locked -p borsuk --bin v248_serve_cohere_graph_100k '
                                      'tests::million_row_hybrid_accepts_only_the_diverse_million_graph -- --exact >>"$root/build.log" 2>&1\n'
                                      if reuse_v257 else '') + ('"$CARGO_HOME/bin/cargo" test --release --locked -p borsuk --lib '
                                      + ('pq64_nominee::tests::cosine_view_uses_reconstructed_direction_instead_of_squared_l2' if global_pq or coarse_pq or hybrid_mode
                                         else 'resident_vector_graph::tests::graph_returns_stable_ids_and_rejects_generation_mismatch')
                                      + ' -- --exact >>"$root/build.log" 2>&1' if exact_nav or million_exact or anchored or global_pq or coarse_pq or big or hybrid_mode else ""),
                       "COARSE_PREP": coarse_prep if coarse_pq or hybrid_mode else "",
                       "MATCHED_PREP": matched_million_prep if million_hybrid or million_exact else matched_prep if not big and (pq_topology or diverse or exact_nav or anchored or global_pq or coarse_pq or hybrid_mode) else "",
                       "MATCHED_GRAPH": ("printf '%s  graph.bin\\n' '688941c7c61c89a39739909af14cb2b4a935a7a4967a168f9503ac34e170d0e7' | sha256sum -c -" if not big and (exact_nav or anchored or global_pq or coarse_pq or hybrid_mode) else "printf '%s  graph.bin\\n' 'dff17235c4b674848d52ae11260445f97fd399039e18119f922aaa82d77309a2' | sha256sum -c -" if million_hybrid or million_exact else ""),
                       "MATCHED_REQUESTS": ("printf '%s  requests.jsonl\\n' '86d9406486a2bb27aa2e603f019e078dd3ecaed47f79ec685558ba3536433812' | sha256sum -c -" if not big and (pq_topology or diverse or exact_nav or anchored or global_pq or coarse_pq or hybrid_mode) or million_hybrid or million_exact else ""),
                       "MATCHED_TRUTH": ("printf '%s  truth.u32\\n' '06cd59b31962d4190367b54d7abf24dd4e018d3c4ac8da0b2b528d21a5a7cbb8' | sha256sum -c -" if not big and (pq_topology or diverse or exact_nav or anchored or global_pq or coarse_pq or hybrid_mode) else "printf '%s  truth.u32\\n' '62e14eba043fafb8d8ec7c833d7d320c5d823c549683d15e5eacdff365a87f39' | sha256sum -c -" if million_hybrid or million_exact else "")}.items():
        script = script.replace("@@" + key + "@@", value)
    if "@@" in script:
        raise ValueError("unresolved V248 worker placeholder")
    return script


def launch(attempt, pq_topology=False, diverse=False, exact_nav=False, anchored=False,
           global_pq=False, coarse_pq=False, million=False, hybrid=False,
           million_hybrid=False, reuse_v257=False, million_exact=False,
           dual=False):
    if sum((pq_topology, diverse, exact_nav, anchored, global_pq, coarse_pq,
            million, hybrid, million_hybrid, million_exact, dual)) > 1:
        raise ValueError("select one graph treatment")
    if reuse_v257 and not ((million_hybrid and attempt == "a0002") or
                           (million_exact and attempt == "a0001")):
        raise ValueError("V257 reuse is only the V257 repair or V258 a0001")
    if million_exact and not reuse_v257:
        raise ValueError("V258 must reuse the closed V257 source graph")
    big = million or million_hybrid or million_exact
    hybrid_mode = hybrid or million_hybrid or dual
    if len(attempt) != 5 or not attempt.startswith("a") or not attempt[1:].isdigit():
        raise ValueError("attempt must be aNNNN")
    if subprocess.check_output(["git", "status", "--porcelain"], text=True).strip():
        raise ValueError("worktree must be clean")
    commit = subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip()
    subprocess.run(["git", "fetch", "origin", "main"], check=True)
    if subprocess.run(["git", "merge-base", "--is-ancestor", "origin/main", "HEAD"],
                      check=False).returncode:
        raise ValueError("source is not a fast-forward descendant of origin/main")
    archive = archive_source(commit)
    archive_sha = hashlib.sha256(archive).hexdigest()
    campaign = ("v259-cohere-dual-navigation-100k" if dual else
                "v258-cohere-exact-navigation-1m" if million_exact else
                "v257-cohere-hybrid-1m" if million_hybrid else
                "v256-cohere-hybrid-100k" if hybrid else
                "v255-cohere-diverse-graph-1m" if million else
                "v254-cohere-coarse-pq-100k" if coarse_pq else
                "v253-cohere-global-pq-100k" if global_pq else
                "v252-cohere-strided-anchor-100k" if anchored else
                "v251-cohere-fp16-navigation-100k" if exact_nav else
                "v250-cohere-diverse-100k" if diverse else
                "v249-cohere-pq-aligned-100k" if pq_topology else
                "v248-cohere-transfer-100k")
    schema = DUAL_SCHEMA if dual else MILLION_EXACT_SCHEMA if million_exact else MILLION_HYBRID_SCHEMA if million_hybrid else HYBRID_SCHEMA if hybrid else MILLION_SCHEMA if million else COARSE_SCHEMA if coarse_pq else GLOBAL_SCHEMA if global_pq else ANCHOR_SCHEMA if anchored else EXACT_SCHEMA if exact_nav else DIVERSE_SCHEMA if diverse else PQ_SCHEMA if pq_topology else SCHEMA
    archive_key = f"research/{campaign}/{commit}/sources/{archive_sha}.tar.gz"
    prefix = f"research/{campaign}/{commit}/runs/{attempt}"
    session = boto3.Session(profile_name="causality", region_name=REGION)
    ec2, s3 = session.client("ec2"), session.client("s3")
    if not big and (pq_topology or diverse or exact_nav or anchored or global_pq or coarse_pq or hybrid_mode):
        parent_raw = s3.get_object(Bucket=BUCKET,
                                   Key=PARENT_PREFIX + "/terminal.json")["Body"].read()
        parent = json.loads(parent_raw)
        if (hashlib.sha256(parent_raw).hexdigest() != PARENT_TERMINAL_SHA
                or parent.get("status") != "complete"
                or parent.get("source_commit")
                != "7b66f9f7d306a0f1febc2091783de68a6ece72c7"):
            raise ValueError("paired V248 terminal differs")
        for name, digest in {
            "vectors.raw": "0f3631d71c105e5ea3d701c96033b362c2f84bd43002a9c8a5c70040801be06e",
            "plane.bin": "4e164997e53e2e59c0930403386de863ceb17eaa000cd904b699370eca0d9de7",
            "books.bin": "d6d36b22f66ecb5cfc750c117392ab1c0f264dc5bac3e9785cdab92dc6a75b19",
            "codes.bin": "a645cf66c8fd92d1f248438807f97cfd676ca8fb4233efd23d7adc299ff5e9e3",
            "map.u32": "20ff50e632cc575386b15d7fcd9c3842ef435388ed29ae8c30617158ee907dc5",
            "requests.jsonl": "86d9406486a2bb27aa2e603f019e078dd3ecaed47f79ec685558ba3536433812",
            "truth.u32": "06cd59b31962d4190367b54d7abf24dd4e018d3c4ac8da0b2b528d21a5a7cbb8",
        }.items():
            if parent["artifacts"].get(name, {}).get("sha256") != digest:
                raise ValueError(f"paired V248 artifact differs: {name}")
    if not big and (diverse or exact_nav or anchored or global_pq or coarse_pq or hybrid_mode):
        previous = s3.get_object(Bucket=BUCKET,
                                 Key=V249_PREFIX + "/terminal.json")["Body"].read()
        if (hashlib.sha256(previous).hexdigest() != V249_TERMINAL_SHA
                or json.loads(previous).get("status") != "complete"):
            raise ValueError("V249 baseline terminal differs")
    if big or exact_nav or anchored or global_pq or coarse_pq or hybrid_mode:
        previous = s3.get_object(Bucket=BUCKET,
                                 Key=V250_PREFIX + "/terminal.json")["Body"].read()
        prior = json.loads(previous)
        if (hashlib.sha256(previous).hexdigest() != V250_TERMINAL_SHA
                or prior.get("status") != "complete"
                or prior["artifacts"].get("graph.bin", {}).get("sha256")
                != "688941c7c61c89a39739909af14cb2b4a935a7a4967a168f9503ac34e170d0e7"):
            raise ValueError("V250 paired graph terminal differs")
    if anchored or global_pq or coarse_pq or dual:
        previous = s3.get_object(Bucket=BUCKET,
                                 Key=V251_PREFIX + "/terminal.json")["Body"].read()
        prior = json.loads(previous)
        if (hashlib.sha256(previous).hexdigest() != V251_TERMINAL_SHA
                or prior.get("status") != "complete"
                or prior["artifacts"].get("graph.bin", {}).get("sha256")
                != "688941c7c61c89a39739909af14cb2b4a935a7a4967a168f9503ac34e170d0e7"):
            raise ValueError("V251 paired graph terminal differs")
    if global_pq or coarse_pq:
        previous = s3.get_object(Bucket=BUCKET,
                                 Key=V252_PREFIX + "/terminal.json")["Body"].read()
        if (hashlib.sha256(previous).hexdigest() != V252_TERMINAL_SHA
                or json.loads(previous).get("status") != "complete"):
            raise ValueError("V252 paired terminal differs")
    if coarse_pq:
        previous = s3.get_object(Bucket=BUCKET,
                                 Key=V253_PREFIX + "/terminal.json")["Body"].read()
        if (hashlib.sha256(previous).hexdigest() != V253_TERMINAL_SHA
                or json.loads(previous).get("status") != "complete"):
            raise ValueError("V253 paired terminal differs")
    if hybrid or dual:
        previous = s3.get_object(Bucket=BUCKET,
                                 Key=V254_PREFIX + "/terminal.json")["Body"].read()
        if (hashlib.sha256(previous).hexdigest() != V254_TERMINAL_SHA
                or json.loads(previous).get("status") != "complete"):
            raise ValueError("V254 paired terminal differs")
    if million_hybrid or million_exact:
        for prior_prefix, prior_sha in ((V255_PREFIX, V255_TERMINAL_SHA),
                                        (V256_PREFIX, V256_TERMINAL_SHA)):
            previous = s3.get_object(Bucket=BUCKET,
                                     Key=prior_prefix + "/terminal.json")["Body"].read()
            if (hashlib.sha256(previous).hexdigest() != prior_sha
                    or json.loads(previous).get("status") != "complete"):
                raise ValueError("V255/V256 paired terminal differs")
    if dual:
        previous = s3.get_object(Bucket=BUCKET,
                                 Key=V256_PREFIX + "/terminal.json")["Body"].read()
        if (hashlib.sha256(previous).hexdigest() != V256_TERMINAL_SHA
                or json.loads(previous).get("status") != "complete"):
            raise ValueError("V256 hybrid baseline terminal differs")
    if reuse_v257:
        previous = s3.get_object(Bucket=BUCKET,
                                 Key=V257_PREFIX + "/terminal.json")["Body"].read()
        prior = json.loads(previous)
        if (hashlib.sha256(previous).hexdigest() != V257_TERMINAL_SHA
                or prior.get("status") != "failed" or prior.get("phase") != "serve"):
            raise ValueError("closed V257 a0001 reuse authority differs")
    if not missing(s3, prefix + "/reservation.json") or not missing(s3, prefix + "/terminal.json"):
        raise ValueError("attempt already registered")
    active = ec2.describe_instances(Filters=[
        {"Name": "tag:Name", "Values": ["borsuk-*", "v24*"]},
        {"Name": "instance-state-name", "Values": ["pending", "running", "stopping"]},
    ])
    if any(row.get("Instances") for row in active["Reservations"]):
        raise ValueError("another BORSUK worker is active")
    if missing(s3, archive_key):
        put_if_absent(archive_key, archive)
    elif s3.head_object(Bucket=BUCKET, Key=archive_key)["ContentLength"] != len(archive):
        raise ValueError("source archive length differs")
    put_if_absent(prefix + "/reservation.json", json.dumps({
        "schema": schema, "source_commit": commit,
        "source_archive_sha256": archive_sha, "staging_receipt_sha256": RECEIPT_SHA,
        "test_sha256": TEST_SHA, "dataset": "CoHere-large-10M first 1M D768 cosine" if big else "CoHere-large-10M first 100k D768 cosine",
        "split": "development 0-255; validation 256-999 prior used",
        "construction": {"m": 32, "m0": 64, "ef_construction": 128,
                         "workers": 8, "source_only_pq64": True},
        "arms": [[4096, 2048, 8192]] if dual else [[2048, 0]] if million_exact else [[4096, 8192]] if hybrid_mode else [[4096, 4096]] if million else [[0, 8192]] if global_pq or coarse_pq else
                [[512, 0]] if anchored else
                [[512, 0], [1024, 0], [2048, 0]] if exact_nav else
                [[2048, 2048], [4096, 4096], [8192, 8192]],
        "pq_aligned_topology": pq_topology,
        "diverse_pruning": hybrid_mode or big or diverse or exact_nav or anchored or global_pq or coarse_pq,
        "exact_fp16_navigation": exact_nav or anchored or million_exact or dual,
        "strided_anchors": 256 if anchored else 0,
        "global_pq_scan_rows": 100_000 if global_pq else 0,
        "coarse_pq": {"rows_per_cell": 256, "copies": 2, "probes": 32,
                      "shortlist": 8192, "seed": 254, "lloyd_iterations": 6} if coarse_pq or hybrid_mode else None,
        "paired_v248_terminal_sha256": PARENT_TERMINAL_SHA if pq_topology or diverse or exact_nav or anchored or global_pq or coarse_pq or hybrid or dual else None,
        "paired_v249_terminal_sha256": V249_TERMINAL_SHA if diverse or exact_nav or anchored or global_pq or coarse_pq or hybrid or dual else None,
        "paired_v250_terminal_sha256": V250_TERMINAL_SHA if big or exact_nav or anchored or global_pq or coarse_pq or hybrid_mode else None,
        "paired_v251_terminal_sha256": V251_TERMINAL_SHA if anchored or global_pq or coarse_pq or dual else None,
        "paired_v252_terminal_sha256": V252_TERMINAL_SHA if global_pq or coarse_pq else None,
        "paired_v253_terminal_sha256": V253_TERMINAL_SHA if coarse_pq else None,
        "paired_v254_terminal_sha256": V254_TERMINAL_SHA if hybrid or dual else None,
        "paired_v255_terminal_sha256": V255_TERMINAL_SHA if million_hybrid or million_exact else None,
        "paired_v256_terminal_sha256": V256_TERMINAL_SHA if million_hybrid or million_exact or dual else None,
        "reused_v257_a0001_terminal_sha256": V257_TERMINAL_SHA if reuse_v257 else None,
        "interruption_policy": "discard interrupted cell, restart under a new attempt",
        "output_prefix": f"s3://{BUCKET}/{prefix}",
    }, sort_keys=True).encode())
    receipt = ec2.run_instances(
        ClientToken=("v259-" if dual else "v258-" if million_exact else "v257-" if million_hybrid else "v256-" if hybrid else "v255-" if million else "v254-" if coarse_pq else "v253-" if global_pq else "v252-" if anchored else "v251-" if exact_nav else "v250-" if diverse else "v249-" if pq_topology else "v248-")
        + hashlib.sha256(prefix.encode()).hexdigest()[:48],
        ImageId=IMAGE, InstanceType="c7i.4xlarge", MinCount=1, MaxCount=1,
        IamInstanceProfile={"Arn": PROFILE_ARN},
        NetworkInterfaces=[{"AssociatePublicIpAddress": True, "DeviceIndex": 0,
                            "Groups": [SECURITY_GROUP], "SubnetId": SUBNET}],
        InstanceMarketOptions={"MarketType": "spot", "SpotOptions": {
            "InstanceInterruptionBehavior": "terminate", "SpotInstanceType": "one-time"}},
        InstanceInitiatedShutdownBehavior="terminate",
        BlockDeviceMappings=[{"DeviceName": "/dev/xvda", "Ebs": {
            "DeleteOnTermination": True, "Encrypted": True, "VolumeSize": 80 if big else 30,
            "VolumeType": "gp3"}}],
        TagSpecifications=[{"ResourceType": "instance", "Tags": [
            {"Key": "Name", "Value": "borsuk-" + campaign},
            {"Key": "BorsukAttempt", "Value": attempt}]}],
        UserData=base64.b64encode(worker(commit, archive_sha, archive_key, prefix,
                                          pq_topology, diverse, exact_nav, anchored, global_pq,
                                          coarse_pq, million, hybrid, million_hybrid,
                                          reuse_v257, million_exact, dual).encode()).decode(),
    )
    instance_id = receipt["Instances"][0]["InstanceId"]
    print(json.dumps({"instance_id": instance_id, "output_prefix": prefix,
                      "source_commit": commit, "source_archive_sha256": archive_sha}), flush=True)
    try:
        deadline = time.monotonic() + WALL_SECONDS + 600
        while time.monotonic() < deadline:
            try:
                raw = s3.get_object(Bucket=BUCKET, Key=prefix + "/terminal.json")["Body"].read()
                terminal = json.loads(raw)
                ec2.terminate_instances(InstanceIds=[instance_id])
                ec2.get_waiter("instance_terminated").wait(InstanceIds=[instance_id])
                if (terminal.get("schema") != schema or terminal.get("instance_id") != instance_id
                        or terminal.get("source_commit") != commit
                        or terminal.get("source_archive_sha256") != archive_sha):
                    raise ValueError("V248 terminal identity differs")
                terminal["terminal_sha256"] = hashlib.sha256(raw).hexdigest()
                print(json.dumps(terminal, sort_keys=True), flush=True)
                if terminal.get("status") != "complete":
                    raise RuntimeError("V248 failed; inspect only closed terminal artifacts")
                if set(terminal.get("artifacts", {})) != set(ARTIFACTS + HYBRID_ARTIFACTS if hybrid_mode else ARTIFACTS + ("loaded-raw.jsonl",) if million_exact else ARTIFACTS + COARSE_ARTIFACTS if coarse_pq else ARTIFACTS):
                    raise ValueError("V248 artifact roster differs")
                for name, identity in terminal["artifacts"].items():
                    body = s3.get_object(Bucket=BUCKET,
                        Key=f"{prefix}/artifacts/{name}")["Body"]
                    value = hashlib.sha256()
                    size = 0
                    while chunk := body.read(4 * 1024 * 1024):
                        value.update(chunk)
                        size += len(chunk)
                    if size != identity["bytes"] or value.hexdigest() != identity["sha256"]:
                        raise ValueError(f"V248 artifact readback differs: {name}")
                print(json.dumps({"instance_id": instance_id,
                                  "final_state": "terminated", "artifact_replay": "pass"}), flush=True)
                return
            except ClientError as error:
                if error.response.get("Error", {}).get("Code") not in {"NoSuchKey", "404", "NotFound"}:
                    raise
            state = ec2.describe_instances(InstanceIds=[instance_id])["Reservations"][0]["Instances"][0]["State"]["Name"]
            if state in {"terminated", "shutting-down"}:
                raise RuntimeError("V248 worker stopped before terminal")
            time.sleep(20)
        raise TimeoutError("V248 Spot cell exceeded wall cap")
    finally:
        try:
            state = ec2.describe_instances(InstanceIds=[instance_id])["Reservations"][0]["Instances"][0]["State"]["Name"]
            if state not in {"terminated", "shutting-down"}:
                ec2.terminate_instances(InstanceIds=[instance_id])
        except Exception:
            pass


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--attempt", default="a0001")
    parser.add_argument("--pq-topology", action="store_true")
    parser.add_argument("--diverse", action="store_true")
    parser.add_argument("--exact-nav", action="store_true")
    parser.add_argument("--anchored", action="store_true")
    parser.add_argument("--global-pq", action="store_true")
    parser.add_argument("--coarse-pq", action="store_true")
    parser.add_argument("--million", action="store_true")
    parser.add_argument("--hybrid", action="store_true")
    parser.add_argument("--million-hybrid", action="store_true")
    parser.add_argument("--million-exact", action="store_true")
    parser.add_argument("--dual", action="store_true")
    parser.add_argument("--reuse-v257", action="store_true")
    args = parser.parse_args()
    with open("/tmp/borsuk-cohere-graph-launch.lock", "a+") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        launch(args.attempt, args.pq_topology, args.diverse, args.exact_nav,
               args.anchored, args.global_pq, args.coarse_pq, args.million,
               args.hybrid, args.million_hybrid, args.reuse_v257,
               args.million_exact, args.dual)
