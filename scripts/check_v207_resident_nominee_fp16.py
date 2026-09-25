#!/usr/bin/env python3
"""Independently recount the closed V207 resident nominee returned IDs."""

from __future__ import annotations

import hashlib
import json

import boto3
import pyarrow.parquet as pq

from scripts.launch_v207_resident_nominee_fp16_spot import (
    BUCKET, INPUTS, REGION, SCHEMA, TRUTH_INPUT, V121,
)
from scripts.v207_resident_nominee_fp16 import PLANE_BYTES

SOURCE = "03e9c6d53cdcd055ad09ccec8b1d6c376245773d"
INSTANCE = "i-0eee33f20341ffd9f"
PREFIX = f"research/v207-resident-nominee-fp16/{SOURCE}/runs/a0001/"
TERMINAL_SHA = "9d33b68fb3f15cdcd648e146902dbfcc38d524c1b1f1605bb155366a5511849c"
ROWS = 9_990_000


def digest(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def records(raw: bytes) -> list[dict]:
    return [json.loads(line) for line in raw.splitlines()]


def check() -> dict:
    session = boto3.Session(profile_name="causality", region_name=REGION)
    s3 = session.client("s3")

    def fetch(key: str) -> bytes:
        return s3.get_object(Bucket=BUCKET, Key=key)["Body"].read()

    terminal_raw = fetch(PREFIX + "terminal.json")
    terminal = json.loads(terminal_raw)
    if (digest(terminal_raw) != TERMINAL_SHA
            or terminal.get("schema") != SCHEMA or terminal.get("status") != "complete"
            or terminal.get("phase") != "complete" or terminal.get("exit_code") != 0
            or terminal.get("source_commit") != SOURCE
            or terminal.get("instance_id") != INSTANCE):
        raise ValueError("V207 terminal identity differs")
    artifacts = {}
    for name, identity in terminal["artifacts"].items():
        if name == "resident-fp16.bin":
            if identity["bytes"] != PLANE_BYTES:
                raise ValueError("V207 plane length differs")
            # The closed launcher already streamed and authenticated this 1.9-GB
            # artifact from S3. This recount reads only the compact witnesses.
            continue
        raw = fetch(PREFIX + "artifacts/" + name)
        if identity != {"bytes": len(raw), "sha256": digest(raw)}:
            raise ValueError(f"V207 artifact identity differs: {name}")
        artifacts[name] = raw
    if set(artifacts) != {"raw.jsonl", "pretruth-seal.json", "evidence.jsonl",
                          "summary.json", "return-resources.txt", "score-resources.txt",
                          "return.log", "score.log", "smoke.log", "install.log",
                          "run-closed.log"}:
        raise ValueError("V207 artifact roster differs")
    if "V207 synthetic top100 parity pass" not in artifacts["smoke.log"].decode():
        raise ValueError("V207 smoke result differs")
    for local, remote in (("raw.jsonl", "raw.jsonl"),
                          ("pretruth-seal.json", "seal.json")):
        if fetch(PREFIX + "pretruth/" + remote) != artifacts[local]:
            raise ValueError(f"V207 pretruth copy differs: {local}")
    seal = json.loads(artifacts["pretruth-seal.json"])
    expected_input = {name: sha for name, _, _, sha in INPUTS}
    if (seal.get("schema") != "borsuk-v207-resident-nominee-fp16-v1-pretruth-seal"
            or seal.get("source_truth_opened") is not False
            or seal.get("raw_sha256") != digest(artifacts["raw.jsonl"])
            or seal.get("plane_sha256") != terminal["artifacts"]["resident-fp16.bin"]["sha256"]
            or seal.get("plane_bytes") != PLANE_BYTES
            or seal.get("source_sha256") != expected_input["source.parquet"]
            or seal.get("layout_sha256") != expected_input["layout.npy"]
            or seal.get("queries_sha256") != expected_input["queries.jsonl"]
            or seal.get("rosters_sha256") != expected_input["rosters.jsonl"]
            or seal.get("prior_sha256") != expected_input["prior.jsonl"]):
        raise ValueError("V207 pretruth seal differs")
    rosters_raw = fetch(V121 + "rosters.jsonl")
    prior_raw = fetch(next(key for name, key, _, _ in INPUTS if name == "prior.jsonl"))
    if (digest(rosters_raw) != expected_input["rosters.jsonl"]
            or digest(prior_raw) != expected_input["prior.jsonl"]):
        raise ValueError("V207 frozen baselines differ")
    truth_key = TRUTH_INPUT[0][1]
    truth_raw = fetch(truth_key)
    if (len(truth_raw) != TRUTH_INPUT[0][2]
            or digest(truth_raw) != TRUTH_INPUT[0][3]):
        raise ValueError("V207 published GT identity differs")
    import io
    gold = pq.read_table(io.BytesIO(truth_raw), columns=["neighbors_id"])
    truth = gold["neighbors_id"].slice(0, 1000).to_pylist()
    rows, evidence, rosters, prior = map(records, (
        artifacts["raw.jsonl"], artifacts["evidence.jsonl"],
        rosters_raw, prior_raw))
    if any(len(group) != 1000 for group in (rows, evidence, rosters, prior, truth)):
        raise ValueError("V207 paired cohort differs")
    hits = {arm: [] for arm in ("fp16", "f32", "prior_fp16")}
    times = []
    wins = ties = losses = 0
    for ordinal, (row, result, roster, old, neighbors) in enumerate(
            zip(rows, evidence, rosters, prior, truth, strict=True)):
        nominees = row["nominee_ids"]
        if (row["ordinal"] != ordinal or result["ordinal"] != ordinal
                or roster["query_ordinal"] != ordinal or old["ordinal"] != ordinal
                or len(nominees) != 512 or len(set(nominees)) != 512
                or any(type(item) is not int or not 0 <= item < ROWS for item in nominees)
                or row["prior_fp16_ids"] != old["fp16_ids"]):
            raise ValueError(f"V207 roster differs: {ordinal}")
        target = set(neighbors[:100])
        if len(target) != 100:
            raise ValueError(f"V207 GT100 differs: {ordinal}")
        for arm in hits:
            ids = row[f"{arm}_ids"]
            if (len(ids) != 100 or len(set(ids)) != 100
                    or any(type(item) is not int or not 0 <= item < ROWS for item in ids)
                    or (arm != "prior_fp16" and not set(ids).issubset(nominees))):
                raise ValueError(f"V207 returned IDs differ: {ordinal} {arm}")
            count = len(target.intersection(ids))
            if result[f"{arm}_hits"] != count:
                raise ValueError(f"V207 GT reduction differs: {ordinal} {arm}")
            hits[arm].append(count)
        if type(row["fp16_score_ns"]) is not int or row["fp16_score_ns"] <= 0:
            raise ValueError(f"V207 score clock differs: {ordinal}")
        times.append(row["fp16_score_ns"])
        wins += hits["fp16"][-1] > hits["prior_fp16"][-1]
        ties += hits["fp16"][-1] == hits["prior_fp16"][-1]
        losses += hits["fp16"][-1] < hits["prior_fp16"][-1]
    totals = {arm: sum(values) for arm, values in hits.items()}
    p05 = {arm: sorted(values)[49] for arm, values in hits.items()}
    sub90 = {arm: sum(value < 90 for value in values)
             for arm, values in hits.items()}
    ordered = sorted(times)
    summary = json.loads(artifacts["summary.json"])
    if (summary.get("schema") != "borsuk-v207-resident-nominee-fp16-v1-summary"
            or summary.get("hits") != totals or summary.get("p05_hits") != p05
            or summary.get("below_90") != sub90
            or summary.get("fp16_vs_prior_wins") != wins
            or summary.get("fp16_vs_prior_ties") != ties
            or summary.get("fp16_vs_prior_losses") != losses
            or summary.get("fp16_score_ns_p50") != ordered[499]
            or summary.get("fp16_score_ns_p95") != ordered[949]
            or summary.get("fp16_score_ns_p99") != ordered[989]
            or summary.get("plane_bytes") != PLANE_BYTES
            or summary.get("truth_sha256") != digest(truth_raw)):
        raise ValueError("V207 independent summary differs")
    return {"status": "pass", "terminal_sha256": digest(terminal_raw),
            "hits": totals, "p05_hits": p05, "below_90": sub90,
            "wins": wins, "ties": ties, "losses": losses,
            "score_ns_p50_p95_p99": [ordered[index] for index in (499, 949, 989)]}


if __name__ == "__main__":
    print(json.dumps(check(), sort_keys=True))
