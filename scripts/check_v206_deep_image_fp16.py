#!/usr/bin/env python3
"""Independently recount closed Deep-Image FP16 IDs against published GT."""

from __future__ import annotations

import hashlib
import json
import os
import subprocess
import tempfile
from pathlib import Path

import pyarrow.parquet as pq

BUCKET = "borsuk-bench-453182569524-euc1"
SOURCE = "ac6738bdec80e9ba94ee505c271168ab50d0ff88"
INSTANCE = "i-0b30e9d5fbd2e129b"
PREFIX = f"research/v206-deep-image-fp16/{SOURCE}/runs/a0002/"
V121 = ("research/v121-deep-image-paired/79a51449cbeb169851d9c02c173488d0f073d519/"
        "runs/v121-20260924T014535Z/a0003/artifacts/")
TRUTH = ("publication/v3/20260812/datasets/deep-image-96/attempts/0001/"
         "materialized/neighbors.parquet")
TERMINAL_SHA = "8a16c5053a3d65235ef25986bd45b49ddf4db697c4a8bbb028a481118fb71000"
TRUTH_SHA = "d305fcea7387988941defd2942cca1673693271329f977ba073da888cac3de8d"
RAW_SHA = "3fb5ef49833d672b82346612bbd80d0d50cceaed72d5de1d6b5da797c5762dd0"
ROWS, ROW_BYTES, PAGE_BYTES = 9_990_000, 108, 256 * 108


def digest(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def fetch(key: str, root: Path, name: str) -> bytes:
    path = root / name
    subprocess.run(["aws", "s3", "cp", f"s3://{BUCKET}/{key}", str(path),
                    "--only-show-errors"], check=True,
                   env={**os.environ, "AWS_PROFILE": "causality"})
    return path.read_bytes()


def records(raw: bytes) -> list[dict]:
    return [json.loads(line) for line in raw.splitlines()]


def valid_ranges(ranges: list[list[int]], charged: int) -> bool:
    previous = 0
    actual = 0
    for start, end in ranges:
        if (start < previous or start < 0 or start >= end
                or end > ROWS * ROW_BYTES or start % PAGE_BYTES
                or (end != ROWS * ROW_BYTES and end % PAGE_BYTES)
                or start % ROW_BYTES or end % ROW_BYTES):
            return False
        actual += end - start
        previous = end
    return 1 <= len(ranges) <= 32 and charged == actual <= 16_777_216


def check() -> dict:
    with tempfile.TemporaryDirectory(prefix="borsuk-v206-check-") as temporary:
        root = Path(temporary)
        terminal_raw = fetch(PREFIX + "terminal.json", root, "terminal.json")
        terminal = json.loads(terminal_raw)
        if (digest(terminal_raw) != TERMINAL_SHA
                or terminal.get("schema") != "borsuk-v206-deep-image-fp16-spot-v1"
                or terminal.get("status") != "complete" or terminal.get("exit_code") != 0
                or terminal.get("phase") != "complete"
                or terminal.get("instance_id") != INSTANCE
                or terminal.get("source_commit") != SOURCE):
            raise ValueError("V206 terminal differs")
        artifacts = {}
        for name, identity in terminal["artifacts"].items():
            raw = fetch(PREFIX + "artifacts/" + name, root, name)
            if identity != {"bytes": len(raw), "sha256": digest(raw)}:
                raise ValueError(f"V206 artifact differs: {name}")
            artifacts[name] = raw
        if set(artifacts) != {"raw.jsonl", "pretruth-seal.json", "evidence.jsonl",
                              "summary.json", "return-resources.txt", "score-resources.txt",
                              "return.log", "score.log", "smoke.log", "install.log",
                              "run-closed.log"}:
            raise ValueError("V206 artifact roster differs")
        if "V206 synthetic top100 parity pass" not in artifacts["smoke.log"].decode():
            raise ValueError("V206 smoke result differs")
        for name, remote in (("raw.jsonl", "raw.jsonl"),
                             ("pretruth-seal.json", "seal.json")):
            copy = fetch(PREFIX + "pretruth/" + remote, root, "pretruth-" + remote)
            if copy != artifacts[name]:
                raise ValueError(f"V206 pretruth copy differs: {name}")
        seal = json.loads(artifacts["pretruth-seal.json"])
        if (seal.get("source_truth_opened") is not False
                or seal.get("schema") != "borsuk-v206-deep-image-fp16-v1-pretruth-seal"
                or seal.get("raw_sha256") != RAW_SHA
                or digest(artifacts["raw.jsonl"]) != RAW_SHA):
            raise ValueError("V206 pretruth seal differs")
        replay_raw = fetch(V121 + "rust-replay.jsonl", root, "v121-replay.jsonl")
        old_evidence_raw = fetch(V121 + "evidence.jsonl", root, "v121-evidence.jsonl")
        if (digest(replay_raw) !=
                "ddc9af991bdc6d3ef77d34a156994daa43aeb78f67f18de2cd0dc5ebb93abe91"
                or digest(old_evidence_raw) !=
                "906e54777e65227ffef329d36d1ac7961c8bc06aed7dcd5a76327a5f748745de"):
            raise ValueError("V121 frozen paired evidence differs")
        truth_raw = fetch(TRUTH, root, "truth.parquet")
        if digest(truth_raw) != TRUTH_SHA:
            raise ValueError("V206 GT identity differs")
        truth = pq.read_table(root / "truth.parquet", columns=["neighbors_id"])
        gold = truth["neighbors_id"].slice(0, 1000).to_pylist()
        rows = records(artifacts["raw.jsonl"])
        evidence = records(artifacts["evidence.jsonl"])
        prior = records(replay_raw)
        old_evidence = records(old_evidence_raw)
        if any(len(group) != 1000 for group in (rows, evidence, prior, old_evidence, gold)):
            raise ValueError("V206 paired roster differs")
        hits = {arm: [] for arm in ("fp16", "f32", "sq8")}
        bytes_total = gets_total = wins = ties = losses = 0
        for ordinal, (row, result, old, old_score, neighbors) in enumerate(
                zip(rows, evidence, prior, old_evidence, gold, strict=True)):
            if (row.get("ordinal") != ordinal or result.get("ordinal") != ordinal
                    or old.get("query_ordinal") != ordinal
                    or old_score.get("query_ordinal") != ordinal
                    or row["ranges"] != old["ranges"]
                    or row["bytes"] != old["plan_bytes"]
                    or row["gets"] != len(old["ranges"])
                    or row["sq8_ids"] != old["returned_ids"]
                    or not valid_ranges(row["ranges"], row["bytes"])):
                raise ValueError(f"V206 paired physical plan differs: {ordinal}")
            truth_ids = set(neighbors[:100])
            if len(truth_ids) != 100:
                raise ValueError(f"V206 truth row differs: {ordinal}")
            for arm in hits:
                ids = row[f"{arm}_ids"]
                if len(ids) != 100 or len(set(ids)) != 100 or any(
                        type(value) is not int or not 0 <= value < ROWS for value in ids):
                    raise ValueError(f"V206 returned IDs differ: {ordinal} {arm}")
                count = len(truth_ids.intersection(ids))
                if result[f"{arm}_hits"] != count:
                    raise ValueError(f"V206 GT intersection differs: {ordinal} {arm}")
                hits[arm].append(count)
            if old_score["candidate_hits"] != hits["sq8"][-1]:
                raise ValueError(f"V206 V121 SQ8 baseline differs: {ordinal}")
            bytes_total += row["bytes"]
            gets_total += row["gets"]
            wins += hits["fp16"][-1] > hits["sq8"][-1]
            ties += hits["fp16"][-1] == hits["sq8"][-1]
            losses += hits["fp16"][-1] < hits["sq8"][-1]
        summary = json.loads(artifacts["summary.json"])
        totals = {arm: sum(values) for arm, values in hits.items()}
        p05 = {arm: sorted(values)[49] for arm, values in hits.items()}
        sub90 = {arm: sum(value < 90 for value in values) for arm, values in hits.items()}
        if (summary.get("schema") != "borsuk-v206-deep-image-fp16-v1-summary"
                or summary.get("hits") != totals or summary.get("p05_hits") != p05
                or summary.get("below_90") != sub90
                or summary.get("fp16_vs_sq8_wins") != wins
                or summary.get("fp16_vs_sq8_ties") != ties
                or summary.get("fp16_vs_sq8_losses") != losses
                or summary.get("planned_gets") != gets_total
                or summary.get("planned_bytes") != bytes_total
                or summary.get("passes_representation_screen") is not True):
            raise ValueError("V206 independent summary differs")
        return {"status": "pass", "hits": totals, "p05": p05,
                "below_90": sub90, "wins": wins, "ties": ties, "losses": losses,
                "planned_gets": gets_total, "planned_bytes": bytes_total,
                "terminal_sha256": TERMINAL_SHA}


if __name__ == "__main__":
    print(json.dumps(check(), sort_keys=True))
