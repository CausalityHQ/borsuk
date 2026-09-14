#!/usr/bin/env python3
"""Disposable corpus-only nonlinear page-router falsifier for ReLAION2B 1M."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import random
import time
from pathlib import Path

import numpy as np
import pyarrow.parquet as pq
import torch
from torch import nn


ROWS = 1_000_000
QUERIES = 1_000
DIMENSIONS = 768
PAGES = 123
SELECTED_PAGES = 21
SEED = 20260914
EPOCHS = 20
BATCH = 4096


def fixed_list(table, name: str, width: int, rows: int) -> np.ndarray:
    column = table[name].combine_chunks()
    values = column.values.to_numpy(zero_copy_only=False)
    result = np.asarray(values, dtype=np.float32).reshape(rows, width)
    if result.shape != (rows, width) or not np.isfinite(result).all():
        raise ValueError(f"{name} differs")
    return result


def scalar(table, name: str, dtype) -> np.ndarray:
    return np.asarray(table[name].combine_chunks().to_numpy(zero_copy_only=False), dtype=dtype)


def load_corpus(source_path: Path, relation_path: Path):
    source = pq.read_table(source_path)
    if source.num_rows != ROWS:
        raise ValueError("source row count differs")
    feature_ids = scalar(source, "feature_row_id", np.uint64)
    vectors = fixed_list(source, "embedding", DIMENSIONS, ROWS)

    relation = pq.read_table(relation_path)
    required = {"source_ordinal", "feature_row_id", "posting_ordinal", "owner_role"}
    if not required.issubset(relation.column_names):
        raise ValueError("relation schema differs")
    source_ordinal = scalar(relation, "source_ordinal", np.int64)
    relation_features = scalar(relation, "feature_row_id", np.uint64)
    page = scalar(relation, "posting_ordinal", np.int64)
    role = scalar(relation, "owner_role", np.int8)
    if (
        np.any(source_ordinal < 0)
        or np.any(source_ordinal >= ROWS)
        or np.any(page < 0)
        or np.any(page >= PAGES)
        or np.any((role != 0) & (role != 1))
    ):
        raise ValueError("relation values differ")
    primary = np.full(ROWS, -1, dtype=np.int64)
    alternate = np.full(ROWS, -1, dtype=np.int64)
    primary[source_ordinal[role == 0]] = page[role == 0]
    alternate[source_ordinal[role == 1]] = page[role == 1]
    if np.any(primary < 0) or np.any(relation_features != feature_ids[source_ordinal]):
        raise ValueError("relation/source binding differs")
    if np.any((alternate >= 0) & (alternate == primary)):
        raise ValueError("relation owner roles differ")
    return vectors, feature_ids, primary, alternate


class Router(nn.Module):
    def __init__(self) -> None:
        super().__init__()
        self.network = nn.Sequential(
            nn.Linear(DIMENSIONS, 256),
            nn.GELU(),
            nn.Linear(256, 128),
            nn.Linear(128, PAGES),
        )

    def forward(self, inputs: torch.Tensor) -> torch.Tensor:
        return self.network(inputs)


def train(vectors: np.ndarray, primary: np.ndarray, alternate: np.ndarray, device: torch.device):
    torch.manual_seed(SEED)
    torch.cuda.manual_seed_all(SEED)
    model = Router().to(device)
    optimizer = torch.optim.AdamW(model.parameters(), lr=1e-3, weight_decay=1e-4)
    generator = torch.Generator(device="cpu").manual_seed(SEED)
    started = time.perf_counter()
    epoch_losses = []
    model.train()
    corpus = torch.from_numpy(vectors).to(device)
    primary_labels = torch.from_numpy(primary).to(device)
    alternate_labels = torch.from_numpy(alternate).to(device)
    for epoch in range(EPOCHS):
        permutation = torch.randperm(ROWS, generator=generator)
        loss_sum = 0.0
        for start in range(0, ROWS, BATCH):
            indices = permutation[start : start + BATCH].to(device)
            inputs = corpus[indices]
            first = primary_labels[indices]
            second = alternate_labels[indices]
            logits = model(inputs)
            primary_loss = nn.functional.cross_entropy(logits, first, reduction="none")
            valid = second >= 0
            safe_second = torch.where(valid, second, first)
            alternate_loss = nn.functional.cross_entropy(logits, safe_second, reduction="none")
            loss = torch.where(valid, (primary_loss + alternate_loss) * 0.5, primary_loss).mean()
            optimizer.zero_grad(set_to_none=True)
            loss.backward()
            optimizer.step()
            loss_sum += float(loss.detach()) * indices.numel()
        epoch_loss = loss_sum / ROWS
        epoch_losses.append(epoch_loss)
        print(json.dumps({"epoch": epoch + 1, "loss": epoch_loss}), flush=True)
    return model, epoch_losses, time.perf_counter() - started


def load_queries(query_path: Path, truth_path: Path):
    queries_table = pq.read_table(query_path)
    truth_table = pq.read_table(truth_path)
    if queries_table.num_rows != QUERIES or truth_table.num_rows != QUERIES * 100:
        raise ValueError("development row count differs")
    vectors = fixed_list(queries_table, "embedding", DIMENSIONS, QUERIES)
    truth_queries = scalar(truth_table, "query_ordinal", np.int64)
    truth_ranks = scalar(truth_table, "rank", np.int64)
    truth = scalar(truth_table, "feature_row_id", np.uint64).reshape(QUERIES, 100)
    if not np.array_equal(truth_queries, np.repeat(np.arange(QUERIES), 100)) or not np.array_equal(
        truth_ranks, np.tile(np.arange(100), QUERIES)
    ):
        raise ValueError("development ground-truth order differs")
    return vectors, truth


def evaluate(
    model: Router,
    query_vectors: np.ndarray,
    truth: np.ndarray,
    feature_ids: np.ndarray,
    primary: np.ndarray,
    alternate: np.ndarray,
    indices: np.ndarray,
    device: torch.device,
    aggregate_gate: int,
    minimum_gate: int,
):
    order = np.argsort(feature_ids)
    ordered_ids = feature_ids[order]
    positions = np.searchsorted(ordered_ids, truth[indices])
    if np.any(positions >= ROWS) or np.any(ordered_ids[positions] != truth[indices]):
        raise ValueError("ground truth/source binding differs")
    truth_rows = order[positions]
    truth_primary = primary[truth_rows]
    truth_alternate = alternate[truth_rows]
    model.eval()
    started = time.perf_counter()
    with torch.no_grad():
        logits = model(torch.from_numpy(query_vectors[indices]).to(device)).cpu().numpy()
        page_ids = np.arange(PAGES)
        selected = np.stack(
            [np.lexsort((page_ids, -scores))[:SELECTED_PAGES] for scores in logits]
        )
    elapsed = time.perf_counter() - started
    hit_primary = np.any(selected[:, :, None] == truth_primary[:, None, :], axis=1)
    hit_alternate = np.any(selected[:, :, None] == truth_alternate[:, None, :], axis=1)
    hits = hit_primary | hit_alternate
    per_query = hits.sum(axis=1)

    rare_hits = 0
    rare_total = 0
    for query in range(len(indices)):
        support = np.zeros(PAGES, dtype=np.int32)
        np.add.at(support, truth_primary[query], 1)
        valid = truth_alternate[query] >= 0
        np.add.at(support, truth_alternate[query, valid], 1)
        neighbor_support = np.maximum(
            support[truth_primary[query]],
            np.where(valid, support[np.maximum(truth_alternate[query], 0)], 0),
        )
        rare = neighbor_support <= 2
        rare_hits += int(np.count_nonzero(hits[query] & rare))
        rare_total += int(np.count_nonzero(rare))

    aggregate = int(per_query.sum() * 1_000_000 // (len(indices) * 100))
    minimum = int(per_query.min() * 10_000)
    return {
        "queries": int(len(indices)),
        "aggregate_recall_ppm": aggregate,
        "minimum_recall_ppm": minimum,
        "rare_support_recall_ppm": int(rare_hits * 1_000_000 // rare_total) if rare_total else None,
        "selected_pages": SELECTED_PAGES,
        "inference_elapsed_ms": int(elapsed * 1000),
        "passed": aggregate >= aggregate_gate and minimum >= minimum_gate,
    }


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--development-query", type=Path, required=True)
    parser.add_argument("--development-ground-truth", type=Path, required=True)
    parser.add_argument("--spill-relation", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    random.seed(SEED)
    np.random.seed(SEED)
    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    if device.type == "cpu":
        torch.set_num_threads(os.cpu_count() or 1)
    vectors, feature_ids, primary, alternate = load_corpus(args.source, args.spill_relation)
    model, epoch_losses, training_seconds = train(vectors, primary, alternate, device)
    query_vectors, truth = load_queries(args.development_query, args.development_ground_truth)
    screen = evaluate(
        model,
        query_vectors,
        truth,
        feature_ids,
        primary,
        alternate,
        np.arange(0, QUERIES, 4),
        device,
        990_000,
        700_000,
    )
    development = None
    if screen["passed"]:
        development = evaluate(
            model,
            query_vectors,
            truth,
            feature_ids,
            primary,
            alternate,
            np.arange(QUERIES),
            device,
            995_000,
            800_000,
        )
    parameter_count = sum(parameter.numel() for parameter in model.parameters())
    result = {
        "schema": "borsuk-v53-algorithm-first-page-mlp-result-v1",
        "claim_eligible": False,
        "training_lineage": "corpus-rows-and-query-blind-primary-replica-page-labels-only",
        "query_gt_used_for_training": False,
        "source_rows": ROWS,
        "dimensions": DIMENSIONS,
        "page_count": PAGES,
        "selected_pages": SELECTED_PAGES,
        "seed": SEED,
        "epochs": EPOCHS,
        "batch": BATCH,
        "network": [DIMENSIONS, 256, 128, PAGES],
        "parameter_count": parameter_count,
        "parameter_bytes_fp32": parameter_count * 4,
        "training_device": str(device),
        "training_elapsed_ms": int(training_seconds * 1000),
        "epoch_losses": epoch_losses,
        "screen": screen,
        "development": development,
        "validation_opened": False,
    }
    payload = (json.dumps(result, sort_keys=True, separators=(",", ":")) + "\n").encode()
    args.output.write_bytes(payload)
    print(json.dumps({"result_sha256": hashlib.sha256(payload).hexdigest(), **result}), flush=True)


if __name__ == "__main__":
    main()
