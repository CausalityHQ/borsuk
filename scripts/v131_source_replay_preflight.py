"""Check sealed V122 query and truth identity before production-source replay."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import numpy as np


def verify(queries: Path, truth: Path, evidence: Path, summary: Path) -> None:
    expected = np.load(truth, allow_pickle=False)
    query_rows = [json.loads(line) for line in queries.read_text().splitlines()]
    evidence_rows = [json.loads(line) for line in evidence.read_text().splitlines()]
    baseline = json.loads(summary.read_text())
    if (expected.shape != (1000, 100) or len(query_rows) != 1000
            or len(evidence_rows) != 1000
            or baseline.get("returned_hits") != {"candidate": 99106, "baseline": 98203}
            or baseline.get("query_count") != 1000):
        raise ValueError("sealed V122 cohort or SQ8 baseline differs")
    hit_sums = {"candidate": 0, "baseline": 0}
    for ordinal, (query, row, gold) in enumerate(zip(query_rows, evidence_rows, expected)):
        if (query.get("query_ordinal") != ordinal
                or row.get("query_ordinal") != ordinal
                or query.get("source_query_ordinal") != 9000 + ordinal
                or row.get("source_query_ordinal") != 9000 + ordinal
                or row.get("truth_ids") != gold.tolist()
                or len(query.get("query", [])) != 96
                or len(row.get("nominees", [])) != 512):
            raise ValueError(f"sealed V122 query/truth mismatch at {ordinal}")
        for arm in ("candidate", "baseline"):
            returned = row.get(f"{arm}_returned_ids", [])
            hits = len(set(returned).intersection(map(int, gold)))
            if (len(returned) != 100 or len(set(returned)) != 100
                    or hits != row.get(f"{arm}_hits")):
                raise ValueError(f"sealed V122 {arm} evidence mismatch at {ordinal}")
            hit_sums[arm] += hits
    if hit_sums != baseline["returned_hits"]:
        raise ValueError("sealed V122 SQ8 hit totals differ")


def main() -> None:
    parser = argparse.ArgumentParser()
    for name in ("queries", "truth", "evidence", "summary"):
        parser.add_argument(name, type=Path)
    args = parser.parse_args()
    verify(args.queries, args.truth, args.evidence, args.summary)
    print("sealed V122 1000-query identity and SQ8 baseline verified")


if __name__ == "__main__":
    main()
