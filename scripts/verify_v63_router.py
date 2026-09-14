"""Cross-check the vectorised V63 router against a naive reference."""
import importlib.util
import sys
import numpy as np

spec = importlib.util.spec_from_file_location(
    "v63", "/home/rb/worktrees/borsuk-prod-ready-v9/scripts/v63_algorithm_first_layout_oracle.py"
)
v63 = importlib.util.module_from_spec(spec)
spec.loader.exec_module(v63)

ROWS, QUERIES, NEIGHBORS, DIM, CLUSTERS = 20_000, 40, 25, 32, 64
v63.ROWS, v63.QUERIES, v63.NEIGHBORS, v63.DIMENSIONS = ROWS, QUERIES, NEIGHBORS, DIM
v63.MAXIMUM_REPLICATION = 4
v63.PAGE_BUDGET_LADDER = (1, 2, 3, 5, 8, 13, 21, 34, 55, 89)

rng = np.random.default_rng(4242)
# Clustered, not uniform: a uniform corpus makes every layout look identical.
anchors = rng.standard_normal((24, DIM), dtype=np.float32) * 4.0
vectors = (anchors[rng.integers(0, 24, ROWS)] + rng.standard_normal((ROWS, DIM), dtype=np.float32)).astype(np.float32)
queries = (anchors[rng.integers(0, 24, QUERIES)] + rng.standard_normal((QUERIES, DIM), dtype=np.float32)).astype(np.float32)
norms = np.einsum("ij,ij->i", vectors, vectors)
truth_rows = np.stack([
    np.argsort(norms - 2.0 * (vectors @ queries[i]), kind="stable")[:NEIGHBORS] for i in range(QUERIES)
]).astype(np.int32)

partition = v63.CoarsePartition(vectors, CLUSTERS, seed=11)
order, rank = partition.probe_order(queries)

failures = 0
for replication in (1, 2, 4):
    for page_rows in (8, 16, 64):
        posting = partition.replicated_pages(replication, page_rows)
        fast = v63.evaluate_routed("t", partition, posting, queries, truth_rows, replication, page_rows)

        # Naive reference: literally walk posting lists in probe order,
        # collect page ids until the budget is spent, then test membership.
        for entry in fast["curve"]:
            budget = entry["pages"]
            total_hits = 0
            for q in range(QUERIES):
                selected = set()
                for cluster in order[q]:
                    count = int(posting.pages_by_cluster[cluster])
                    start = int(posting.start_by_cluster[cluster])
                    if count == 0:
                        continue
                    room = budget - len(selected)
                    if room <= 0:
                        break
                    selected.update(range(start, start + min(count, room)))
                found = sum(
                    1 for row in truth_rows[q]
                    if any(int(p) in selected for p in posting.row_pages[row])
                )
                total_hits += found
            reference = int(round(total_hits * 1_000_000 / (QUERIES * NEIGHBORS)))
            got = entry["aggregate_oracle_recall_ppm"]
            if reference != got:
                failures += 1
                print(f"MISMATCH r={replication} rows={page_rows} budget={budget}: fast={got} naive={reference}")
        print(f"ok r={replication} rows={page_rows} pages={posting.total_pages} "
              f"agg@32={[e['aggregate_oracle_recall_ppm'] for e in fast['curve'] if e['pages']==34]}")

print("FAILURES", failures)
sys.exit(1 if failures else 0)
