"""Measure how far the V63 greedy page oracle can fall below the true optimum."""
import itertools
import importlib.util
import numpy as np

spec = importlib.util.spec_from_file_location(
    "v63", "/home/rb/worktrees/borsuk-prod-ready-v9/scripts/v63_algorithm_first_layout_oracle.py"
)
v63 = importlib.util.module_from_spec(spec)
spec.loader.exec_module(v63)

ROWS, QUERIES, NEIGHBORS, DIM, CLUSTERS = 8_000, 60, 20, 24, 48
v63.ROWS, v63.QUERIES, v63.NEIGHBORS, v63.DIMENSIONS = ROWS, QUERIES, NEIGHBORS, DIM
v63.MAXIMUM_REPLICATION = 4

rng = np.random.default_rng(909)
anchors = rng.standard_normal((18, DIM), dtype=np.float32) * 3.5
vectors = (anchors[rng.integers(0, 18, ROWS)] + rng.standard_normal((ROWS, DIM), dtype=np.float32)).astype(np.float32)
queries = (anchors[rng.integers(0, 18, QUERIES)] + rng.standard_normal((QUERIES, DIM), dtype=np.float32)).astype(np.float32)
norms = np.einsum("ij,ij->i", vectors, vectors)
truth_rows = np.stack([
    np.argsort(norms - 2.0 * (vectors @ queries[i]), kind="stable")[:NEIGHBORS] for i in range(QUERIES)
]).astype(np.int32)

partition = v63.CoarsePartition(vectors, CLUSTERS, seed=3)
worst_gap = 0.0
total_greedy = total_optimal = 0
for replication in (2, 4):
    for page_rows in (32, 64):
        posting = partition.replicated_pages(replication, page_rows)
        for budget in (2, 3, 4):
            v63.PAGE_BUDGET_LADDER = (budget,)
            cell = v63.evaluate_replicated("t", posting, truth_rows, replication, page_rows)
            greedy_ppm = cell["curve"][0]["aggregate_oracle_recall_ppm"]
            optimal_hits = 0
            for q in range(QUERIES):
                entries = posting.row_pages[truth_rows[q]]
                pages = np.unique(entries)
                best = 0
                # Exhaustive optimum over the pages that hold any GT row.
                for combo in itertools.combinations(pages.tolist(), min(budget, pages.size)):
                    chosen = set(combo)
                    hit = sum(1 for row in entries if any(int(p) in chosen for p in row))
                    best = max(best, hit)
                optimal_hits += best
            optimal_ppm = int(round(optimal_hits * 1_000_000 / (QUERIES * NEIGHBORS)))
            total_greedy += greedy_ppm
            total_optimal += optimal_ppm
            gap = (optimal_ppm - greedy_ppm) / 10_000
            worst_gap = max(worst_gap, gap)
            print(f"r={replication} rows={page_rows} budget={budget}: "
                  f"greedy={greedy_ppm/10_000:.3f}% optimal={optimal_ppm/10_000:.3f}% gap={gap:.4f}pp")
print(f"worst greedy shortfall {worst_gap:.4f} percentage points")
