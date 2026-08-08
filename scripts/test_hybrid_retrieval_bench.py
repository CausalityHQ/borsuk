import csv
import json
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PREPARE = ROOT / "scripts" / "prepare_hybrid_dataset.py"
EXPECTED_MODES = {
    "dense",
    "sparse",
    "text",
    "dense+sparse",
    "dense+text",
    "sparse+text",
    "dense+sparse+text",
}


class HybridRetrievalBenchTests(unittest.TestCase):
    def test_build_and_all_mode_query_emit_publication_distributions(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            source = root / "source"
            (source / "qrels").mkdir(parents=True)
            corpus = [
                {"_id": "d1", "title": "Apple", "text": "red orchard fruit"},
                {"_id": "d2", "title": "Banana", "text": "yellow tropical fruit"},
                {"_id": "d3", "title": "Car", "text": "road vehicle engine"},
                {"_id": "d4", "title": "Fruit", "text": "apple banana salad"},
            ]
            queries = [
                {"_id": "q1", "text": "apple orchard"},
                {"_id": "q2", "text": "banana fruit"},
            ]
            (source / "corpus.jsonl").write_text(
                "".join(json.dumps(row) + "\n" for row in corpus),
                encoding="utf-8",
            )
            (source / "queries.jsonl").write_text(
                "".join(json.dumps(row) + "\n" for row in queries),
                encoding="utf-8",
            )
            (source / "qrels" / "test.tsv").write_text(
                "query-id\tcorpus-id\tscore\nq1\td1\t2\nq1\td4\t1\nq2\td2\t2\nq2\td4\t1\n",
                encoding="utf-8",
            )
            prepared = root / "prepared"
            subprocess.run(
                [
                    sys.executable,
                    str(PREPARE),
                    "--source",
                    str(source),
                    "--output",
                    str(prepared),
                    "--dataset",
                    "fixture",
                    "--dense-backend",
                    "hash",
                    "--dense-dimensions",
                    "16",
                ],
                cwd=ROOT,
                check=True,
                capture_output=True,
                text=True,
            )

            index = root / "index"
            output = root / "results"
            env = os.environ.copy()
            env["CARGO_INCREMENTAL"] = "0"
            env.update(
                {
                    "BORSUK_HYBRID_DATASET": str(prepared),
                    "BORSUK_HYBRID_INDEX_URI": str(index),
                    "BORSUK_HYBRID_OUTPUT": str(output),
                    "BORSUK_HYBRID_SCAN_CODEC": "pq-scan",
                    "BORSUK_HYBRID_SEGMENT_MAX": "2",
                    "BORSUK_HYBRID_BATCH_SIZE": "2",
                    "BORSUK_HYBRID_REPETITIONS": "2",
                    "BORSUK_HYBRID_QUERY_SEED": "17",
                    "BORSUK_HYBRID_K": "4",
                    "BORSUK_HYBRID_CANDIDATE_DEPTH": "4",
                    "BORSUK_HYBRID_MAX_SEGMENTS": "8",
                    "BORSUK_HYBRID_FUSION": "rrf",
                }
            )
            build = subprocess.run(
                [
                    "cargo",
                    "run",
                    "--quiet",
                    "--example",
                    "hybrid_retrieval_bench",
                    "--",
                    "build",
                ],
                cwd=ROOT,
                env=env,
                check=False,
                capture_output=True,
                text=True,
            )
            self.assertEqual(
                build.returncode,
                0,
                f"hybrid build failed\nstdout:\n{build.stdout}\nstderr:\n{build.stderr}",
            )
            query = subprocess.run(
                [
                    "cargo",
                    "run",
                    "--quiet",
                    "--example",
                    "hybrid_retrieval_bench",
                    "--",
                    "query",
                ],
                cwd=ROOT,
                env=env,
                check=False,
                capture_output=True,
                text=True,
            )
            self.assertEqual(
                query.returncode,
                0,
                f"hybrid query failed\nstdout:\n{query.stdout}\nstderr:\n{query.stderr}",
            )

            with (output / "hybrid_build.csv").open(newline="") as handle:
                build_rows = list(csv.DictReader(handle))
            self.assertEqual(len(build_rows), 1)
            self.assertEqual(build_rows[0]["dataset"], "fixture")
            self.assertEqual(int(build_rows[0]["documents"]), 4)

            with (output / "hybrid_summary.csv").open(newline="") as handle:
                summary_rows = list(csv.DictReader(handle))
            self.assertEqual({row["mode"] for row in summary_rows}, EXPECTED_MODES)
            for row in summary_rows:
                self.assertEqual(int(row["samples"]), 4)
                self.assertIn("stddev_ms", row)
                self.assertIn("p99_ms", row)
                self.assertIn("ndcg_at_10", row)
                self.assertIn("recall_at_10", row)
                self.assertIn("precision_at_10", row)
                self.assertIn("mrr_at_10", row)
                self.assertGreaterEqual(float(row["mean_ms"]), 0.0)
                self.assertGreaterEqual(float(row["stddev_ms"]), 0.0)
                self.assertGreaterEqual(float(row["ndcg_at_10"]), 0.0)
                self.assertLessEqual(float(row["ndcg_at_10"]), 1.0)

            with (output / "hybrid_queries.csv").open(newline="") as handle:
                query_rows = list(csv.DictReader(handle))
            self.assertEqual(len(query_rows), 7 * 2 * 2)
            self.assertEqual({row["mode"] for row in query_rows}, EXPECTED_MODES)
            self.assertIn("disk_cache_bytes_read", query_rows[0])
            self.assertIn("backing_bytes_read", query_rows[0])
            self.assertIn("observed_cache_tier", query_rows[0])
            self.assertIn("target_hot_query_fraction", query_rows[0])
            self.assertIn("query_class", query_rows[0])
            self.assertIn("candidate_depth", query_rows[0])
            self.assertIn("max_segments", query_rows[0])
            self.assertIn("fusion", query_rows[0])
            self.assertIn("query_seed", query_rows[0])
            self.assertEqual({row["query_seed"] for row in query_rows}, {"17"})

    def test_source_contract_exposes_seeded_query_permutation(self) -> None:
        source = (ROOT / "crates/borsuk/examples/hybrid_retrieval_bench.rs").read_text(
            encoding="utf-8"
        )
        self.assertIn("BORSUK_HYBRID_QUERY_SEED", source)
        self.assertIn("query_seed", source)
        self.assertIn("permuted_positions", source)


if __name__ == "__main__":
    unittest.main()
