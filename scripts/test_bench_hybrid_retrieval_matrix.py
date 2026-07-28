import csv
import os
import subprocess
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts" / "bench_hybrid_retrieval_matrix.sh"


class BenchHybridRetrievalMatrixTests(unittest.TestCase):
    def test_dry_run_enumerates_mode_search_and_cache_fraction_matrix(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            datasets = root / "datasets"
            (datasets / "scifact").mkdir(parents=True)
            output = root / "results"
            env = os.environ.copy()
            env.update(
                {
                    "BORSUK_HYBRID_MATRIX_EXECUTE": "0",
                    "BORSUK_HYBRID_DATASETS_ROOT": str(datasets),
                    "BORSUK_HYBRID_MATRIX_DATASETS": "scifact",
                    "BORSUK_HYBRID_MATRIX_PROFILES": "srht",
                    "BORSUK_HYBRID_MATRIX_MODES": "dense sparse+text",
                    "BORSUK_HYBRID_SEARCH_POINTS": "128:32",
                    "BORSUK_HYBRID_HOT_FRACTIONS": "0 0.5",
                    "BORSUK_HYBRID_RRF_KS": "1 60",
                    "BORSUK_HYBRID_REPETITIONS": "1",
                    "OUT": str(output),
                }
            )
            subprocess.run(
                ["bash", str(SCRIPT)],
                cwd=ROOT,
                env=env,
                check=True,
                capture_output=True,
                text=True,
            )
            with (output / "coverage.csv").open(newline="") as handle:
                rows = list(csv.DictReader(handle))
            self.assertEqual(len(rows), 7)
            self.assertEqual(rows[0]["stage"], "build")
            query_rows = rows[1:]
            self.assertEqual(
                {row["mode"] for row in query_rows}, {"dense", "sparse+text"}
            )
            self.assertEqual(
                {row["target_hot_query_fraction"] for row in query_rows},
                {"0", "0.5"},
            )
            self.assertEqual({row["rrf_k"] for row in query_rows}, {"1", "60"})
            self.assertEqual(
                {row["rrf_k"] for row in query_rows if row["mode"] == "dense"},
                {"1"},
            )
            self.assertTrue(all(row["candidate_depth"] == "128" for row in query_rows))
            self.assertTrue(all(row["max_segments"] == "32" for row in query_rows))
            self.assertTrue(all(row["status"] == "planned" for row in rows))

    def test_dry_run_uses_independent_seeded_repetition_directories(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            datasets = root / "datasets"
            (datasets / "scifact").mkdir(parents=True)
            output = root / "results"
            env = os.environ.copy()
            env.update(
                {
                    "BORSUK_HYBRID_MATRIX_EXECUTE": "0",
                    "BORSUK_HYBRID_DATASETS_ROOT": str(datasets),
                    "BORSUK_HYBRID_MATRIX_DATASETS": "scifact",
                    "BORSUK_HYBRID_MATRIX_PROFILES": "srht",
                    "BORSUK_HYBRID_MATRIX_MODES": "dense",
                    "BORSUK_HYBRID_SEARCH_POINTS": "128:32",
                    "BORSUK_HYBRID_HOT_FRACTIONS": "0",
                    "BORSUK_HYBRID_REPETITIONS": "2",
                    "BORSUK_HYBRID_MASTER_SEED": "100",
                    "OUT": str(output),
                }
            )
            subprocess.run(["bash", str(SCRIPT)], cwd=ROOT, env=env, check=True)
            with (output / "coverage.csv").open(newline="") as handle:
                rows = list(csv.DictReader(handle))
            query_rows = [row for row in rows if row["stage"] == "query"]
            self.assertEqual(
                [row["campaign_repetition"] for row in query_rows], ["1", "2"]
            )
            self.assertEqual([row["query_seed"] for row in query_rows], ["101", "102"])
            self.assertEqual(len({row["artifact_dir"] for row in query_rows}), 2)
            self.assertTrue(query_rows[0]["artifact_dir"].endswith("repetition-1"))
            self.assertTrue(query_rows[1]["artifact_dir"].endswith("repetition-2"))

    def test_paid_mode_requires_explicit_guard_and_bucket(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            env = os.environ.copy()
            env.update(
                {
                    "BORSUK_HYBRID_MATRIX_EXECUTE": "1",
                    "BORSUK_HYBRID_DATASETS_ROOT": temporary,
                    "BORSUK_HYBRID_MATRIX_DATASETS": "scifact",
                    "OUT": str(Path(temporary) / "out"),
                }
            )
            completed = subprocess.run(
                ["bash", str(SCRIPT)],
                cwd=ROOT,
                env=env,
                capture_output=True,
                text=True,
            )
            self.assertNotEqual(completed.returncode, 0)
            self.assertIn("paid execution", completed.stderr.lower())


if __name__ == "__main__":
    unittest.main()
