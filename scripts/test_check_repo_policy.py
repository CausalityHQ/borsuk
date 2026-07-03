import contextlib
import io
import unittest
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent))
import check_repo_policy


class BenchmarkArtifactPolicyTests(unittest.TestCase):
    def test_recall_gate_rejects_low_tie_aware_recall(self) -> None:
        csv_text = (
            "dataset,mode,records,tie_aware_recall_at_10,id_recall_at_10\n"
            "synthetic-uniform,pq-scan,100000,0.940000,1.000000\n"
        )

        stderr = io.StringIO()
        with contextlib.redirect_stderr(stderr), self.assertRaises(SystemExit):
            check_repo_policy.assert_benchmark_recall_rows(
                "scale.csv",
                csv_text,
                [
                    {
                        "dataset": "synthetic-uniform",
                        "mode": "pq-scan",
                        "records": "100000",
                    }
                ],
            )
        self.assertIn("tie-aware recall", stderr.getvalue())

    def test_recall_gate_requires_expected_rows(self) -> None:
        csv_text = (
            "dataset,mode,records,tie_aware_recall_at_10,id_recall_at_10\n"
            "synthetic-uniform,pq-scan,100000,1.000000,1.000000\n"
        )

        stderr = io.StringIO()
        with contextlib.redirect_stderr(stderr), self.assertRaises(SystemExit):
            check_repo_policy.assert_benchmark_recall_rows(
                "scale.csv",
                csv_text,
                [
                    {
                        "dataset": "synthetic-clustered",
                        "mode": "hybrid",
                        "records": "100000",
                    }
                ],
            )
        self.assertIn("required benchmark row", stderr.getvalue())

    def test_recall_gate_accepts_required_high_recall_rows(self) -> None:
        csv_text = (
            "dataset,mode,records,tie_aware_recall_at_10,id_recall_at_10\n"
            "synthetic-uniform,pq-scan,100000,0.950000,0.900000\n"
        )

        check_repo_policy.assert_benchmark_recall_rows(
            "scale.csv",
            csv_text,
            [
                {
                    "dataset": "synthetic-uniform",
                    "mode": "pq-scan",
                    "records": "100000",
                }
            ],
        )


if __name__ == "__main__":
    unittest.main()
