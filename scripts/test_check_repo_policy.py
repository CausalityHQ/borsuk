import contextlib
import io
import unittest
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent))
import check_repo_policy


class BenchmarkArtifactPolicyTests(unittest.TestCase):
    def test_local_benchmark_gate_rejects_weak_id_recall_threshold(self) -> None:
        benchmark_text = (
            "use borsuk::recall_at_k;\n"
            "fn assert_approx_report() {\n"
            "    assert!(recall_at_k(&exact_ids, &approx_ids, 10).expect(\"recall\") >= 0.1);\n"
            "}\n"
        )

        stderr = io.StringIO()
        with contextlib.redirect_stderr(stderr), self.assertRaises(SystemExit):
            check_repo_policy.assert_local_benchmark_recall_gate(benchmark_text)
        self.assertIn("tie-aware recall", stderr.getvalue())

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

    def test_numeric_gate_rejects_missing_parallel_memory_evidence(self) -> None:
        csv_text = (
            "dataset,mode,parallelism,tie_aware_recall_at_10,avg_graph_bytes_read,p95_ms,qps\n"
            "synthetic-uniform,vamana-pq,8,1.000000,49152,12.5,300\n"
        )

        stderr = io.StringIO()
        with contextlib.redirect_stderr(stderr), self.assertRaises(SystemExit):
            check_repo_policy.assert_benchmark_numeric_rows(
                "parallel.csv",
                csv_text,
                [
                    {
                        "dataset": "synthetic-uniform",
                        "mode": "vamana-pq",
                        "parallelism": "8",
                    }
                ],
                {"avg_graph_bytes_read": 1.0, "rss_peak_delta": 1.0},
            )
        self.assertIn("rss_peak_delta", stderr.getvalue())

    def test_numeric_gate_rejects_non_positive_graph_pressure(self) -> None:
        csv_text = (
            "dataset,mode,parallelism,tie_aware_recall_at_10,avg_graph_bytes_read,rss_peak_delta\n"
            "synthetic-uniform,vamana-pq,8,1.000000,0,4096\n"
        )

        stderr = io.StringIO()
        with contextlib.redirect_stderr(stderr), self.assertRaises(SystemExit):
            check_repo_policy.assert_benchmark_numeric_rows(
                "parallel.csv",
                csv_text,
                [
                    {
                        "dataset": "synthetic-uniform",
                        "mode": "vamana-pq",
                        "parallelism": "8",
                    }
                ],
                {"avg_graph_bytes_read": 1.0, "rss_peak_delta": 1.0},
            )
        self.assertIn("avg_graph_bytes_read", stderr.getvalue())

    def test_numeric_gate_accepts_parallel_memory_evidence(self) -> None:
        csv_text = (
            "dataset,mode,parallelism,tie_aware_recall_at_10,avg_graph_bytes_read,rss_peak_delta\n"
            "synthetic-uniform,vamana-pq,8,1.000000,49152,4096\n"
        )

        check_repo_policy.assert_benchmark_numeric_rows(
            "parallel.csv",
            csv_text,
            [
                {
                    "dataset": "synthetic-uniform",
                    "mode": "vamana-pq",
                    "parallelism": "8",
                }
            ],
            {"avg_graph_bytes_read": 1.0, "rss_peak_delta": 1.0},
        )

    def test_scale_scope_rejects_unbacked_million_row_claims(self) -> None:
        scale_csv = (
            "family,dataset,mode,records\n"
            "synthetic-uniform,synthetic-uniform-n10000,pq-scan,10000\n"
            "synthetic-uniform,synthetic-uniform-n100000,pq-scan,100000\n"
        )
        docs_text = (
            "cargo run --locked --release -p borsuk --example benchmark_report -- "
            "--synthetic-records-list 10000,100000,1000000\n"
            "The benchmark report must include synthetic datasets at 10k, 100k, and 1M record counts."
        )

        stderr = io.StringIO()
        with contextlib.redirect_stderr(stderr), self.assertRaises(SystemExit):
            check_repo_policy.assert_scale_scope_matches_docs(scale_csv, docs_text)
        self.assertIn("scale.csv does not contain 1M rows", stderr.getvalue())

    def test_typescript_interface_gate_rejects_duplicate_fields(self) -> None:
        ts_text = (
            "export interface IndexStats {\n"
            "  routingMaxLevel: number;\n"
            "  routingPageFanout: number;\n"
            "  routingMaxLevel: number;\n"
            "}\n"
        )

        stderr = io.StringIO()
        with contextlib.redirect_stderr(stderr), self.assertRaises(SystemExit):
            check_repo_policy.assert_typescript_interfaces_have_unique_fields(
                "packages/borsuk/src/index.ts",
                ts_text,
            )
        self.assertIn("duplicate TypeScript interface field", stderr.getvalue())

    def test_typescript_docs_gate_rejects_positional_numeric_search_options(self) -> None:
        docs_text = "const ids = await index.searchIds([0.1, 0], 1);\n"

        stderr = io.StringIO()
        with contextlib.redirect_stderr(stderr), self.assertRaises(SystemExit):
            check_repo_policy.assert_no_positional_numeric_typescript_search_options(
                "README.md",
                docs_text,
            )
        self.assertIn("options object", stderr.getvalue())

    def test_typescript_docs_gate_rejects_duplicate_example_keys(self) -> None:
        docs_text = (
            "const rebuild = await index.rebuild({\n"
            "  sourceLevel: 0,\n"
            "  sourceLevel: 0,\n"
            "  targetLevel: 1\n"
            "});\n"
        )

        stderr = io.StringIO()
        with contextlib.redirect_stderr(stderr), self.assertRaises(SystemExit):
            check_repo_policy.assert_typescript_examples_have_unique_object_keys(
                "packages/borsuk/README.md",
                docs_text,
            )
        self.assertIn("duplicate TypeScript example object key", stderr.getvalue())


if __name__ == "__main__":
    unittest.main()
