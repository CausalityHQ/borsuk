import csv
import json
import tempfile
import unittest
from pathlib import Path

from analyze_wal_layout_qualification import (
    analyze,
    exact_bootstrap_median_interval,
)

PROTOCOL = {
    "repetitions": 3,
    "workloads": [{"name": "wide"}, {"name": "control"}],
    "backends": ["local-disk"],
    "promotion_gates": {
        "maximum_candidate_to_baseline_ingest_median_ratio": 1.05,
        "maximum_candidate_to_baseline_warm_query_p95_median_ratio": 1.05,
        "maximum_candidate_to_baseline_warm_query_p99_median_ratio": 1.05,
        "maximum_candidate_to_baseline_flush_median_ratio": 1.05,
        "maximum_candidate_to_baseline_peak_rss_median_ratio": 1.05,
        "maximum_candidate_to_baseline_cpu_core_ms_median_ratio": 1.05,
        "maximum_vortex_candidate_to_baseline_wal_bytes_median_ratio": 0.9,
        "maximum_vortex_candidate_to_baseline_first_query_median_ratio": 0.95,
        "maximum_vortex_candidate_to_baseline_first_query_bootstrap_high_95": 0.99,
        "maximum_parquet_control_wal_bytes_difference": 0,
    },
}


def case(
    repetition: int,
    workload: str,
    arm: str,
    *,
    expected_format: str,
    bytes_value: float,
    ingest: float,
    first: float,
    warm: float,
    warm_p99: float | None = None,
    flush: float,
    peak_rss: float = 1000,
    cpu_core_ms: float = 100,
) -> dict[str, str]:
    return {
        "repetition_id": f"r{repetition:02d}",
        "workload": workload,
        "backend": "local-disk",
        "arm": arm,
        "expected_candidate_format": expected_format,
        "wal_bytes": str(bytes_value),
        "ingest_ms": str(ingest),
        "first_query_ms": str(first),
        "warm_query_p95_ms": str(warm),
        "warm_query_p99_ms": str(warm if warm_p99 is None else warm_p99),
        "flush_ms": str(flush),
        "peak_rss_bytes": str(peak_rss),
        "cpu_core_ms": str(cpu_core_ms),
    }


def fixture_rows(candidate_ingest: float = 101.0) -> list[dict[str, str]]:
    rows = []
    for repetition in range(1, 4):
        rows.extend(
            [
                case(
                    repetition,
                    "wide",
                    "fixed-parquet",
                    expected_format="vortex",
                    bytes_value=1000,
                    ingest=100,
                    first=10,
                    warm=4,
                    flush=20,
                ),
                case(
                    repetition,
                    "wide",
                    "adaptive-candidate",
                    expected_format="vortex",
                    bytes_value=500,
                    ingest=candidate_ingest,
                    first=8,
                    warm=4,
                    flush=19,
                ),
                case(
                    repetition,
                    "control",
                    "fixed-parquet",
                    expected_format="parquet",
                    bytes_value=300,
                    ingest=20,
                    first=2,
                    warm=1,
                    flush=3,
                ),
                case(
                    repetition,
                    "control",
                    "adaptive-candidate",
                    expected_format="parquet",
                    bytes_value=300,
                    ingest=20,
                    first=2.1,
                    warm=1,
                    flush=3,
                ),
            ]
        )
    return rows


class AnalyzeWalLayoutQualificationTests(unittest.TestCase):
    def write_fixture(
        self, root: Path, rows: list[dict[str, str]]
    ) -> tuple[Path, Path]:
        cases = root / "cases.csv"
        with cases.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
            writer.writeheader()
            writer.writerows(rows)
        protocol = root / "protocol.json"
        protocol.write_text(json.dumps(PROTOCOL), encoding="utf-8")
        return cases, protocol

    def test_exact_bootstrap_interval_is_deterministic(self) -> None:
        self.assertEqual(
            exact_bootstrap_median_interval([1.0, 2.0, 3.0]),
            exact_bootstrap_median_interval([1.0, 2.0, 3.0]),
        )

    def test_all_paired_gates_promote(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            cases, protocol = self.write_fixture(Path(directory), fixture_rows())
            decisions, promotion = analyze(cases, protocol)
            self.assertTrue(promotion)
            self.assertEqual(decisions[-1]["scope"], "global")
            self.assertEqual(decisions[-1]["promotion_gate_pass"], "true")

    def test_regression_in_one_workload_rejects_globally(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            cases, protocol = self.write_fixture(
                Path(directory), fixture_rows(candidate_ingest=110.0)
            )
            decisions, promotion = analyze(cases, protocol)
            self.assertFalse(promotion)
            wide = next(row for row in decisions if row["workload"] == "wide")
            self.assertEqual(wide["ingest_gate_pass"], "false")
            self.assertEqual(decisions[-1]["promotion_gate_pass"], "false")

    def test_first_query_confidence_interval_must_be_below_baseline(self) -> None:
        rows = fixture_rows()
        for row in rows:
            if row["workload"] == "wide" and row["arm"] == "adaptive-candidate":
                row["first_query_ms"] = "10.1"
        with tempfile.TemporaryDirectory() as directory:
            cases, protocol = self.write_fixture(Path(directory), rows)
            decisions, promotion = analyze(cases, protocol)
            self.assertFalse(promotion)
            wide = next(row for row in decisions if row["workload"] == "wide")
            self.assertEqual(wide["first_query_confidence_gate_pass"], "false")

    def test_p99_cpu_and_rss_regressions_reject(self) -> None:
        rows = fixture_rows()
        for row in rows:
            if row["workload"] == "wide" and row["arm"] == "adaptive-candidate":
                row["warm_query_p99_ms"] = "5"
                row["peak_rss_bytes"] = "1200"
                row["cpu_core_ms"] = "120"
        with tempfile.TemporaryDirectory() as directory:
            cases, protocol = self.write_fixture(Path(directory), rows)
            decisions, promotion = analyze(cases, protocol)
            self.assertFalse(promotion)
            wide = next(row for row in decisions if row["workload"] == "wide")
            self.assertEqual(wide["warm_query_p99_gate_pass"], "false")
            self.assertEqual(wide["peak_rss_gate_pass"], "false")
            self.assertEqual(wide["cpu_gate_pass"], "false")


if __name__ == "__main__":
    unittest.main()
