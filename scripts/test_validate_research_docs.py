import csv
import tempfile
import unittest
from pathlib import Path

from validate_research_docs import (
    LEAF_METHODS,
    STANDARD_DATASETS,
    validate_repository,
)


class ResearchDocsValidatorTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        (self.root / "docs/research").mkdir(parents=True)
        assets = self.root / "docs/web/assets/benchmarks"
        assets.mkdir(parents=True)
        resources = assets / "resources"
        resources.mkdir()

        for name in (
            "README.md",
            "standard-datasets.md",
            "methods.md",
            "configuration-ablation.md",
            "scale-and-workloads.md",
            "systems-comparison.md",
            "cost-and-deployment.md",
            "reproducibility.md",
        ):
            (self.root / "docs/research" / name).write_text("# Research\n")
        (self.root / "README.md").write_text(
            "# BORSUK\n[Research](docs/research/README.md)\n"
        )
        (self.root / "docs/api.md").write_text("# API\n")
        (self.root / "docs/architecture.md").write_text("# Architecture\n")
        (self.root / "docs/benchmarks.md").write_text(
            "# Production benchmark contract\n"
        )
        (self.root / "docs/deployment-and-integrations.md").write_text(
            "file:// s3:// cache_dir LangChain TypeScript S3-compatible\n"
        )
        (self.root / "docs/web/docs.html").parent.mkdir(parents=True, exist_ok=True)
        (self.root / "docs/web/docs.html").write_text("<h1>Docs</h1>")

        self._write_csv(
            assets / "aws-production-profiles.csv",
            [
                "dataset",
                "leaf_mode",
                "global_cap",
                "width",
                "recall_at_10",
                "uncached_p95_ms",
                "disk_cached_p95_ms",
                "experiment_peak_rss_mib",
            ],
            [
                [dataset, "pq-scan", "24", "8", "0.99", "10", "2", "100"]
                for dataset in STANDARD_DATASETS
            ],
        )
        self._write_csv(
            assets / "aws-recall-latency-2026-07-20.csv",
            [
                "dataset",
                "nprobe",
                "max_candidates",
                "recall_at_10",
                "p95_ms",
                "cache_state",
            ],
            [
                [dataset, "1", "16", "0.95", "10", "memory_preloaded"]
                for dataset in STANDARD_DATASETS
            ],
        )
        self._write_csv(
            assets / "aws-uncapped-research-2026-07-20.csv",
            ["dataset", "workers", "qps", "p95_ms", "experiment_peak_rss_mib"],
            [[dataset, "1", "1", "10", "100"] for dataset in STANDARD_DATASETS],
        )
        self._write_csv(
            assets / "sequential.csv",
            ["dataset", "mode", "tie_aware_recall_at_10", "p95_ms"],
            [["sklearn-digits", method, "1.0", "10"] for method in LEAF_METHODS],
        )
        self._write_csv(
            assets / "resource-schema.csv",
            [
                "elapsed_ms",
                "cpu_percent",
                "rss_bytes",
                "vms_bytes",
                "process_read_bytes",
                "process_write_bytes",
                "cache_disk_bytes",
            ],
            [["0", "1", "2", "3", "4", "5", "6"]],
        )
        for dataset in STANDARD_DATASETS:
            (resources / f"production-{dataset}.svg").write_text("<svg/>")
            recall_dir = assets / "recall-latency"
            recall_dir.mkdir(exist_ok=True)
            (recall_dir / f"recall-latency-{dataset}.svg").write_text("<svg/>")
        self._write_csv(
            assets / "aws-cost-model-2026-07-20.csv",
            [
                "dataset",
                "index_bytes",
                "storage_usd_per_month",
                "uncached_gets_per_query",
                "uncached_get_usd_per_million_queries",
                "disk_cached_backing_gets_per_query",
                "price_region",
                "price_snapshot_date",
            ],
            [
                [
                    dataset,
                    "1000",
                    "0.01",
                    "2",
                    "0.86",
                    "0",
                    "eu-central-1",
                    "2026-07-20",
                ]
                for dataset in STANDARD_DATASETS
            ],
        )
        self._write_csv(
            assets / "current-results.csv",
            [
                "claim_id",
                "dataset",
                "method",
                "index_capability",
                "cache_state",
                "queries",
                "source_artifact",
                "status",
            ],
            [
                [
                    "fashion-production-current",
                    "fashion-mnist-784",
                    "pq-scan",
                    "pq-scan-only",
                    "disk_cached",
                    "100",
                    "docs/web/assets/benchmarks/aws-production-profiles.csv",
                    "current",
                ]
            ],
        )
        matrix = assets / "standard-method-matrix"
        matrix.mkdir()
        self._write_csv(
            matrix / "coverage.csv",
            ["dataset", "method", "status", "resource_path"],
            [
                [
                    dataset,
                    method,
                    "planned",
                    f"results/{dataset}/{method}/resources.csv",
                ]
                for dataset in STANDARD_DATASETS
                for method in LEAF_METHODS
            ],
        )
        documented = "\n".join(
            path.name
            for path in assets.glob("*.csv")
            if path.name != "resource-schema.csv"
        )
        (self.root / "docs/research/README.md").write_text(
            f"# Research\n{documented}\npq-scan\nbounded rerank-read gate\n"
            '<span data-result-status="current" '
            'data-claim-id="fashion-production-current">Fashion 0.99</span>\n'
        )
        (self.root / "docs/research/cost-and-deployment.md").write_text(
            "aws-cost-model-2026-07-20.csv common application/client compute "
            "US $100,000 2030-07-02 BUSL MIT\n"
        )
        (self.root / "docs/publication-notes.md").write_text(
            "TurboQuant-4b SRHT SIMD runtime dispatch scalar fallback novelty prior art\n"
        )

    def tearDown(self):
        self.temp.cleanup()

    @staticmethod
    def _write_csv(path, columns, rows):
        with path.open("w", newline="") as handle:
            writer = csv.writer(handle)
            writer.writerow(columns)
            writer.writerows(rows)

    def test_accepts_complete_evidence_contract(self):
        self.assertEqual(validate_repository(self.root), [])

    def test_reports_missing_standard_dataset(self):
        path = self.root / "docs/web/assets/benchmarks/aws-production-profiles.csv"
        with path.open() as handle:
            rows = list(csv.DictReader(handle))[:-1]
        self._write_csv(path, rows[0].keys(), [row.values() for row in rows])
        errors = validate_repository(self.root)
        self.assertTrue(any(STANDARD_DATASETS[-1] in error for error in errors))

    def test_reports_missing_leaf_method(self):
        path = self.root / "docs/web/assets/benchmarks/sequential.csv"
        with path.open() as handle:
            rows = list(csv.DictReader(handle))[:-1]
        self._write_csv(path, rows[0].keys(), [row.values() for row in rows])
        errors = validate_repository(self.root)
        self.assertTrue(any(LEAF_METHODS[-1] in error for error in errors))

    def test_reports_incomplete_resource_schema(self):
        path = self.root / "docs/web/assets/benchmarks/resource-schema.csv"
        self._write_csv(path, ["elapsed_ms", "rss_bytes"], [["0", "1"]])
        errors = validate_repository(self.root)
        self.assertTrue(any("cpu_percent" in error for error in errors))

    def test_reports_broken_local_research_link(self):
        (self.root / "docs/research/README.md").write_text("[missing](missing.md)\n")
        errors = validate_repository(self.root)
        self.assertTrue(any("missing.md" in error for error in errors))

    def test_rejects_research_tables_in_default_docs(self):
        (self.root / "docs/api.md").write_text("## Uncapped research ceiling\n")
        errors = validate_repository(self.root)
        self.assertTrue(any("docs/api.md" in error for error in errors))

    def test_rejects_measured_matrix_cell_without_result_and_resources(self):
        path = (
            self.root / "docs/web/assets/benchmarks/standard-method-matrix/coverage.csv"
        )
        with path.open() as handle:
            rows = list(csv.DictReader(handle))
        rows[0]["status"] = "measured"
        self._write_csv(path, rows[0].keys(), [row.values() for row in rows])
        errors = validate_repository(self.root)
        self.assertTrue(any("measured matrix cell" in error for error in errors))

    def test_reports_undocumented_top_level_artifact(self):
        path = self.root / "docs/web/assets/benchmarks/orphan.csv"
        self._write_csv(path, ["value"], [["1"]])
        errors = validate_repository(self.root)
        self.assertTrue(any("orphan.csv" in error for error in errors))

    def test_rejects_non_production_leaf_default(self):
        path = self.root / "docs/web/assets/benchmarks/aws-production-profiles.csv"
        with path.open() as handle:
            rows = list(csv.DictReader(handle))
        rows[0]["leaf_mode"] = "graph"
        self._write_csv(path, rows[0].keys(), [row.values() for row in rows])
        errors = validate_repository(self.root)
        self.assertTrue(any("pq-scan" in error for error in errors))

    def test_rejects_unbounded_production_decode_cap(self):
        path = self.root / "docs/web/assets/benchmarks/aws-production-profiles.csv"
        with path.open() as handle:
            rows = list(csv.DictReader(handle))
        rows[0]["global_cap"] = "0"
        self._write_csv(path, rows[0].keys(), [row.values() for row in rows])
        errors = validate_repository(self.root)
        self.assertTrue(
            any("bounded production global decode cap" in error for error in errors)
        )

    def test_requires_current_global_rerank_gate_prose(self):
        path = self.root / "docs/research/README.md"
        path.write_text(
            path.read_text().replace(
                "bounded rerank-read gate", "unbounded rerank reads"
            )
        )
        errors = validate_repository(self.root)
        self.assertTrue(any("bounded rerank-read gate" in error for error in errors))

    def test_requires_recall_latency_chart_for_every_dataset(self):
        chart = (
            self.root
            / "docs/web/assets/benchmarks/recall-latency"
            / f"recall-latency-{STANDARD_DATASETS[-1]}.svg"
        )
        chart.unlink()
        errors = validate_repository(self.root)
        self.assertTrue(any(str(chart) in error for error in errors))

    def test_requires_precise_license_and_common_compute_boundary(self):
        path = self.root / "docs/research/cost-and-deployment.md"
        path.write_text("free and serverless\n")
        errors = validate_repository(self.root)
        self.assertTrue(
            any("common application/client compute" in error for error in errors)
        )
        self.assertTrue(any("US $100,000" in error for error in errors))

    def test_rejects_obsolete_memory_marketing_claims(self):
        (self.root / "README.md").write_text("near-zero RAM and a few hundred bytes\n")
        errors = validate_repository(self.root)
        self.assertTrue(any("obsolete production claim" in error for error in errors))

    def test_rejects_current_claim_with_missing_source_artifact(self):
        path = self.root / "docs/web/assets/benchmarks/current-results.csv"
        with path.open() as handle:
            rows = list(csv.DictReader(handle))
        rows[0]["source_artifact"] = "docs/web/assets/benchmarks/missing.csv"
        self._write_csv(path, rows[0].keys(), [row.values() for row in rows])

        errors = validate_repository(self.root)

        self.assertTrue(any("missing source artifact" in error for error in errors))

    def test_rejects_current_rendering_backed_by_historical_manifest_row(self):
        path = self.root / "docs/web/assets/benchmarks/current-results.csv"
        with path.open() as handle:
            rows = list(csv.DictReader(handle))
        rows[0]["status"] = "historical"
        self._write_csv(path, rows[0].keys(), [row.values() for row in rows])

        errors = validate_repository(self.root)

        self.assertTrue(any("rendered current" in error for error in errors))

    def test_rejects_public_current_claim_below_full_query_count(self):
        path = self.root / "docs/web/assets/benchmarks/current-results.csv"
        with path.open() as handle:
            rows = list(csv.DictReader(handle))
        rows[0]["queries"] = "99"
        self._write_csv(path, rows[0].keys(), [row.values() for row in rows])

        errors = validate_repository(self.root)

        self.assertTrue(any("at least 100 queries" in error for error in errors))

    def test_rejects_numeric_claim_id_absent_from_manifest(self):
        page = self.root / "docs/web/docs.html"
        page.write_text(
            '<p data-result-status="current" data-claim-id="unknown">12.3 ms</p>'
        )

        errors = validate_repository(self.root)

        self.assertTrue(any("unknown claim id `unknown`" in error for error in errors))

    def test_rejects_stale_glove_latency_card_without_historical_label(self):
        page = self.root / "docs/web/docs.html"
        page.write_text(
            "<p>GloVe recall 0.98, 256 candidates, 28 MB loaded, 2.0 s latency</p>"
        )

        errors = validate_repository(self.root)

        self.assertTrue(
            any("stale GloVe result signature" in error for error in errors)
        )


if __name__ == "__main__":
    unittest.main()
