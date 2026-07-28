import unittest

from scripts.validate_reported_comparisons import validate_rows


def valid_row(**overrides):
    row = {
        "evidence_id": "vendor-1",
        "system": "vendor",
        "evidence_class": "vendor-reported",
        "primary_source_url": "https://vendor.example/performance",
        "access_date": "2026-07-26",
        "dataset": "not-reported",
        "metric": "p95-latency",
        "k": "not-reported",
        "cache_state": "warm",
        "latency_scope": "not-reported",
        "reported_value": "100",
        "unit": "ms",
        "logical_cpus": "not-reported",
        "ram_bytes": "not-reported",
        "accelerator": "not-reported",
        "storage_class": "not-reported",
        "mismatch_reasons": "dataset;hardware;scope",
        "permitted_wording": "context-only",
    }
    row.update(overrides)
    return row


class ValidateReportedComparisonsTests(unittest.TestCase):
    def test_accepts_vendor_or_paper_context_rows(self):
        validate_rows(
            [
                valid_row(),
                valid_row(
                    evidence_id="paper-1",
                    evidence_class="paper-reported",
                    primary_source_url="https://arxiv.org/abs/2111.08566",
                ),
            ]
        )

    def test_rejects_direct_rows_missing_fields_and_non_primary_urls(self):
        with self.assertRaises(ValueError):
            validate_rows([valid_row(evidence_class="direct-controlled")])
        with self.assertRaises(ValueError):
            validate_rows([valid_row(dataset="")])
        with self.assertRaises(ValueError):
            validate_rows([valid_row(primary_source_url="search-results")])

    def test_unknown_comparability_forbids_superiority_wording(self):
        with self.assertRaisesRegex(ValueError, "superiority"):
            validate_rows([valid_row(permitted_wording="borsuk-is-faster")])


if __name__ == "__main__":
    unittest.main()
