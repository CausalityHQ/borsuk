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

    def test_benchmark_query_count_gate_rejects_shallow_artifacts(self) -> None:
        csv_text = "dataset,mode,queries\nsynthetic-uniform,pq-scan,10\n"

        stderr = io.StringIO()
        with contextlib.redirect_stderr(stderr), self.assertRaises(SystemExit):
            check_repo_policy.assert_benchmark_query_counts(
                "docs/web/assets/benchmarks/sequential.csv",
                csv_text,
                minimum_queries=100,
            )
        self.assertIn("must use at least 100 queries", stderr.getvalue())

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

    def test_byte_budget_docs_gate_requires_number_and_string_forms(self) -> None:
        docs_text = "`ramBudget` and `maxBytes` accept unit strings like `128MB`."

        stderr = io.StringIO()
        with contextlib.redirect_stderr(stderr), self.assertRaises(SystemExit):
            check_repo_policy.assert_byte_budget_docs_explain_number_and_string_forms(
                "packages/borsuk/README.md",
                docs_text,
                ["ramBudget", "maxBytes"],
            )
        self.assertIn("raw integer numbers", stderr.getvalue())

    def test_benchmark_docs_gate_rejects_stale_artifact_sentinels(self) -> None:
        docs_text = (
            "| Dataset | Records | Ingest vectors/sec | Compaction vectors/sec |\n"
            "| synthetic-uniform | 10,000 | 999 | 999 |\n"
        )
        lifecycle_csv = (
            "dataset,records,dimensions,segment_max_vectors,ingest_ms,ingest_vectors_per_sec,"
            "compaction_ms,compaction_vectors_per_sec,pre_compaction_segments,"
            "post_compaction_segments,compacted_segments_read,compacted_segments_written,"
            "records_rewritten,routing_page_indexes_read,routing_pages_read,"
            "routing_page_indexes_written,routing_pages_written,graph_payloads_read,"
            "graph_bytes_read,compaction_bytes_read,compaction_bytes_written,"
            "compaction_read_bytes_per_sec,compaction_write_bytes_per_sec\n"
            "synthetic-uniform-n10000,10000,64,256,1000.0,10000.0,2000.0,5000.0,"
            "40,40,40,40,10000,1,1,1,1,0,0,1048576,524288,1,1\n"
        )
        sequential_csv = (
            "dataset,mode,records,dimensions,segment_max_vectors,max_segments,"
            "routing_page_overfetch,max_candidates_per_segment,queries,"
            "tie_aware_recall_at_10,id_recall_at_10,termination_reasons,p50_ms,p95_ms,"
            "avg_bytes_read,avg_graph_bytes_read,avg_routing_page_indexes_read,"
            "avg_routing_pages_read,avg_resident_bytes,avg_segments,avg_records_considered,"
            "avg_records_scored,avg_cache_hits,avg_cache_misses\n"
            "synthetic-uniform-n10000,exact,10000,64,256,8,8,64,100,1.0,1.0,"
            "exact-pruned=100,1.0,2.0,1024,0,1,1,267,1,1,1,0,1\n"
        )
        scale_csv = sequential_csv.replace(
            "dataset,mode,records",
            "family,dataset,mode,records",
        ).replace(
            "synthetic-uniform-n10000,exact,10000",
            "synthetic-uniform,synthetic-uniform-n100000,pq-scan,100000",
        )
        parallel_csv = (
            "dataset,mode,records,dimensions,segment_max_vectors,max_segments,"
            "routing_page_overfetch,max_candidates_per_segment,parallelism,queries,"
            "tie_aware_recall_at_10,id_recall_at_10,termination_reasons,p50_ms,p95_ms,qps,"
            "avg_bytes_read,avg_graph_bytes_read,avg_routing_page_indexes_read,"
            "avg_routing_pages_read,avg_resident_bytes,avg_cache_hits,avg_cache_misses,"
            "rss_before,rss_peak,rss_after,rss_peak_delta\n"
            "synthetic-uniform-n10000,graph,10000,64,256,8,8,64,8,800,1.0,1.0,"
            "max-segments=800,1.0,2.0,300.0,1024,2048,1,1,267,0,1,"
            "1000000,2000000,1000000,1000000\n"
        )
        large_scale_csv = (
            "records,dimensions,segment_max_vectors,max_segments,routing_page_overfetch,"
            "max_candidates_per_segment,pre_segments,post_segments,ingest_ms,"
            "compaction_ms,exact_ms,compaction_bytes_read,compaction_bytes_written,"
            "mode,tie_aware_recall_at_10,id_recall_at_10,termination_reason,query_ms,"
            "segments_searched,bytes_read,graph_bytes_read,routing_page_indexes_read,"
            "routing_pages_read,resident_bytes,records_considered,records_scored,"
            "graph_candidates_added\n"
            "1000000,16,128,512,8,128,7813,7813,142000,93200,6890,"
            "14460000,18880000,pq-scan,1.0,1.0,max-segments,22,512,"
            "14460000,0,1,8,61000,65536,65536,0\n"
            "1000000,16,128,512,8,128,7813,7813,142000,93200,6890,"
            "14460000,18880000,vamana-pq,1.0,1.0,max-segments,40,512,"
            "14460000,4096,1,8,61000,65536,65536,64\n"
            "1000000,16,128,512,8,128,7813,7813,142000,93200,6890,"
            "14460000,18880000,hybrid,1.0,1.0,max-segments,41,512,"
            "14460000,4096,1,8,61000,65536,65536,64\n"
        )

        stderr = io.StringIO()
        with contextlib.redirect_stderr(stderr), self.assertRaises(SystemExit):
            check_repo_policy.assert_benchmark_docs_match_artifacts(
                docs_text,
                lifecycle_csv,
                sequential_csv,
                scale_csv,
                parallel_csv,
                large_scale_csv,
            )
        self.assertIn("docs/benchmarks.md", stderr.getvalue())

    def test_benchmark_docs_gate_rejects_stale_large_scale_artifact_summary(self) -> None:
        docs_text = "The latest million-vector gate used stale numbers.\n"
        lifecycle_csv = (
            "dataset,records,dimensions,segment_max_vectors,ingest_ms,ingest_vectors_per_sec,"
            "compaction_ms,compaction_vectors_per_sec,pre_compaction_segments,"
            "post_compaction_segments,compacted_segments_read,compacted_segments_written,"
            "records_rewritten,routing_page_indexes_read,routing_pages_read,"
            "routing_page_indexes_written,routing_pages_written,graph_payloads_read,"
            "graph_bytes_read,compaction_bytes_read,compaction_bytes_written,"
            "compaction_read_bytes_per_sec,compaction_write_bytes_per_sec\n"
            "synthetic-uniform-n10000,10000,64,256,1000.0,10000.0,2000.0,5000.0,"
            "40,40,40,40,10000,1,1,1,1,0,0,1048576,524288,1,1\n"
        )
        sequential_csv = (
            "dataset,mode,records,dimensions,segment_max_vectors,max_segments,"
            "routing_page_overfetch,max_candidates_per_segment,queries,"
            "tie_aware_recall_at_10,id_recall_at_10,termination_reasons,p50_ms,p95_ms,"
            "avg_bytes_read,avg_graph_bytes_read,avg_routing_page_indexes_read,"
            "avg_routing_pages_read,avg_resident_bytes,avg_segments,avg_records_considered,"
            "avg_records_scored,avg_cache_hits,avg_cache_misses\n"
            "synthetic-uniform-n10000,exact,10000,64,256,8,8,64,100,1.0,1.0,"
            "exact-pruned=100,1.0,2.0,1024,0,1,1,267,1,1,1,0,1\n"
        )
        scale_csv = sequential_csv.replace(
            "dataset,mode,records",
            "family,dataset,mode,records",
        ).replace(
            "synthetic-uniform-n10000,exact,10000",
            "synthetic-uniform,synthetic-uniform-n100000,pq-scan,100000",
        )
        parallel_csv = (
            "dataset,mode,records,dimensions,segment_max_vectors,max_segments,"
            "routing_page_overfetch,max_candidates_per_segment,parallelism,queries,"
            "tie_aware_recall_at_10,id_recall_at_10,termination_reasons,p50_ms,p95_ms,qps,"
            "avg_bytes_read,avg_graph_bytes_read,avg_routing_page_indexes_read,"
            "avg_routing_pages_read,avg_resident_bytes,avg_cache_hits,avg_cache_misses,"
            "rss_before,rss_peak,rss_after,rss_peak_delta\n"
            "synthetic-uniform-n10000,graph,10000,64,256,8,8,64,8,800,1.0,1.0,"
            "max-segments=800,1.0,2.0,300.0,1024,2048,1,1,267,0,1,"
            "1000000,2000000,1000000,1000000\n"
        )
        large_scale_csv = (
            "records,dimensions,segment_max_vectors,max_segments,routing_page_overfetch,"
            "max_candidates_per_segment,pre_segments,post_segments,ingest_ms,"
            "compaction_ms,exact_ms,compaction_bytes_read,compaction_bytes_written,"
            "mode,tie_aware_recall_at_10,id_recall_at_10,termination_reason,query_ms,"
            "segments_searched,bytes_read,graph_bytes_read,routing_page_indexes_read,"
            "routing_pages_read,resident_bytes,records_considered,records_scored,"
            "graph_candidates_added\n"
            "1000000,16,128,512,8,128,7813,7813,142000,93200,6890,"
            "14460000,18880000,pq-scan,1.0,1.0,max-segments,22,512,"
            "14460000,0,1,8,61000,65536,65536,0\n"
            "1000000,16,128,512,8,128,7813,7813,142000,93200,6890,"
            "14460000,18880000,vamana-pq,1.0,1.0,max-segments,40,512,"
            "14460000,4096,1,8,61000,65536,65536,64\n"
            "1000000,16,128,512,8,128,7813,7813,142000,93200,6890,"
            "14460000,18880000,hybrid,1.0,1.0,max-segments,41,512,"
            "14460000,4096,1,8,61000,65536,65536,64\n"
        )

        stderr = io.StringIO()
        with contextlib.redirect_stderr(stderr), self.assertRaises(SystemExit):
            check_repo_policy.assert_benchmark_docs_match_artifacts(
                docs_text,
                lifecycle_csv,
                sequential_csv,
                scale_csv,
                parallel_csv,
                large_scale_csv,
            )
        self.assertIn("million-vector benchmark artifact row", stderr.getvalue())

    def test_github_markdown_gate_rejects_unsupported_math_macros(self) -> None:
        markdown = "```math\n\\operatorname{argmin}_x d(q, x)\n```\n"

        stderr = io.StringIO()
        with contextlib.redirect_stderr(stderr), self.assertRaises(SystemExit):
            check_repo_policy.assert_github_rich_markdown_safe("README.md", markdown)
        self.assertIn("operatorname", stderr.getvalue())

    def test_github_markdown_gate_rejects_reserved_mermaid_graph_node(self) -> None:
        markdown = (
            "```mermaid\n"
            "flowchart TD\n"
            "  route --> graph[\"Parquet graph blocks\"]\n"
            "  graph --> rerank[\"exact rerank\"]\n"
            "```\n"
        )

        stderr = io.StringIO()
        with contextlib.redirect_stderr(stderr), self.assertRaises(SystemExit):
            check_repo_policy.assert_github_rich_markdown_safe("docs/architecture.md", markdown)
        self.assertIn("Mermaid", stderr.getvalue())

    def test_github_markdown_gate_accepts_safe_math_and_mermaid(self) -> None:
        markdown = (
            "```math\n"
            "lb(q, s) = max(0, d(q, c_s) - r_s)\n"
            "```\n"
            "```mermaid\n"
            "flowchart TD\n"
            "  route --> graphBlocks[\"Parquet graph blocks\"]\n"
            "  graphBlocks --> rerank[\"exact rerank\"]\n"
            "```\n"
        )

        check_repo_policy.assert_github_rich_markdown_safe("docs/architecture.md", markdown)


if __name__ == "__main__":
    unittest.main()
