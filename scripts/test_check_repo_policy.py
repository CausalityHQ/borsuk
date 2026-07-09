import contextlib
import io
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import check_repo_policy


class BenchmarkArtifactPolicyTests(unittest.TestCase):
    def test_local_benchmark_gate_rejects_weak_id_recall_threshold(self) -> None:
        benchmark_text = (
            "use borsuk::recall_at_k;\n"
            "fn assert_approx_report() {\n"
            '    assert!(recall_at_k(&exact_ids, &approx_ids, 10).expect("recall") >= 0.1);\n'
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

    def test_parallel_graph_pressure_gate_rejects_missing_100k_rows(self) -> None:
        self.assertTrue(
            hasattr(check_repo_policy, "assert_parallel_graph_pressure_artifact"),
            "repo policy should expose a focused parallel graph pressure gate",
        )
        csv_text = (
            "dataset,mode,records,parallelism,routing_page_overfetch,p95_ms,qps,"
            "avg_graph_bytes_read,avg_routing_page_indexes_read,avg_routing_pages_read,"
            "avg_resident_bytes,avg_cache_misses,rss_peak_delta\n"
            "synthetic-uniform-n10000,graph,10000,8,8,10.0,100.0,"
            "4096,1,1,275,1,4096\n"
        )

        stderr = io.StringIO()
        with contextlib.redirect_stderr(stderr), self.assertRaises(SystemExit):
            check_repo_policy.assert_parallel_graph_pressure_artifact(csv_text)
        self.assertIn("synthetic-uniform-n100000", stderr.getvalue())

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

    def test_typescript_docs_gate_rejects_positional_numeric_search_options(
        self,
    ) -> None:
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

    def test_routing_topology_docs_gate_rejects_single_map_only_explanation(
        self,
    ) -> None:
        self.assertTrue(
            hasattr(check_repo_policy, "assert_routing_topology_docs"),
            "repo policy should expose a focused routing topology docs gate",
        )
        docs = {
            "README.md": (
                "## ELI5 Intuition\n"
                "Think of the index as a map plus boxes of vectors.\n"
            ),
            "docs/architecture.md": (
                "The architecture has a routing map and vector boxes.\n"
            ),
            "docs/api.md": ("`routing_page_fanout` is configurable.\n"),
        }

        stderr = io.StringIO()
        with contextlib.redirect_stderr(stderr), self.assertRaises(SystemExit):
            check_repo_policy.assert_routing_topology_docs(docs)
        self.assertIn("computed multi-level routing", stderr.getvalue())

    def test_routing_topology_docs_gate_requires_single_level_degenerate_case(
        self,
    ) -> None:
        docs = {
            "README.md": (
                "## ELI5 Intuition\n"
                'So "map plus boxes" is only the beginner picture. '
                "The production shape is a computed multi-level routing tree. "
                "`routing_page_fanout` and `routing_page_overfetch` tune it.\n"
            ),
            "docs/architecture.md": (
                "The right production model is not one flat map. "
                "At large scale, BORSUK uses a map of maps. "
                "This does not put vectors in higher layers. "
                "The tree is computed during publish and compaction. "
                "`routing_page_fanout` and `routing_page_overfetch` tune it.\n"
            ),
            "docs/api.md": (
                'Do not manually choose "one map" versus "many maps". '
                "Do not model production-scale search as one flat map. "
                "BORSUK computes a computed hierarchy. "
                "It has a root page index, parent routing pages, L0 leaf routing pages. "
                "`routing_max_level = 0`; higher values mean parent layers exist.\n"
            ),
        }

        stderr = io.StringIO()
        with contextlib.redirect_stderr(stderr), self.assertRaises(SystemExit):
            check_repo_policy.assert_routing_topology_docs(docs)
        self.assertIn("degenerate", stderr.getvalue())

    def test_routing_implementation_gate_rejects_missing_deep_search_coverage(
        self,
    ) -> None:
        self.assertTrue(
            hasattr(check_repo_policy, "assert_routing_implementation_tests"),
            "repo policy should expose a focused routing implementation gate",
        )
        local_index_tests = (
            "fn approximate_search_drills_through_deep_paged_routing_tree() {}\n"
            "fn approximate_search_with_inner_product_ranks_segments_by_metric_distance() {}\n"
            "fn compact_reuses_unaffected_routing_layer_page_objects() {}\n"
        )
        index_unit_tests = "fn parent_page_routing_overfetch_reads_sibling_branches_when_first_branch_is_dense() {}\n"

        stderr = io.StringIO()
        with contextlib.redirect_stderr(stderr), self.assertRaises(SystemExit):
            check_repo_policy.assert_routing_implementation_tests(
                local_index_tests,
                index_unit_tests,
            )
        self.assertIn(
            "approximate_search_walks_parent_routing_pages_without_l0_index",
            stderr.getvalue(),
        )

    def test_storage_format_versioning_policy_gate_rejects_missing_unknown_column_rule(
        self,
    ) -> None:
        self.assertTrue(
            hasattr(check_repo_policy, "assert_storage_format_versioning_policy"),
            "repo policy should expose a focused storage-format versioning gate",
        )
        docs_text = (
            "## Versioning Policy\n"
            "Pointer-format version changes when CURRENT changes.\n"
            "Table-format version changes when metadata tables change.\n"
        )

        stderr = io.StringIO()
        with contextlib.redirect_stderr(stderr), self.assertRaises(SystemExit):
            check_repo_policy.assert_storage_format_versioning_policy(docs_text)
        self.assertIn("additive columns", stderr.getvalue())

    def test_storage_format_versioning_policy_gate_accepts_required_text(self) -> None:
        self.assertTrue(
            hasattr(check_repo_policy, "assert_storage_format_versioning_policy"),
            "repo policy should expose a focused storage-format versioning gate",
        )
        docs_text = (
            "## Versioning Policy\n"
            "Pointer-format version changes when the fixed binary CURRENT layout changes.\n"
            "Table-format version changes when an incompatible table schema change is made.\n"
            "Same-major readers must ignore unknown columns.\n"
            "Additive columns must be written so older same-major readers can ignore them.\n"
        )

        check_repo_policy.assert_storage_format_versioning_policy(docs_text)

    def test_updates_and_deletes_docs_gate_rejects_missing_mutation_contract(
        self,
    ) -> None:
        self.assertTrue(
            hasattr(check_repo_policy, "assert_updates_and_deletes_docs"),
            "repo policy should expose a focused updates/deletes docs gate",
        )
        docs = {
            "README.md": "## Updates and deletes\nUse rebuild when records change.\n",
            "docs/api.md": "## Updates and deletes\nUse garbage collection later.\n",
        }

        stderr = io.StringIO()
        with contextlib.redirect_stderr(stderr), self.assertRaises(SystemExit):
            check_repo_policy.assert_updates_and_deletes_docs(docs)
        self.assertIn("cumulative tombstone", stderr.getvalue())

    def test_updates_and_deletes_docs_gate_accepts_required_contract(self) -> None:
        self.assertTrue(
            hasattr(check_repo_policy, "assert_updates_and_deletes_docs"),
            "repo policy should expose a focused updates/deletes docs gate",
        )
        contract = (
            "## Updates and deletes\n"
            "Delete records with a cumulative soft tombstone; reclaim them lazily "
            "with compaction or immediately with `purge`.\n"
            "For a wholesale replacement, rebuild into a fresh index and run "
            "garbage collection with `borsuk gc --delete`.\n"
            "```bash\nborsuk rebuild --uri file:///tmp/new-index\n"
            "borsuk gc --uri file:///tmp/new-index --delete\n```\n"
        )
        docs = {"README.md": contract, "docs/api.md": contract}

        check_repo_policy.assert_updates_and_deletes_docs(docs)

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
            "routing_pages_read,resident_bytes,rss_before,rss_peak,rss_after,"
            "rss_peak_delta,records_considered,records_scored,graph_candidates_added\n"
            "1000000,16,128,512,8,128,7813,7813,142000,93200,6890,"
            "14460000,18880000,pq-scan,1.0,1.0,max-segments,22,512,"
            "14460000,0,1,8,61000,1000000,1250000,1100000,250000,65536,65536,0\n"
            "1000000,16,128,512,8,128,7813,7813,142000,93200,6890,"
            "14460000,18880000,vamana-pq,1.0,1.0,max-segments,40,512,"
            "14460000,4096,1,8,61000,1000000,1300000,1100000,300000,65536,65536,64\n"
            "1000000,16,128,512,8,128,7813,7813,142000,93200,6890,"
            "14460000,18880000,hybrid,1.0,1.0,max-segments,41,512,"
            "14460000,4096,1,8,61000,1000000,1300000,1100000,300000,65536,65536,64\n"
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

    def test_benchmark_docs_gate_rejects_stale_large_scale_artifact_summary(
        self,
    ) -> None:
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
            "routing_pages_read,resident_bytes,rss_before,rss_peak,rss_after,"
            "rss_peak_delta,records_considered,records_scored,graph_candidates_added\n"
            "1000000,16,128,512,8,128,7813,7813,142000,93200,6890,"
            "14460000,18880000,pq-scan,1.0,1.0,max-segments,22,512,"
            "14460000,0,1,8,61000,1000000,1250000,1100000,250000,65536,65536,0\n"
            "1000000,16,128,512,8,128,7813,7813,142000,93200,6890,"
            "14460000,18880000,vamana-pq,1.0,1.0,max-segments,40,512,"
            "14460000,4096,1,8,61000,1000000,1300000,1100000,300000,65536,65536,64\n"
            "1000000,16,128,512,8,128,7813,7813,142000,93200,6890,"
            "14460000,18880000,hybrid,1.0,1.0,max-segments,41,512,"
            "14460000,4096,1,8,61000,1000000,1300000,1100000,300000,65536,65536,64\n"
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

    def test_large_scale_markdown_line_includes_rss_peak_delta(self) -> None:
        line = check_repo_policy.benchmark_large_scale_markdown_line(
            {
                "records": "1000000",
                "mode": "pq-scan",
                "tie_aware_recall_at_10": "1.0",
                "id_recall_at_10": "1.0",
                "query_ms": "22",
                "segments_searched": "512",
                "bytes_read": "14460000",
                "graph_bytes_read": "0",
                "routing_pages_read": "8",
                "resident_bytes": "61000",
                "rss_peak_delta": "250000",
            }
        )

        self.assertIn("244.1 KB", line)

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
            '  route --> graph["Parquet graph blocks"]\n'
            '  graph --> rerank["exact rerank"]\n'
            "```\n"
        )

        stderr = io.StringIO()
        with contextlib.redirect_stderr(stderr), self.assertRaises(SystemExit):
            check_repo_policy.assert_github_rich_markdown_safe(
                "docs/architecture.md", markdown
            )
        self.assertIn("Mermaid", stderr.getvalue())

    def test_github_markdown_gate_accepts_safe_math_and_mermaid(self) -> None:
        markdown = (
            "```math\n"
            "lb(q, s) = max(0, d(q, c_s) - r_s)\n"
            "```\n"
            "```mermaid\n"
            "flowchart TD\n"
            '  route --> graphBlocks["Parquet graph blocks"]\n'
            '  graphBlocks --> rerank["exact rerank"]\n'
            "```\n"
        )

        check_repo_policy.assert_github_rich_markdown_safe(
            "docs/architecture.md", markdown
        )

    def test_package_platform_gate_rejects_missing_node_26_ci_matrix(self) -> None:
        self.assertTrue(
            hasattr(check_repo_policy, "assert_package_platform_coverage"),
            "repo policy should expose a focused package platform coverage gate",
        )
        ci_text = (
            "python-package:\n"
            "  name: Python package (${{ matrix.os }}, py${{ matrix.python-version }})\n"
            "  os: [ubuntu-latest, ubuntu-24.04-arm, macos-26, macos-15-intel, windows-latest]\n"
            '  python-version: ["3.12", "3.13", "3.14"]\n'
            "node-package:\n"
            "  name: TypeScript package (${{ matrix.os }}, node${{ matrix.node-version }})\n"
            "  os: [ubuntu-latest, ubuntu-24.04-arm, macos-26, macos-15-intel, windows-latest]\n"
            '  node-version: ["22", "24"]\n'
        )
        publish_text = (
            "os: [ubuntu-latest, ubuntu-24.04-arm, macos-26, macos-15-intel, windows-latest]\n"
            'python-version: ["3.12", "3.13", "3.14"]\n'
            'node-version: "24"\n'
            "borsuk-*cp312-*.whl\n"
            "borsuk-*cp313-*.whl\n"
            "borsuk-*cp314-*.whl\n"
            "borsuk-*manylinux*x86_64.whl\n"
            "borsuk-*manylinux*aarch64.whl\n"
            "borsuk-*macosx*x86_64.whl\n"
            "borsuk-*macosx*arm64.whl\n"
            "borsuk-*win_amd64.whl\n"
            "index.linux-x64-gnu.node\n"
            "index.linux-arm64-gnu.node\n"
            "index.darwin-arm64.node\n"
            "index.darwin-x64.node\n"
            "index.win32-x64-msvc.node\n"
        )
        package_text = '"engines": {\n  "node": ">=22 <27"\n}\n'
        pyproject_text = (
            'requires-python = ">=3.12"\n'
            '"Programming Language :: Python :: 3.12"\n'
            '"Programming Language :: Python :: 3.13"\n'
            '"Programming Language :: Python :: 3.14"\n'
        )

        stderr = io.StringIO()
        with contextlib.redirect_stderr(stderr), self.assertRaises(SystemExit):
            check_repo_policy.assert_package_platform_coverage(
                ci_text,
                publish_text,
                package_text,
                pyproject_text,
            )
        self.assertIn("Node", stderr.getvalue())

    def test_package_platform_gate_accepts_current_supported_matrix(self) -> None:
        self.assertTrue(
            hasattr(check_repo_policy, "assert_package_platform_coverage"),
            "repo policy should expose a focused package platform coverage gate",
        )
        check_repo_policy.assert_package_platform_coverage(
            Path(".github/workflows/ci.yml").read_text(),
            Path(".github/workflows/publish.yml").read_text(),
            Path("packages/borsuk/package.json").read_text(),
            Path("python/pyproject.toml").read_text(),
        )


if __name__ == "__main__":
    unittest.main()
