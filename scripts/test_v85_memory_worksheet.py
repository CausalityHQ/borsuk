import unittest

from scripts.v85_memory_worksheet import project_v85_resident_memory


class V85MemoryWorksheetTests(unittest.TestCase):
    def test_100m_page_ordered_router_has_complete_sub_3gib_budget(self) -> None:
        # Break caught: promotion counts PQ codes alone while omitting mutation,
        # page-directory, concurrent planner, metadata, or runtime memory.
        result = project_v85_resident_memory()

        self.assertEqual(result["schema"], "borsuk-v85-memory-worksheet-v1")
        self.assertEqual(result["collection_rows"], 100_000_000)
        self.assertEqual(result["maximum_delta_rows"], 8_000_000)
        self.assertEqual(result["router_codes_bytes"], 1_600_000_000)
        self.assertEqual(result["mutation_directory_bytes"], 256_000_000)
        self.assertEqual(result["page_directories_bytes"], 3_125_016)
        self.assertEqual(result["pq16_codebooks_bytes"], 786_432)
        self.assertEqual(result["concurrent_sparse_planner_bytes"], 268_435_456)
        self.assertEqual(result["runtime_reserve_bytes"], 536_870_912)
        self.assertEqual(result["range_response_buffers_bytes"], 268_435_456)
        self.assertEqual(result["decode_scratch_bytes"], 134_217_728)
        self.assertEqual(result["allocator_stack_reserve_bytes"], 134_217_728)
        self.assertEqual(result["projected_resident_bytes"], 2_698_772_248)
        self.assertEqual(result["headroom_bytes"], 522_453_224)
        self.assertTrue(result["feasible"])
        self.assertEqual(
            result["qualification_state"],
            "feasible-only-sparse-planner-not-implemented",
        )
        self.assertEqual(result["resident_exact_delta_bytes"], 0)
        self.assertEqual(result["base_id_to_page_bytes"], 0)

    def test_100m_projection_rejects_dense_traceback_and_resident_exact_delta(
        self,
    ) -> None:
        # Break caught: the worksheet quietly admits the current O(page-count)
        # traceback or full resident SQ8 delta, either of which exceeds 3 GiB.
        result = project_v85_resident_memory()
        exact_delta = project_v85_resident_memory(resident_exact_delta_row_bytes=768)

        self.assertEqual(result["dense_traceback_bytes_per_query"], 2_165_625_000)
        self.assertEqual(result["dense_planner_projected_bytes"], 4_595_961_792)
        self.assertGreater(
            result["dense_planner_projected_bytes"], result["ram_budget_bytes"]
        )
        self.assertFalse(result["dense_planner_feasible"])
        self.assertGreater(
            exact_delta["projected_resident_bytes"], exact_delta["ram_budget_bytes"]
        )
        self.assertFalse(exact_delta["feasible"])

    def test_100m_sparse_residual_quartile_remains_below_3gib(self) -> None:
        result = project_v85_resident_memory(
            sparse_residual_fraction_ppm=250_000
        )

        self.assertEqual(result["sparse_residual_rows"], 25_000_000)
        self.assertEqual(result["sparse_residual_codes_and_norms_bytes"], 300_000_000)
        self.assertEqual(result["sparse_residual_bitmap_bytes"], 12_500_000)
        self.assertEqual(result["sparse_residual_rank_directory_bytes"], 781_256)
        self.assertEqual(result["sparse_residual_codebooks_bytes"], 786_432)
        self.assertEqual(result["projected_resident_bytes"], 3_012_839_936)
        self.assertEqual(result["headroom_bytes"], 208_385_536)
        self.assertTrue(result["feasible"])


if __name__ == "__main__":
    unittest.main()
