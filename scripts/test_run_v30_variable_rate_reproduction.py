import dataclasses
import json
import unittest

from scripts.run_v30_variable_rate_reproduction import (
    ArtifactAuthority,
    V30ArmObservation,
    V30ConstructionInputs,
    build_reproduction_result,
    pq8_replacement_geometry,
    reduce_page_candidates,
    select_high_fidelity,
    simulate_concurrent_get_latency_ns,
    validate_reproduction_authority,
)


class V30VariableRateReproductionTests(unittest.TestCase):
    def artifacts(self) -> tuple[ArtifactAuthority, ...]:
        return tuple(
            ArtifactAuthority(
                role=role,
                uri=f"s3://frozen/{role}",
                sha256=character * 64,
                encoded_bytes=offset + 1,
            )
            for offset, (role, character) in enumerate(
                (
                    ("pages-manifest", "1"),
                    ("leaf-postings", "2"),
                    ("leaf-centroids", "3"),
                    ("query-parquet", "4"),
                )
            )
        )

    def test_v30_reproduction_authority_is_exact_and_construction_has_no_eval_capability(
        self,
    ) -> None:
        # Break caught: reproduction discovers latest objects or leaks queries/truth into
        # hierarchy, codebook, fidelity, or page construction.
        artifacts = self.artifacts()
        validate_reproduction_authority(
            artifacts,
            source_rows=100_000,
            query_count=32,
            truth_memberships=320,
        )
        construction = V30ConstructionInputs(
            pages_manifest=artifacts[0],
            leaf_postings=artifacts[1],
            leaf_centroids=artifacts[2],
            output_uri="s3://frozen/output",
        )
        self.assertEqual(
            {field.name for field in dataclasses.fields(construction)},
            {"pages_manifest", "leaf_postings", "leaf_centroids", "output_uri"},
        )
        with self.assertRaisesRegex(ValueError, "digest"):
            validate_reproduction_authority(
                (dataclasses.replace(artifacts[0], sha256="z" * 64), *artifacts[1:]),
                source_rows=100_000,
                query_count=32,
                truth_memberships=320,
            )
        with self.assertRaisesRegex(ValueError, "roles"):
            validate_reproduction_authority(
                (dataclasses.replace(artifacts[0], role="query-parquet"), *artifacts[1:]),
                source_rows=100_000,
                query_count=32,
                truth_memberships=320,
            )

    def test_v30_reproduction_fixes_pq8_replacement_geometry(self) -> None:
        # Break caught: the historical PQ8 label silently becomes PQ4/additive or changes
        # dimensional partitions after quality is observed.
        geometry = pq8_replacement_geometry()
        self.assertEqual(
            geometry,
            {
                "base_centroids": 256,
                "base_dimensions": 4,
                "base_subquantizers": 24,
                "base_width_bytes": 24,
                "high_centroids": 256,
                "high_dimensions": 2,
                "high_subquantizers": 48,
                "high_width_bytes": 48,
            },
        )

    def test_v30_reproduction_selects_exact_error_tail_without_queries(self) -> None:
        # Break caught: fidelity is chosen from query misses or unstable threshold ties.
        errors = [0.0] * 20
        errors[7] = 9.0
        errors[3] = 9.0
        self.assertEqual(select_high_fidelity(errors, 100_000), (3, 7))
        self.assertEqual(select_high_fidelity(errors, 50_000), (3,))
        with self.assertRaisesRegex(ValueError, "fraction"):
            select_high_fidelity(errors, 50_001)

    def test_v30_reproduction_reducer_is_bounded_and_page_stable(self) -> None:
        # Break caught: routing retains corpus-sized candidates, returns fewer than ten
        # distinct pages, or lets score ties depend on traversal order.
        ranked = [(float(row // 2), row) for row in range(24)]
        row_pages = tuple(row // 2 for row in range(24))
        self.assertEqual(
            reduce_page_candidates(ranked, row_pages, candidate_depth=24, page_count=10),
            tuple(range(10)),
        )
        with self.assertRaisesRegex(ValueError, "candidate depth"):
            reduce_page_candidates(ranked, row_pages, candidate_depth=12_289, page_count=10)
        with self.assertRaisesRegex(ValueError, "page count"):
            reduce_page_candidates(ranked[:18], row_pages, candidate_depth=18, page_count=10)

    def test_v30_reproduction_result_recomputes_the_archived_boundary(self) -> None:
        # Break caught: the gate trusts aggregate labels, averages away the one miss, or
        # promotes burned reproduction evidence to a release claim.
        arms = []
        for fraction in (0, 50_000, 100_000, 200_000):
            hits = [9] * 32
            if fraction == 50_000:
                hits = [10] * 32
                hits[11] = 9
            arms.append(
                V30ArmObservation(
                    fidelity_fraction_ppm=fraction,
                    hits=tuple(hits),
                    selected_page_counts=(10,) * 32,
                    maximum_encoded_bytes=4_000_000,
                    maximum_scanned_codes=100_000,
                )
            )
        payload = build_reproduction_result(tuple(arms))
        value = json.loads(payload)
        self.assertEqual(payload, json.dumps(value, allow_nan=False, separators=(",", ":"), sort_keys=True).encode() + b"\n")
        self.assertFalse(value["claim_eligible"])
        self.assertEqual(value["status"], "reproduced")
        self.assertEqual(value["selected_fraction_ppm"], 50_000)
        selected = next(arm for arm in value["arms"] if arm["fidelity_fraction_ppm"] == 50_000)
        self.assertEqual(selected["aggregate_recall_ppm"], 996_875)
        self.assertEqual(selected["minimum_recall_ppm"], 900_000)
        self.assertEqual(selected["perfect_queries"], 31)

    def test_v30_reproduction_simulates_one_concurrent_s3_wave_without_sleeping(self) -> None:
        # Break caught: the fast gate sums ten GET latencies as if serial, sleeps, or hides
        # a hard-tail request behind an average.
        waves = tuple(tuple(1_000_000 + query * 10_000 + request for request in range(10)) for query in range(32))
        projection = simulate_concurrent_get_latency_ns(waves)
        self.assertEqual(projection["request_count"], 320)
        self.assertEqual(projection["wave_count"], 32)
        self.assertEqual(projection["maximum_ns"], max(max(wave) for wave in waves))
        self.assertLess(projection["p99_ns"], sum(waves[-1]))
        self.assertEqual(projection["model"], "concurrent-max-no-sleep")


if __name__ == "__main__":
    unittest.main()
