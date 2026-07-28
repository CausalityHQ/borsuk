#!/usr/bin/env python3
"""Tests for replaying real BORSUK Parquet artifacts across table formats."""

from __future__ import annotations

import importlib.util
import tempfile
import unittest
from pathlib import Path

import benchmark_borsuk_table_formats as benchmark


class BorsukArtifactDiscoveryTest(unittest.TestCase):
    def test_classifies_every_persisted_borsuk_parquet_family(self) -> None:
        cases = {
            "cells/1/42/wal/3/runs/records/a.parquet": "wal",
            "cells/1/42/wal/3/runs/tombstones/a.parquet": "tombstones",
            "segments/L0/ab/seg-a.parquet": "segments",
            "routing/pages/L1/ab/page-a.parquet": "routing",
            "manifests/manifest-7-a.parquet": "manifests",
            "global-pq/descriptors/ab/descriptor-a.parquet": "global-pq-descriptors",
            "quantizer/ab/quant-a.parquet": "quantizer",
            "lexical/terms/ab/terms-a.parquet": "lexical",
            "graphs/L0/ab/graph-a.parquet": "graphs",
        }
        for path, family in cases.items():
            with self.subTest(path=path):
                self.assertEqual(benchmark.classify_object_family(path), family)

        self.assertIsNone(
            benchmark.classify_object_family("cells/1/42/wal/3/runs/id-directory/a.bin")
        )
        self.assertIsNone(benchmark.classify_object_family("segments/L0/a.bin"))

    def test_discovers_only_classified_parquet_objects_below_local_root(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            wanted = root / "segments" / "L0" / "seg-a.parquet"
            wanted.parent.mkdir(parents=True)
            wanted.write_bytes(b"PAR1")
            (root / "segments" / "L0" / "notes.txt").write_text("ignore")
            unknown = root / "scratch" / "unknown.parquet"
            unknown.parent.mkdir()
            unknown.write_bytes(b"PAR1")

            objects = benchmark.discover_objects(str(root))

        self.assertEqual(len(objects), 1)
        self.assertEqual(objects[0].relative_path, "segments/L0/seg-a.parquet")
        self.assertEqual(objects[0].family, "segments")
        self.assertEqual(objects[0].bytes, 4)
        self.assertEqual(objects[0].backend, "local_disk")

    def test_current_cell_wal_record_and_tombstone_objects_are_distinct(self) -> None:
        cases = {
            "cells/9/17/wal/0/runs/records/a.parquet": "wal",
            "cells/9/17/wal/0/runs/tombstones/a.parquet": "tombstones",
            "cells/9/17/wal/0/frontier/a.bin": None,
            "cells/9/17/wal/0/runs/id-directory/a.bin": None,
        }
        for path, expected in cases.items():
            with self.subTest(path=path):
                self.assertEqual(benchmark.classify_object_family(path), expected)

    def test_parses_native_s3_prefix_without_starting_aws_resources(self) -> None:
        source = benchmark.parse_source("s3://bench-bucket/existing/run-1")

        self.assertEqual(source.backend, "s3")
        self.assertEqual(source.bucket, "bench-bucket")
        self.assertEqual(source.prefix, "existing/run-1")
        with self.assertRaisesRegex(ValueError, "prefix"):
            benchmark.parse_source("s3://bench-bucket")


class BorsukTracePlanningTest(unittest.TestCase):
    def test_segment_trace_uses_real_schema_for_family_appropriate_accesses(
        self,
    ) -> None:
        columns = (
            benchmark.ColumnProfile("record_id", "binary", b"id-0"),
            benchmark.ColumnProfile("sequence", "integer", 10),
            benchmark.ColumnProfile("routing_code", "floating", 0.25),
            benchmark.ColumnProfile("vector", "nested", None),
        )

        traces = benchmark.plan_traces("segments", columns, rows=128)

        self.assertEqual(
            [trace.operation for trace in traces],
            ["projection", "point", "range", "filtered_scan", "full_scan"],
        )
        self.assertEqual(traces[0].columns, ("record_id", "routing_code"))
        self.assertEqual(traces[1].predicate.column, "record_id")
        self.assertEqual(traces[2].predicate.column, "sequence")
        self.assertEqual(traces[3].predicate.column, "routing_code")

    def test_trace_planning_never_coerces_an_incompatible_schema(self) -> None:
        columns = (benchmark.ColumnProfile("payload", "nested", None),)

        traces = benchmark.plan_traces("quantizer", columns, rows=1)

        self.assertEqual(
            [trace.operation for trace in traces],
            ["projection", "point", "range", "filtered_scan", "full_scan"],
        )
        blocked = {trace.operation: trace.blocker for trace in traces if trace.blocker}
        self.assertEqual(
            set(blocked),
            {"point", "range", "filtered_scan"},
        )
        self.assertTrue(all("schema" in blocker for blocker in blocked.values()))

    def test_graph_and_tombstone_traces_use_their_real_schema(self) -> None:
        graph_columns = (
            benchmark.ColumnProfile("level", "integer", 0),
            benchmark.ColumnProfile("source_record_index", "integer", 7),
            benchmark.ColumnProfile("neighbor_record_index", "integer", 8),
            benchmark.ColumnProfile("neighbor_distance", "floating", 0.25),
        )
        tombstone_columns = (
            benchmark.ColumnProfile("record_id", "binary", b"id-0"),
            benchmark.ColumnProfile("min_visible_generation", "integer", 3),
        )

        graph_traces = benchmark.plan_traces("graphs", graph_columns, rows=64)
        tombstone_traces = benchmark.plan_traces(
            "tombstones", tombstone_columns, rows=64
        )

        self.assertEqual(
            graph_traces[0].columns,
            ("source_record_index", "neighbor_record_index"),
        )
        self.assertEqual(graph_traces[1].predicate.column, "source_record_index")
        self.assertEqual(
            tombstone_traces[0].columns,
            ("record_id", "min_visible_generation"),
        )
        self.assertEqual(tombstone_traces[1].predicate.column, "record_id")


class BorsukMaterializationAndReplayTest(unittest.TestCase):
    def test_materialization_blocks_only_the_incompatible_layout_without_coercion(
        self,
    ) -> None:
        calls: list[tuple[str, object]] = []

        def writer(layout: str, table: object, path: Path) -> None:
            calls.append((layout, table))
            if layout == "compact":
                raise TypeError("FixedSizeList is unsupported")
            path.write_bytes(layout.encode())

        with tempfile.TemporaryDirectory() as temp:
            source_path = Path(temp) / "segments" / "seg.parquet"
            source_path.parent.mkdir()
            source_path.write_bytes(b"PAR1-source")
            source = benchmark.ObjectRef(
                backend="local_disk",
                storage_path=str(source_path),
                relative_path="segments/seg.parquet",
                family="segments",
                bytes=source_path.stat().st_size,
            )
            table = object()

            variants = benchmark.materialize_variants(
                source,
                table,
                Path(temp) / "materialized",
                vortex_writer=writer,
            )

        self.assertEqual(
            [(item.format, item.layout, item.status) for item in variants],
            [
                ("parquet", "source", "ready"),
                ("vortex", "default", "ready"),
                ("vortex", "compact", "blocked"),
            ],
        )
        self.assertIs(calls[0][1], table)
        self.assertIs(calls[1][1], table)
        self.assertIn("TypeError: FixedSizeList is unsupported", variants[2].blocker)

    def test_replay_opens_metadata_and_warms_before_timed_samples(self) -> None:
        events: list[str] = []
        ticks = iter((10.0, 10.004, 20.0, 20.006))

        def prepare(_variant: benchmark.Variant):
            events.append("open_metadata")

            def execute(trace: benchmark.TraceSpec) -> int:
                events.append(f"execute:{trace.operation}")
                return 3

            return execute

        trace = benchmark.TraceSpec("full_scan", "full_scan", None, None)
        variant = benchmark.Variant(
            format="parquet",
            layout="source",
            path="unused",
            bytes=99,
            status="ready",
            blocker="",
        )

        rows = benchmark.replay_variant(
            object_ref=benchmark.ObjectRef(
                "local_disk", "unused", "segments/a.parquet", "segments", 99
            ),
            variant=variant,
            traces=(trace,),
            repetitions=2,
            warmups=1,
            prepare=prepare,
            execution_mode="materialized_arrow",
            timer=lambda: next(ticks),
        )

        self.assertEqual(events[0], "open_metadata")
        self.assertEqual(events[1], "execute:full_scan")
        self.assertEqual([row["elapsed_ms"] for row in rows], [4.0, 6.0])
        self.assertTrue(all(row["rows"] == 3 for row in rows))
        self.assertTrue(
            all(row["execution_mode"] == "materialized_arrow" for row in rows)
        )

    def test_vortex_materialized_mode_forces_arrow_decode_before_counting(self) -> None:
        events: list[str] = []

        class ArrowTable:
            num_rows = 7

        class VortexResult:
            def __len__(self) -> int:
                events.append("len_vortex")
                return 7

            def to_arrow_table(self) -> ArrowTable:
                events.append("to_arrow_table")
                return ArrowTable()

        rows = benchmark.vortex_result_rows(VortexResult(), "materialized_arrow")

        self.assertEqual(rows, 7)
        self.assertEqual(events, ["to_arrow_table"])

    def test_vortex_compressed_native_contract_rejects_len_only_traces(self) -> None:
        for operation in (
            "projection",
            "point",
            "range",
            "filtered_scan",
            "full_scan",
        ):
            trace = benchmark.TraceSpec(operation, operation, None, None)
            with self.subTest(operation=operation):
                with self.assertRaisesRegex(ValueError, "value-consuming"):
                    benchmark.validate_execution_contract(
                        format_name="vortex",
                        execution_mode="compressed_native",
                        trace=trace,
                    )

    def test_cli_defaults_to_materialized_arrow_only(self) -> None:
        args = benchmark.parse_args(["source", "--output-dir", "results"])

        self.assertEqual(args.execution_modes, "materialized_arrow")

    def test_summary_preserves_blockers_and_reports_required_statistics(self) -> None:
        raw = [
            {
                "object": "segments/a.parquet",
                "family": "segments",
                "format": "parquet",
                "layout": "source",
                "workload": "full_scan",
                "elapsed_ms": 1.0,
                "bytes": 40,
                "rows": 2,
                "status": "complete",
                "blocker": "",
            },
            {
                "object": "segments/a.parquet",
                "family": "segments",
                "format": "parquet",
                "layout": "source",
                "workload": "full_scan",
                "elapsed_ms": 3.0,
                "bytes": 40,
                "rows": 2,
                "status": "complete",
                "blocker": "",
            },
            {
                "object": "segments/a.parquet",
                "family": "segments",
                "format": "vortex",
                "layout": "compact",
                "workload": "all",
                "elapsed_ms": "",
                "bytes": 0,
                "rows": 2,
                "status": "blocked",
                "blocker": "schema-incompatible",
            },
        ]

        summary = benchmark.summarize_rows(raw)

        complete = next(row for row in summary if row["status"] == "complete")
        self.assertEqual(complete["mean_ms"], 2.0)
        self.assertEqual(complete["stddev_ms"], 2**0.5)
        self.assertEqual(complete["p50_ms"], 2.0)
        self.assertAlmostEqual(complete["p95_ms"], 2.9)
        self.assertAlmostEqual(complete["p99_ms"], 2.98)
        blocked = next(row for row in summary if row["status"] == "blocked")
        self.assertEqual(blocked["blocker"], "schema-incompatible")
        self.assertEqual(blocked["samples"], 0)

    def test_logical_validation_rejects_cross_format_mismatch(self) -> None:
        rows = [
            {
                "object": "cells/1/0/wal/0/runs/records/a.parquet",
                "family": "wal",
                "format": "parquet",
                "layout": "source",
                "execution_mode": "materialized_arrow",
                "workload": "projection",
                "repetition": 1,
                "rows": 2,
                "logical_checksum": "parquet-values",
                "materialized": True,
                "status": "complete",
            },
            {
                "object": "cells/1/0/wal/0/runs/records/a.parquet",
                "family": "wal",
                "format": "vortex",
                "layout": "compact",
                "execution_mode": "materialized_arrow",
                "workload": "projection",
                "repetition": 1,
                "rows": 2,
                "logical_checksum": "different-values",
                "materialized": True,
                "status": "complete",
            },
        ]

        with self.assertRaisesRegex(ValueError, "logical result mismatch"):
            benchmark.validate_logical_results(rows)

    def test_logical_validation_requires_materialized_checked_values(self) -> None:
        valid = [
            {
                "object": "manifests/a.parquet",
                "family": "manifests",
                "format": format_name,
                "layout": layout,
                "execution_mode": "materialized_arrow",
                "workload": "full_scan",
                "repetition": 1,
                "rows": 1,
                "logical_checksum": "same-values",
                "materialized": True,
                "status": "complete",
            }
            for format_name, layout in (
                ("parquet", "source"),
                ("vortex", "default"),
            )
        ]
        benchmark.validate_logical_results(valid)

        missing = [dict(valid[0], logical_checksum="")]
        with self.assertRaisesRegex(ValueError, "logical checksum"):
            benchmark.validate_logical_results(missing)
        unmaterialized = [dict(valid[0], materialized=False)]
        with self.assertRaisesRegex(ValueError, "materialized"):
            benchmark.validate_logical_results(unmaterialized)

    @unittest.skipUnless(
        importlib.util.find_spec("pyarrow"),
        "physical-format benchmark dependencies are not installed",
    )
    def test_logical_checksum_ignores_nonlogical_arrow_schema_metadata(self) -> None:
        import pyarrow as pa

        values = pa.array([b"a", b"b"], type=pa.binary())
        plain = pa.Table.from_arrays([values], names=["record_id"])
        annotated = plain.replace_schema_metadata(
            {b"borsuk.wal.vector_element_type": b"float32"}
        )

        self.assertEqual(
            benchmark.materialized_table_result(plain).logical_checksum,
            benchmark.materialized_table_result(annotated).logical_checksum,
        )


if __name__ == "__main__":
    unittest.main()
