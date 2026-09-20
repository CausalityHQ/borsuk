import hashlib
import json
import pathlib
import subprocess
import sys
import tempfile
import unittest

import numpy as np
import pyarrow as pa
import pyarrow.ipc as ipc
import pyarrow.parquet as pq

from scripts.v85_pq16_page_nomination import (
    PageEntry,
    evaluate_page_nominations,
    plan_rank_weighted_ranges,
)
from scripts.v85_pq16_rescore_summary import summarize_paired_rescore


class V85Pq16PageNominationTests(unittest.TestCase):
    def test_paired_summary_uses_pq_truth_when_centroid_result_omits_it(self) -> None:
        # Break caught: the reducer requires duplicated truth IDs from the
        # centroid result even though PQ evidence is the authenticated authority.
        truth = list(range(100))
        centroid = {
            "samples": [
                {
                    "query": 0,
                    "result_ids": truth,
                    "hits": 100,
                    "bytes": 12,
                    "requests": 2,
                    "latency_ns": 30,
                }
            ]
        }
        pq16 = {
            "average_recall10_ppm": 1_000_000,
            "average_recall100_ppm": 1_000_000,
            "gate_passed": True,
            "p05_recall100_ppm": 1_000_000,
            "samples": [
                {
                    "query": 0,
                    "truth_ids": truth,
                    "hit10_ids": truth[:10],
                    "hit_ids": truth,
                    "hits10": 10,
                    "hits": 100,
                }
            ],
        }

        summary = summarize_paired_rescore(
            centroid,
            pq16,
            centroid_sha256="1" * 64,
            pq16_sha256="2" * 64,
        )

        self.assertEqual(summary["centroid_average_recall10_ppm"], 1_000_000)
        self.assertEqual(summary["centroid_average_recall100_ppm"], 1_000_000)
        self.assertEqual(summary["paired_recall10_delta_ci95_ppm"], [0, 0])
        self.assertEqual(summary["paired_recall100_delta_ci95_ppm"], [0, 0])

    def test_100k_rescore_runner_preregisters_full_development_evidence(self) -> None:
        # Break caught: the paid rescore silently uses 32 queries, retunes the
        # arm, or enables nested query-level work stealing.
        root = pathlib.Path(__file__).resolve().parents[1]
        completed = subprocess.run(
            [
                "bash",
                str(root / "scripts/v85_pq16_100k_rescore_run_remote.sh"),
                "--describe",
            ],
            cwd=root,
            check=True,
            capture_output=True,
            text=True,
        )

        self.assertEqual(
            json.loads(completed.stdout),
            {
                "blas_threads": 16,
                "instance_type": "c7i.8xlarge",
                "max_wall_seconds": 1800,
                "page_budget": 32,
                "queries": 1000,
                "query_parallelism": 1,
                "shortlist_rows": 2048,
                "spot_only": True,
            },
        )

    def test_rank_weighted_planner_uses_exact_dense_range_budget(self) -> None:
        # Break caught: the screen reads every page touched by the shortlist
        # instead of selecting the maximum reciprocal-rank evidence under the
        # registered physical span and range budgets.
        ranges = plan_rank_weighted_ranges(
            ranked_base_ids=np.asarray([10, 30, 31, 40], dtype=np.int64),
            base_page_by_id={10: 0, 30: 3, 31: 3, 40: 4},
            page_count=6,
            max_span_pages=2,
            max_ranges=1,
        )

        self.assertEqual(ranges, [(3, 4)])

    @staticmethod
    def _write_page(path: pathlib.Path, row_ids: list[int], dimensions: int) -> int:
        schema = pa.schema(
            [
                pa.field("id", pa.int64(), nullable=False),
                pa.field("sequence", pa.uint64(), nullable=False),
                pa.field("state", pa.uint8(), nullable=False),
                pa.field(
                    "code",
                    pa.list_(
                        pa.field("element", pa.uint8(), nullable=False), dimensions
                    ),
                    nullable=False,
                ),
            ]
        )
        table = pa.Table.from_arrays(
            [
                pa.array(row_ids, type=pa.int64()),
                pa.array([1] * len(row_ids), type=pa.uint64()),
                pa.array([0] * len(row_ids), type=pa.uint8()),
                pa.FixedSizeListArray.from_arrays(
                    pa.array([0] * (len(row_ids) * dimensions), type=pa.uint8()),
                    dimensions,
                ),
            ],
            schema=schema,
        )
        sink = pa.BufferOutputStream()
        with ipc.new_stream(sink, schema) as writer:
            writer.write_table(table)
        body = sink.getvalue().to_pybytes()
        path.write_bytes(body)
        return len(body)

    def test_cli_authenticates_real_artifacts_and_emits_canonical_result(self) -> None:
        # Break caught: the paid screen reaches science with a mismatched Arrow
        # page schema, query/truth Parquet shape, digest, or CLI binding.
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            dimensions = 16
            base_ids = list(range(256))
            source_ids = base_ids + [900]
            vectors = np.arange(len(source_ids) * dimensions, dtype=np.float32).reshape(
                -1, dimensions
            )
            source = root / "source.parquet"
            queries = root / "queries.parquet"
            truth = root / "truth.parquet"
            generation = root / "generation.json"
            base = root / "base.arrow"
            delta = root / "delta.arrow"
            output = root / "result.json"
            pq.write_table(
                pa.Table.from_arrays(
                    [
                        pa.array(source_ids, type=pa.uint64()),
                        pa.FixedSizeListArray.from_arrays(
                            pa.array(vectors.reshape(-1), type=pa.float32()), dimensions
                        ),
                    ],
                    schema=pa.schema(
                        [
                            pa.field("feature_row_id", pa.uint64(), nullable=False),
                            pa.field(
                                "embedding",
                                pa.list_(
                                    pa.field("item", pa.float32(), nullable=False),
                                    dimensions,
                                ),
                                nullable=False,
                            ),
                        ]
                    ),
                ),
                source,
            )
            pq.write_table(
                pa.Table.from_arrays(
                    [
                        pa.array([0], type=pa.uint32()),
                        pa.FixedSizeListArray.from_arrays(
                            pa.array(vectors[0], type=pa.float32()), dimensions
                        ),
                    ],
                    schema=pa.schema(
                        [
                            pa.field("query", pa.uint32(), nullable=False),
                            pa.field(
                                "vector",
                                pa.list_(
                                    pa.field("element", pa.float32(), nullable=False),
                                    dimensions,
                                ),
                                nullable=False,
                            ),
                        ]
                    ),
                ),
                queries,
            )
            pq.write_table(
                pa.Table.from_arrays(
                    [
                        pa.array([0], type=pa.uint32()),
                        pa.FixedSizeListArray.from_arrays(
                            pa.array([0, 900], type=pa.int64()), 2
                        ),
                    ],
                    schema=pa.schema(
                        [
                            pa.field("query", pa.uint32(), nullable=False),
                            pa.field(
                                "neighbors",
                                pa.list_(
                                    pa.field("element", pa.int64(), nullable=False), 2
                                ),
                                nullable=False,
                            ),
                        ]
                    ),
                ),
                truth,
            )
            base_bytes = self._write_page(base, base_ids, dimensions)
            delta_bytes = self._write_page(delta, [900], dimensions)

            def identity(path: pathlib.Path) -> dict[str, object]:
                body = path.read_bytes()
                return {
                    "bytes": len(body),
                    "sha256": hashlib.sha256(body).hexdigest(),
                    "uri": f"s3://fixture/{path.name}",
                }

            manifest = {
                "dimensions": dimensions,
                "runs": [
                    {
                        "kind": "base",
                        "object": identity(base),
                        "pages": [
                            {"bytes": base_bytes, "offset": 0, "page": 0, "rows": 256}
                        ],
                    },
                    {
                        "kind": "delta",
                        "object": identity(delta),
                        "pages": [
                            {"bytes": delta_bytes, "offset": 0, "page": 1, "rows": 1}
                        ],
                    },
                ],
            }
            generation.write_bytes(
                json.dumps(manifest, separators=(",", ":"), sort_keys=True).encode()
                + b"\n"
            )
            command = [
                sys.executable,
                str(pathlib.Path(__file__).with_name("v85_pq16_page_nomination.py")),
            ]
            for role, path in (
                ("source", source),
                ("queries", queries),
                ("truth", truth),
                ("generation", generation),
            ):
                command.extend(
                    [
                        f"--{role}",
                        str(path),
                        f"--{role}-uri",
                        f"s3://fixture/{path.name}",
                        f"--{role}-sha256",
                        hashlib.sha256(path.read_bytes()).hexdigest(),
                    ]
                )
            command.extend(
                [
                    "--base",
                    str(base),
                    "--delta",
                    str(delta),
                    "--output",
                    str(output),
                    "--dimensions",
                    str(dimensions),
                    "--neighbors",
                    "2",
                    "--queries-count",
                    "1",
                    "--shortlist-rows",
                    "256",
                ]
            )
            completed = subprocess.run(
                command, check=True, capture_output=True, text=True
            )
            result_body = output.read_bytes()
            result = json.loads(result_body)
            self.assertEqual(
                result["schema"], "borsuk-v85-pq16-page-nomination-result-v2"
            )
            self.assertEqual(result["average_recall10_ppm"], 1_000_000)
            self.assertEqual(result["average_recall100_ppm"], 1_000_000)
            self.assertEqual(result["p05_recall100_ppm"], 1_000_000)
            self.assertEqual(result["rows"], 257)
            self.assertTrue(result["gate_passed"])
            self.assertEqual(result_body[-1:], b"\n")
            self.assertEqual(
                json.loads(completed.stdout)["result_sha256"],
                hashlib.sha256(result_body).hexdigest(),
            )

    def test_resident_delta_and_coalesced_base_ranges_are_counted_exactly(self) -> None:
        # Break caught: delta hits consume S3 work, or a coalesced request counts
        # only selected page payloads rather than the full physical byte span.
        pages = {
            0: PageEntry(offset=0, encoded_bytes=100),
            1: PageEntry(offset=100, encoded_bytes=100),
            3: PageEntry(offset=200, encoded_bytes=100),
            5: PageEntry(offset=300, encoded_bytes=100),
        }
        result = evaluate_page_nominations(
            ranked_base_ids=np.asarray([[10, 30], [10, 30]], dtype=np.int64),
            truth_ids=np.asarray([[10, 50, 900], [50, 900, 901]], dtype=np.int64),
            base_page_by_id={10: 0, 30: 3, 50: 5},
            resident_delta_ids={900, 901},
            page_entries=pages,
            neighbors=3,
            gap_pages=2,
            max_gets=1,
            max_bytes=300,
            min_average_recall10_ppm=600_000,
            min_average_recall100_ppm=600_000,
            min_p05_recall100_ppm=600_000,
        )

        self.assertEqual(result["average_recall10_ppm"], 666_666)
        self.assertEqual(result["average_recall100_ppm"], 666_666)
        self.assertEqual(result["p05_recall100_ppm"], 666_666)
        self.assertEqual(result["worst_recall_ppm"], 666_666)
        self.assertEqual(result["max_gets_per_query"], 1)
        self.assertEqual(result["max_bytes_per_query"], 300)
        self.assertTrue(result["gate_passed"])
        self.assertEqual(
            result["samples"],
            [
                {
                    "bytes": 300,
                    "gets": 1,
                    "hit10_ids": [10, 900],
                    "hit_ids": [10, 900],
                    "hits10": 2,
                    "hits": 2,
                    "query": 0,
                    "selected_pages": [0, 3],
                    "truth_ids": [10, 50, 900],
                },
                {
                    "bytes": 300,
                    "gets": 1,
                    "hit10_ids": [900, 901],
                    "hit_ids": [900, 901],
                    "hits10": 2,
                    "hits": 2,
                    "query": 1,
                    "selected_pages": [0, 3],
                    "truth_ids": [50, 900, 901],
                },
            ],
        )

    def test_gate_rejects_quality_requests_and_bytes_independently(self) -> None:
        # Break caught: a containment result promotes after any frozen serving
        # boundary is exceeded.
        pages = {
            0: PageEntry(offset=0, encoded_bytes=10),
            2: PageEntry(offset=10, encoded_bytes=10),
        }
        common = dict(
            ranked_base_ids=np.asarray([[10, 20]], dtype=np.int64),
            truth_ids=np.asarray([[10, 20]], dtype=np.int64),
            base_page_by_id={10: 0, 20: 2},
            resident_delta_ids=set(),
            page_entries=pages,
            neighbors=2,
            gap_pages=0,
            min_average_recall10_ppm=1_000_000,
            min_average_recall100_ppm=1_000_000,
            min_p05_recall100_ppm=1_000_000,
        )

        self.assertTrue(
            evaluate_page_nominations(**common, max_gets=2, max_bytes=20)["gate_passed"]
        )
        self.assertFalse(
            evaluate_page_nominations(**common, max_gets=1, max_bytes=20)["gate_passed"]
        )
        self.assertFalse(
            evaluate_page_nominations(**common, max_gets=2, max_bytes=19)["gate_passed"]
        )
        low_quality = dict(common)
        low_quality["truth_ids"] = np.asarray([[10, 99]], dtype=np.int64)
        self.assertFalse(
            evaluate_page_nominations(**low_quality, max_gets=2, max_bytes=20)[
                "gate_passed"
            ]
        )

    def test_gate_uses_p05_distribution_and_reports_absolute_worst(self) -> None:
        # Break caught: one outlier still vetoes promotion, or the p05 gate is
        # approximated from an aggregate rather than per-query containment.
        truth = np.asarray(
            [list(range(query * 100, query * 100 + 100)) for query in range(21)],
            dtype=np.int64,
        )
        ranked = truth[:, :1].copy()
        pages = {
            page: PageEntry(offset=page * 10, encoded_bytes=10) for page in range(21)
        }
        base_page_by_id = {
            int(row_id): query for query, row_id in enumerate(truth[:, 0])
        }
        resident = set(int(row_id) for row_id in truth[:, 1:].reshape(-1))
        for row_id in truth[0, 80:]:
            resident.remove(int(row_id))

        result = evaluate_page_nominations(
            ranked_base_ids=ranked,
            truth_ids=truth,
            base_page_by_id=base_page_by_id,
            resident_delta_ids=resident,
            page_entries=pages,
            neighbors=100,
            gap_pages=0,
            max_gets=32,
            max_bytes=16 * 1024 * 1024,
            min_average_recall10_ppm=960_000,
            min_average_recall100_ppm=975_000,
            min_p05_recall100_ppm=900_000,
        )

        self.assertEqual(result["average_recall10_ppm"], 1_000_000)
        self.assertEqual(result["average_recall100_ppm"], 990_476)
        self.assertEqual(result["p05_recall100_ppm"], 1_000_000)
        self.assertEqual(result["worst_recall_ppm"], 800_000)
        self.assertTrue(result["gate_passed"])


if __name__ == "__main__":
    unittest.main()
