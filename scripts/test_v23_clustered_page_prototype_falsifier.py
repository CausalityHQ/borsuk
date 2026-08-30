from __future__ import annotations

import copy
import dataclasses
import hashlib
import json
from pathlib import Path
import struct
import tempfile
import unittest

from scripts import v23_clustered_page_prototype_falsifier as subject
from blake3 import blake3
import numpy
import pyarrow as pa
import pyarrow.parquet as pq


def _page_fixture(
    *,
    primary_vectors: tuple[tuple[float, ...], ...] = ((1.0, 0.0), (0.0, 1.0)),
    replica_vectors: tuple[tuple[float, ...], ...] = ((-1.0, 0.0),),
) -> tuple[subject.PageRef, bytes, numpy.ndarray]:
    dimensions = len(primary_vectors[0])
    vectors = primary_vectors + replica_vectors
    ids = tuple(
        [f"p{index}".encode() for index in range(len(primary_vectors))]
        + [f"r{index}".encode() for index in range(len(replica_vectors))]
    )
    offsets = [0]
    id_bytes = bytearray()
    for record_id in ids:
        id_bytes.extend(record_id)
        offsets.append(len(id_bytes))
    offset_bytes = b"".join(struct.pack("<I", offset) for offset in offsets)
    id_section_bytes = len(offset_bytes) + len(id_bytes)
    codes = numpy.asarray(vectors, dtype="<f2").tobytes()
    generation = bytes.fromhex("42" * 32)
    header = bytearray(96)
    header[:4] = b"BVP2"
    header[4] = 2
    header[5] = 3
    header[6] = 4
    struct.pack_into("<I", header, 8, dimensions)
    struct.pack_into("<I", header, 12, 0)
    struct.pack_into("<I", header, 16, len(primary_vectors))
    struct.pack_into("<I", header, 20, len(replica_vectors))
    struct.pack_into("<I", header, 24, id_section_bytes)
    struct.pack_into("<I", header, 28, len(codes))
    header[32:64] = generation
    struct.pack_into("<H", header, 64, dimensions * 2)
    body = bytes(header) + offset_bytes + bytes(id_bytes) + codes
    checksum = blake3(body).hexdigest()
    reference = subject.PageRef(
        generation_checksum=generation,
        page_ordinal=0,
        metric="cosine",
        dimensions=dimensions,
        family="f16-flat",
        code_width=dimensions * 2,
        path=f"pages/{checksum}",
        checksum=checksum,
        encoded_bytes=len(body),
        primary_rows=len(primary_vectors),
        replicated_rows=len(replica_vectors),
    )
    expected = numpy.asarray(vectors, dtype="<f2").astype(numpy.float32)
    expected /= numpy.linalg.norm(expected.astype(numpy.float64), axis=1)[:, None]
    return reference, body, expected.astype(numpy.float32)


def _with_checksum(reference: subject.PageRef, body: bytes) -> subject.PageRef:
    checksum = blake3(body).hexdigest()
    return subject.PageRef(
        generation_checksum=reference.generation_checksum,
        page_ordinal=reference.page_ordinal,
        metric=reference.metric,
        dimensions=reference.dimensions,
        family=reference.family,
        code_width=reference.code_width,
        path=f"pages/{checksum}",
        checksum=checksum,
        encoded_bytes=len(body),
        primary_rows=reference.primary_rows,
        replicated_rows=reference.replicated_rows,
    )


class PageCodecAndClusteringTests(unittest.TestCase):
    def test_bvp2_f16_flat_decodes_primary_then_replicas(self) -> None:
        reference, body, expected = _page_fixture()

        actual = subject.decode_bvp2_page(reference, body)

        numpy.testing.assert_array_equal(actual, expected)

    def test_bvp2_rejects_envelope_and_header_authority_mutations(self) -> None:
        reference, body, _ = _page_fixture()
        mutations: list[tuple[str, subject.PageRef, bytes]] = []

        wrong_path = copy.copy(reference)
        object.__setattr__(wrong_path, "path", f"other/{reference.checksum}")
        mutations.append(("path", wrong_path, body))
        wrong_length = copy.copy(reference)
        object.__setattr__(wrong_length, "encoded_bytes", len(body) + 1)
        mutations.append(("length", wrong_length, body))
        wrong_checksum = copy.copy(reference)
        object.__setattr__(wrong_checksum, "checksum", "00" * 32)
        object.__setattr__(wrong_checksum, "path", f"pages/{'00' * 32}")
        mutations.append(("checksum", wrong_checksum, body))

        for name, index, value in [
            ("magic", 0, ord("X")),
            ("version", 4, 3),
            ("metric", 5, 2),
            ("family", 6, 3),
            ("reserved", 7, 1),
            ("reserved-tail", 95, 1),
        ]:
            changed = bytearray(body)
            changed[index] = value
            mutations.append((name, _with_checksum(reference, bytes(changed)), bytes(changed)))

        for name, offset, value in [
            ("dimension", 8, reference.dimensions + 1),
            ("ordinal", 12, 1),
            ("primary-count", 16, reference.primary_rows + 1),
            ("replica-count", 20, reference.replicated_rows + 1),
            ("id-section", 24, 0),
            ("code-section", 28, 0),
        ]:
            changed = bytearray(body)
            struct.pack_into("<I", changed, offset, value)
            mutations.append((name, _with_checksum(reference, bytes(changed)), bytes(changed)))

        changed_generation = bytearray(body)
        changed_generation[32] ^= 1
        mutations.append(
            (
                "generation",
                _with_checksum(reference, bytes(changed_generation)),
                bytes(changed_generation),
            )
        )
        changed_width = bytearray(body)
        struct.pack_into("<H", changed_width, 64, reference.code_width + 2)
        mutations.append(
            ("code-width", _with_checksum(reference, bytes(changed_width)), bytes(changed_width))
        )

        for name, mutant_reference, mutant_body in mutations:
            with self.subTest(name=name):
                with self.assertRaises(ValueError):
                    subject.decode_bvp2_page(mutant_reference, mutant_body)

    def test_bvp2_rejects_offsets_ids_codes_and_trailing_bytes(self) -> None:
        reference, body, _ = _page_fixture()
        row_count = reference.primary_rows + reference.replicated_rows
        offset_start = 96
        code_start = 96 + struct.unpack_from("<I", body, 24)[0]
        mutations: list[tuple[str, subject.PageRef, bytes]] = []

        bad_offset = bytearray(body)
        struct.pack_into("<I", bad_offset, offset_start + 4, 0)
        mutations.append(("offset", _with_checksum(reference, bytes(bad_offset)), bytes(bad_offset)))

        id_start = 96 + (row_count + 1) * 4
        bad_primary_order = bytearray(body)
        bad_primary_order[id_start : id_start + 4] = b"p1p0"
        mutations.append(
            (
                "primary-order",
                _with_checksum(reference, bytes(bad_primary_order)),
                bytes(bad_primary_order),
            )
        )

        duplicate_cross_partition = bytearray(body)
        duplicate_cross_partition[id_start + 4 : id_start + 6] = b"p0"
        mutations.append(
            (
                "cross-partition-duplicate",
                _with_checksum(reference, bytes(duplicate_cross_partition)),
                bytes(duplicate_cross_partition),
            )
        )

        non_finite = bytearray(body)
        non_finite[code_start : code_start + 2] = struct.pack("<H", 0x7E00)
        mutations.append(
            ("non-finite", _with_checksum(reference, bytes(non_finite)), bytes(non_finite))
        )

        trailing = body + b"x"
        mutations.append(("trailing", _with_checksum(reference, trailing), trailing))
        short = body[:80]
        short_checksum = blake3(short).hexdigest()
        short_ref = copy.copy(reference)
        object.__setattr__(short_ref, "checksum", short_checksum)
        object.__setattr__(short_ref, "path", f"pages/{short_checksum}")
        object.__setattr__(short_ref, "encoded_bytes", len(short))
        mutations.append(("short", short_ref, short))

        for name, mutant_reference, mutant_body in mutations:
            with self.subTest(name=name):
                with self.assertRaises(ValueError):
                    subject.decode_bvp2_page(mutant_reference, mutant_body)

    def test_splitmix64_matches_registered_stream(self) -> None:
        generator = subject.SplitMix64(0)

        self.assertEqual(generator.next_u64(), 0xE220_A839_7B1D_CDAF)
        self.assertEqual(generator.next_u64(), 0x6E78_9E6A_A1B9_65F4)

    def test_spherical_kmeans_is_repeatable_finite_and_f16_roundtripped(self) -> None:
        vectors = numpy.asarray(
            [
                [1.0, 0.0],
                [0.98, 0.2],
                [0.0, 1.0],
                [-0.2, 0.98],
                [-1.0, 0.0],
                [-0.98, -0.2],
                [0.0, -1.0],
                [0.2, -0.98],
            ],
            dtype=numpy.float32,
        )
        vectors /= numpy.linalg.norm(vectors.astype(numpy.float64), axis=1)[:, None]

        first = subject.spherical_kmeans(vectors, "12" * 32, clusters=4, iterations=8)
        second = subject.spherical_kmeans(vectors, "12" * 32, clusters=4, iterations=8)

        numpy.testing.assert_array_equal(first, second)
        self.assertEqual(first.dtype, numpy.float32)
        self.assertEqual(first.shape, (4, 2))
        self.assertTrue(numpy.isfinite(first).all())
        self.assertTrue((numpy.linalg.norm(first, axis=1) > 0.99).all())
        numpy.testing.assert_array_equal(first, first.astype("<f2").astype("<f4"))

    def test_spherical_kmeans_repairs_empty_clusters_deterministically(self) -> None:
        vectors = numpy.asarray(
            [[1.0, 0.0], [1.0, 0.0], [0.0, 1.0], [0.0, 1.0]],
            dtype=numpy.float32,
        )

        means = subject.spherical_kmeans(vectors, "34" * 32, clusters=4, iterations=8)

        self.assertEqual(means.shape, (4, 2))
        self.assertTrue(numpy.isfinite(means).all())
        self.assertTrue((numpy.linalg.norm(means, axis=1) > 0.99).all())

    def test_page_score_is_minimum_squared_distance_without_vote_weight(self) -> None:
        queries = numpy.asarray([[1.0, 0.0], [0.0, 1.0]], dtype=numpy.float32)
        means = numpy.asarray([[1.0, 0.0], [-1.0, 0.0]], dtype=numpy.float32)

        scores = subject.score_page_means(queries, means)

        numpy.testing.assert_array_equal(
            scores,
            numpy.asarray([0.0, 2.0], dtype=numpy.float32),
        )


def _canonical_payload(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":")).encode() + b"\n"


def _json_page(page_ordinal: int) -> dict[str, object]:
    checksum = f"{page_ordinal + 1:02x}" * 32
    return {
        "generation_checksum": [66] * 32,
        "page_ordinal": page_ordinal,
        "metric": "cosine",
        "dimensions": 2,
        "family": "f16-flat",
        "code_width": 4,
        "path": f"pages/{checksum}",
        "checksum": checksum,
        "encoded_bytes": 200,
        "primary_rows": 2,
        "replicated_rows": 1,
    }


@dataclasses.dataclass
class _AuthorityFixture:
    temporary: tempfile.TemporaryDirectory[str]
    arguments: dict[str, object]
    report: dict[str, object]
    roster: dict[str, object]
    registered: subject.RegisteredAuthority
    shape: subject.ScientificShape

    def rewrite(self) -> None:
        paths = {name: Path(self.arguments[f"{name}_path"]) for name in ("terminal", "result", "report", "roster")}
        documents = {
            "terminal": {"schema": "fixture-terminal-v1"},
            "result": {"schema": "fixture-result-v1"},
            "report": self.report,
            "roster": self.roster,
        }
        digests: dict[str, str] = {}
        for name, document in documents.items():
            payload = _canonical_payload(document)
            paths[name].write_bytes(payload)
            digests[name] = hashlib.sha256(payload).hexdigest()
        query_path = Path(self.arguments["query_path"])
        digests["query"] = hashlib.sha256(query_path.read_bytes()).hexdigest()
        self.registered = dataclasses.replace(
            self.registered,
            terminal_sha256=digests["terminal"],
            result_sha256=digests["result"],
            report_sha256=digests["report"],
            roster_sha256=digests["roster"],
            query_sha256=digests["query"],
        )
        self.arguments["registered"] = self.registered


def _authority_fixture() -> _AuthorityFixture:
    temporary = tempfile.TemporaryDirectory()
    root = Path(temporary.name)
    terminal_path = root / "TERMINAL.json"
    result_path = root / "RESULT.json"
    report_path = root / "report.json"
    roster_path = root / "roster.json"
    query_path = root / "test.parquet"
    pages = [_json_page(index) for index in range(3)]
    assignments = [
        [[0], [2]],
        [[0], [1]],
    ]
    samples = []
    for query_index, query_assignments in enumerate(assignments):
        samples.append(
            {
                "query_index": query_index,
                "page_ordinals": [0, 1],
                "oracle_page_ordinals": [0, 2] if query_index == 0 else [0, 1],
                "ground_truth_page_assignments": query_assignments,
                "encoded_bytes": 400,
                "candidate_rows": 6,
                "selector_candidate_anchors": 3,
                "selector_routed_cells": 1,
                "selector_ranked_anchors": 3,
                "ground_truth_ids": [[query_index, rank] for rank in range(2)],
                "ranked": {"ids": [], "distances": []},
                "gt_page_hits": 1 + query_index,
                "oracle_gt_page_hits": 2,
                "hits": 0,
                "recall_ppm": 0,
                "cpu_ns": 1,
            }
        )
    selector = {
        "generation_checksum": [66] * 32,
        "metric": "cosine",
        "dimensions": 2,
        "coarse_cells": 1,
        "page_count": 3,
        "anchors_per_page": 1,
        "code_width": 4,
        "anchor_count": 3,
        "path": f"selectors/{'55' * 32}",
        "checksum": "55" * 32,
        "encoded_bytes": 512,
    }
    arm = {
        "d1_key": {"family": "f16-flat", "code_width_bytes": 4},
        "selector_key": {"family": "f16-flat", "code_width_bytes": 4},
        "selector": selector,
        "selector_routing_cells": 1,
        "selector_ranked_anchor_cap": 3,
        "primary_target_rows": 2,
        "maximum_assignments_per_row": 2,
        "maximum_query_pages": 2,
        "maximum_record_id_bytes": 8,
        "pages": copy.deepcopy(pages),
        "unique_rows": 4,
        "total_assignments": 9,
        "storage_amplification_ppm": 2_250_000,
        "projected_root_bytes": 1,
        "projected_ram_bytes": 1,
        "projected_build_bytes": 1,
        "query_samples": samples,
        "aggregate_recall_ppm": 750_000,
        "minimum_query_recall_ppm": 500_000,
        "coverage_oracle_recall_ppm": 1_000_000,
        "coverage_oracle_minimum_query_recall_ppm": 1_000_000,
        "selector_regret_ppm": 750_000,
        "cpu_p99_ns": 1,
        "passed": False,
    }
    source_commit = "c59128ee68eb28beaa7f5eef7e0570dc7c787b88"
    source_archive_sha256 = "aa" * 32
    page_uri = "s3://fixture-bucket/fixture-attempt/pages"
    report = {
        "schema": "borsuk-v23-d2-artifact-v1",
        "document_kind": "publication-v3-v23-d2-report",
        "claim_eligible": False,
        "stage": "d2",
        "source_archive_sha256": source_archive_sha256,
        "index_id": "fixture-index",
        "dataset_id": "deep-image-96",
        "d1_report_sha256": "44" * 32,
        "page_uri": page_uri,
        "report": {
            "schema": "borsuk-v23-d2-v8",
            "d1_report_checksum": "33" * 32,
            "query_ordinals": [0, 1],
            "rows": 4,
            "arms": [arm],
        },
    }
    roster = {
        "schema": "borsuk-v23-pages-v1",
        "document_kind": "publication-v3-v23-page-roster",
        "claim_eligible": False,
        "stage": "d2",
        "source_archive_sha256": source_archive_sha256,
        "index_id": "fixture-index",
        "dataset_id": "deep-image-96",
        "d1_report_sha256": "44" * 32,
        "page_uri": page_uri,
        "pages": pages,
    }
    values = pa.array(
        [1.0, 0.0, 0.0, 1.0],
        type=pa.float32(),
    )
    embeddings = pa.FixedSizeListArray.from_arrays(values, 2)
    schema = pa.schema(
        [pa.field("emb", pa.list_(pa.field("item", pa.float32(), nullable=False), 2), nullable=False)]
    )
    pq.write_table(pa.Table.from_arrays([embeddings], schema=schema), query_path)
    shape = subject.ScientificShape(
        page_count=3,
        query_count=2,
        dimensions=2,
        recall_k=2,
        selection_width=2,
    )
    registered = subject.RegisteredAuthority(
        source_commit=source_commit,
        attempt_prefix="s3://fixture-bucket/fixture-attempt/",
        terminal_sha256="00" * 32,
        result_sha256="00" * 32,
        report_sha256="00" * 32,
        roster_sha256="00" * 32,
        query_uri="s3://fixture-bucket/dataset/test.parquet",
        query_sha256="00" * 32,
    )
    fixture = _AuthorityFixture(
        temporary=temporary,
        arguments={
            "terminal_path": terminal_path,
            "result_path": result_path,
            "report_path": report_path,
            "roster_path": roster_path,
            "query_path": query_path,
            "registered": registered,
            "shape": shape,
        },
        report=report,
        roster=roster,
        registered=registered,
        shape=shape,
    )
    fixture.rewrite()
    return fixture


class AuthorityAndQualityTests(unittest.TestCase):
    def test_load_authority_binds_exact_bytes_schema_and_scientific_shape(self) -> None:
        fixture = _authority_fixture()
        self.addCleanup(fixture.temporary.cleanup)

        authority = subject.load_authority(**fixture.arguments)

        self.assertEqual([page.page_ordinal for page in authority.pages], [0, 1, 2])
        self.assertEqual(authority.queries.shape, (2, 2))
        self.assertEqual(authority.query_ordinals, (0, 1))

    def test_authority_rejects_digest_schema_order_shape_and_type_drift(self) -> None:
        mutations = (
            ("report-digest", lambda fixture: fixture.arguments.__setitem__(
                "registered", dataclasses.replace(fixture.registered, report_sha256="00" * 32)
            )),
            ("bool-ordinal", lambda fixture: fixture.roster["pages"][0].__setitem__("page_ordinal", False)),
            ("page-order", lambda fixture: fixture.roster["pages"].reverse()),
            ("roster-drift", lambda fixture: fixture.roster["pages"][0].__setitem__("encoded_bytes", 201)),
            ("query-ordinal", lambda fixture: fixture.report["report"].__setitem__("query_ordinals", [0, 2])),
        )
        for name, mutate in mutations:
            fixture = _authority_fixture()
            self.addCleanup(fixture.temporary.cleanup)
            mutate(fixture)
            if name != "report-digest":
                fixture.rewrite()
            with self.subTest(name=name):
                with self.assertRaises(ValueError):
                    subject.load_authority(**fixture.arguments)

    def test_authority_rejects_noncanonical_json_and_query_hash_drift(self) -> None:
        fixture = _authority_fixture()
        self.addCleanup(fixture.temporary.cleanup)
        report_path = Path(fixture.arguments["report_path"])
        report_path.write_text(json.dumps(fixture.report, indent=2) + "\n")
        registered = dataclasses.replace(
            fixture.registered,
            report_sha256=hashlib.sha256(report_path.read_bytes()).hexdigest(),
        )
        fixture.arguments["registered"] = registered
        with self.assertRaises(ValueError):
            subject.load_authority(**fixture.arguments)

        fixture = _authority_fixture()
        self.addCleanup(fixture.temporary.cleanup)
        fixture.arguments["registered"] = dataclasses.replace(
            fixture.registered,
            query_sha256="00" * 32,
        )
        with self.assertRaises(ValueError):
            subject.load_authority(**fixture.arguments)

    def test_page_selection_uses_distance_then_page_ordinal(self) -> None:
        scores = numpy.asarray([[1.0, 0.5, 0.5, 0.2]], dtype=numpy.float32)

        selected = subject.select_pages(scores, 3)

        numpy.testing.assert_array_equal(selected, [[3, 1, 2]])

    def test_quality_is_recomputed_from_assignments_and_oracle(self) -> None:
        fixture = _authority_fixture()
        self.addCleanup(fixture.temporary.cleanup)
        authority = subject.load_authority(**fixture.arguments)
        selections = numpy.asarray([[0, 1], [0, 1]], dtype=numpy.uint32)

        result = subject.quality_metrics(authority, selections)

        self.assertEqual(result["aggregate_recall_ppm"], 750_000)
        self.assertEqual(result["minimum_query_recall_ppm"], 500_000)
        self.assertEqual(result["oracle_attainment_ppm"], 750_000)
        self.assertEqual(result["query_hits"], [1, 2])

    def test_projection_is_exact_and_within_three_gibibytes(self) -> None:
        self.assertEqual(subject.projected_serving_bytes(), 2_686_433_028)
        self.assertLessEqual(subject.projected_serving_bytes(), 3 * 1024**3)


if __name__ == "__main__":
    unittest.main()
