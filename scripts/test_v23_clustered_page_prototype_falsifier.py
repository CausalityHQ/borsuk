from __future__ import annotations

import copy
import dataclasses
import hashlib
import json
import struct
import tempfile
import threading
import time
import unittest
from pathlib import Path
from unittest import mock

import numpy
import pyarrow as pa
import pyarrow.parquet as pq
from blake3 import blake3

from scripts import v23_clustered_page_prototype_falsifier as subject


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

    def test_authority_rejects_bool_as_int_in_outer_artifact(self) -> None:
        fixture = _authority_fixture()
        self.addCleanup(fixture.temporary.cleanup)
        fixture.report["claim_eligible"] = 0
        fixture.rewrite()

        with self.assertRaises(ValueError):
            subject.load_authority(**fixture.arguments)

    def test_authority_rejects_nested_key_and_selector_schema_drift(self) -> None:
        mutations = (
            (
                "d1-key-value",
                lambda fixture: fixture.report["report"]["arms"][0]["d1_key"].__setitem__(
                    "code_width_bytes", True
                ),
            ),
            (
                "selector-field",
                lambda fixture: fixture.report["report"]["arms"][0]["selector"].pop(
                    "checksum"
                ),
            ),
            (
                "selector-metric",
                lambda fixture: fixture.report["report"]["arms"][0]["selector"].__setitem__(
                    "metric", "euclidean"
                ),
            ),
        )
        for name, mutate in mutations:
            fixture = _authority_fixture()
            self.addCleanup(fixture.temporary.cleanup)
            mutate(fixture)
            fixture.rewrite()
            with self.subTest(name=name):
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


def _ordinal_page(ordinal: int) -> tuple[subject.PageRef, bytes]:
    reference, body, _ = _page_fixture()
    changed = bytearray(body)
    struct.pack_into("<I", changed, 12, ordinal)
    reference = dataclasses.replace(reference, page_ordinal=ordinal)
    return _with_checksum(reference, bytes(changed)), bytes(changed)


class _StreamingBody:
    def __init__(self, client: "_FakeS3", payload: bytes, delay: float) -> None:
        self._client = client
        self._payload = payload
        self._position = 0
        self._delay = delay
        self._closed = False
        with client.lock:
            client.open_bodies += 1
            client.peak_open_bodies = max(client.peak_open_bodies, client.open_bodies)

    def read(self, size: int = -1) -> bytes:
        if self._delay:
            time.sleep(self._delay)
            self._delay = 0.0
        if size < 0:
            size = len(self._payload) - self._position
        start = self._position
        self._position = min(len(self._payload), start + size)
        return self._payload[start : self._position]

    def close(self) -> None:
        if not self._closed:
            self._closed = True
            with self._client.lock:
                self._client.open_bodies -= 1


class _FakeS3:
    def __init__(self, payloads: dict[str, bytes], delays: dict[str, float] | None = None) -> None:
        self.payloads = payloads
        self.delays = delays or {}
        self.lock = threading.Lock()
        self.open_bodies = 0
        self.peak_open_bodies = 0
        self.requested: list[str] = []

    def get_object(self, *, Bucket: str, Key: str) -> dict[str, object]:
        if Bucket != "fixture-bucket" or Key not in self.payloads:
            raise KeyError((Bucket, Key))
        with self.lock:
            self.requested.append(Key)
        return {"Body": _StreamingBody(self, self.payloads[Key], self.delays.get(Key, 0.0))}


def _stream_fixture(page_count: int = 3) -> tuple[subject.Authority, _FakeS3]:
    pages_and_bodies = tuple(_ordinal_page(ordinal) for ordinal in range(page_count))
    pages = tuple(reference for reference, _ in pages_and_bodies)
    payloads = {f"attempt/{reference.path}": body for reference, body in pages_and_bodies}
    shape = subject.ScientificShape(
        page_count=page_count,
        query_count=2,
        dimensions=2,
        recall_k=2,
        selection_width=min(2, page_count),
    )
    assignments = tuple(
        tuple(((query + neighbor) % page_count,) for neighbor in range(2))
        for query in range(2)
    )
    authority = subject.Authority(
        registered=subject.RegisteredAuthority(
            source_commit="12" * 20,
            attempt_prefix="s3://fixture-bucket/attempt/",
            terminal_sha256="21" * 32,
            result_sha256="22" * 32,
            report_sha256="23" * 32,
            roster_sha256="24" * 32,
            query_uri="s3://fixture-bucket/query.parquet",
            query_sha256="25" * 32,
        ),
        shape=shape,
        pages=pages,
        queries=numpy.asarray(((1.0, 0.0), (0.0, 1.0)), dtype=numpy.float32),
        query_ordinals=(3, 7),
        ground_truth_page_assignments=assignments,
        oracle_hits=(2, 2),
    )
    return authority, _FakeS3(payloads)


class _PressureProbe:
    def __init__(self, samples: list[subject.PressureSample]) -> None:
        self.samples = samples
        self.index = 0

    def __call__(self) -> subject.PressureSample:
        sample = self.samples[min(self.index, len(self.samples) - 1)]
        self.index += 1
        return sample


def _safe_pressure(count: int = 8) -> _PressureProbe:
    return _PressureProbe(
        [
            subject.PressureSample(
                rss_bytes=64 * 1024**2,
                psi_full_avg10_ppm=0,
                swap_bytes=0,
                monotonic_ns=index * 1_000_000,
            )
            for index in range(count)
        ]
    )


class StreamingAndResultTests(unittest.TestCase):
    def test_ordered_fetch_never_retains_more_than_four_bodies(self) -> None:
        authority, client = _stream_fixture(page_count=9)
        client.delays = {
            f"attempt/{page.path}": (9 - page.page_ordinal) * 0.0001
            for page in authority.pages
        }

        observed = list(
            subject.ordered_page_bodies(
                client, "fixture-bucket", "attempt/", authority.pages, 4
            )
        )

        self.assertEqual([page.page_ordinal for page, _ in observed], list(range(9)))
        self.assertLessEqual(client.peak_open_bodies, 4)
        self.assertEqual(client.open_bodies, 0)

    def test_stream_rejects_short_and_overlong_bodies(self) -> None:
        authority, client = _stream_fixture(page_count=1)
        key = f"attempt/{authority.pages[0].path}"
        for name, payload in (("short", client.payloads[key][:-1]), ("long", client.payloads[key] + b"x")):
            mutant = _FakeS3({key: payload})
            with self.subTest(name=name):
                with self.assertRaises(ValueError):
                    list(
                        subject.ordered_page_bodies(
                            mutant, "fixture-bucket", "attempt/", authority.pages, 1
                        )
                    )
                self.assertEqual(mutant.open_bodies, 0)

    def test_full_stream_requires_explicit_execution_flag(self) -> None:
        authority, client = _stream_fixture()
        with self.assertRaises(ValueError):
            subject.run_falsifier(authority, client, _safe_pressure(), False)
        self.assertEqual(client.requested, [])

    def test_pressure_stop_emits_no_partial_quality(self) -> None:
        authority, client = _stream_fixture()
        pressure = _PressureProbe(
            [
                subject.PressureSample(1, 0, 0, 0),
                subject.PressureSample(768 * 1024**2, 0, 0, 1),
            ]
        )

        with self.assertRaises(subject.StreamStopped) as caught:
            subject.run_falsifier(authority, client, pressure, True)

        self.assertEqual(caught.exception.last_authenticated_page, 0)
        self.assertFalse(hasattr(caught.exception, "aggregate_recall_ppm"))

    def test_every_registered_pressure_threshold_stops_at_equality(self) -> None:
        threshold_samples = (
            ("rss-limit", subject.PressureSample(768 * 1024**2, 0, 0, 1)),
            ("psi-limit", subject.PressureSample(1, 500_000, 0, 1)),
            ("swap-growth-limit", subject.PressureSample(1, 0, 128 * 1024**2, 1)),
            ("progress-limit", subject.PressureSample(1, 0, 0, 300 * 1_000_000_000)),
        )
        for reason, threshold in threshold_samples:
            authority, client = _stream_fixture()
            pressure = _PressureProbe(
                [subject.PressureSample(1, 0, 0, 0), threshold]
            )
            with self.subTest(reason=reason):
                with self.assertRaises(subject.StreamStopped) as caught:
                    subject.run_falsifier(authority, client, pressure, True)
                self.assertEqual(caught.exception.reason, reason)
                self.assertEqual(caught.exception.last_authenticated_page, 0)

    def test_stream_propagates_get_failure_and_closes_obtained_bodies(self) -> None:
        authority, client = _stream_fixture(page_count=2)
        del client.payloads[f"attempt/{authority.pages[1].path}"]

        with self.assertRaises(KeyError):
            list(
                subject.ordered_page_bodies(
                    client, "fixture-bucket", "attempt/", authority.pages, 2
                )
            )

        self.assertEqual(client.open_bodies, 0)

    def test_nonfinite_page_scores_fail_before_quality(self) -> None:
        authority, client = _stream_fixture()
        with mock.patch.object(
            subject,
            "score_page_means",
            return_value=numpy.full(authority.shape.query_count, numpy.nan),
        ):
            with self.assertRaises(ValueError):
                subject.run_falsifier(authority, client, _safe_pressure(), True)

    def test_scientific_execution_writes_no_files(self) -> None:
        authority, client = _stream_fixture()
        with (
            mock.patch("builtins.open", side_effect=AssertionError("file write")),
            mock.patch.object(Path, "write_bytes", side_effect=AssertionError("file write")),
            mock.patch("os.open", side_effect=AssertionError("file write")),
            mock.patch("tempfile.TemporaryDirectory", side_effect=AssertionError("scratch")),
        ):
            result = subject.run_falsifier(authority, client, _safe_pressure(), True)
        self.assertEqual(result["authenticated_pages"], authority.shape.page_count)

    def test_complete_result_is_canonical_strict_and_hash_stable(self) -> None:
        authority, client = _stream_fixture()

        result = subject.run_falsifier(authority, client, _safe_pressure(), True)
        validated = subject.validate_result(result)
        payload = subject.canonical_result_bytes(validated)

        expected = json.dumps(validated, sort_keys=True, separators=(",", ":")).encode() + b"\n"
        self.assertEqual(payload, expected)
        digest = hashlib.sha256(payload).hexdigest()
        self.assertRegex(digest, r"\A[0-9a-f]{64}\Z")
        self.assertEqual(
            digest,
            hashlib.sha256(subject.canonical_result_bytes(copy.deepcopy(validated))).hexdigest(),
        )

    def test_result_rejects_concrete_type_gate_and_cardinality_drift(self) -> None:
        authority, client = _stream_fixture()
        result = subject.run_falsifier(authority, client, _safe_pressure(), True)
        mutations = (
            ("extra", lambda value: value.__setitem__("extra", 1)),
            ("bool-count", lambda value: value.__setitem__("page_count", True)),
            ("query-count", lambda value: value.__setitem__("query_count", 3)),
            ("passed", lambda value: value.__setitem__("passed", not value["passed"])),
            (
                "algorithm-bool",
                lambda value: value["algorithm"].__setitem__("max_centers", True),
            ),
        )
        for name, mutate in mutations:
            changed = copy.deepcopy(result)
            mutate(changed)
            with self.subTest(name=name):
                with self.assertRaises(ValueError):
                    subject.validate_result(changed)

    def test_cli_refuses_without_exact_complete_stream_flag(self) -> None:
        bucket, prefix = subject._attempt_location(subject.REGISTERED_AUTHORITY.attempt_prefix)
        arguments = [
            "--terminal",
            "terminal.json",
            "--result",
            "result.json",
            "--report",
            "report.json",
            "--roster",
            "roster.json",
            "--query",
            "query.parquet",
            "--bucket",
            bucket,
            "--prefix",
            prefix,
            "--aws-profile",
            "causality",
            "--region",
            "eu-central-1",
        ]
        with self.assertRaises(SystemExit):
            subject.main(arguments)
        with self.assertRaises(SystemExit):
            subject.main(arguments + ["--output", "forbidden.json"])


if __name__ == "__main__":
    unittest.main()
