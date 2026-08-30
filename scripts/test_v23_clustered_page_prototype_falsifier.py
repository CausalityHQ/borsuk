from __future__ import annotations

import copy
import struct
import unittest

from scripts import v23_clustered_page_prototype_falsifier as subject
from blake3 import blake3
import numpy


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


if __name__ == "__main__":
    unittest.main()

