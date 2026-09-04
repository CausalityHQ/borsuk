import dataclasses
import hashlib
import json
import unittest

import numpy as np
import pyarrow as pa
import pyarrow.ipc as ipc
import pyarrow.parquet as pq

from scripts.run_v30_variable_rate_reproduction import (
    ArtifactAuthority,
    V30ArmObservation,
    V30ConstructionInputs,
    build_base_page_layout,
    build_reproduction_result,
    encode_pq8,
    encode_reproduction_evidence,
    evaluate_pq8_replacement_arms,
    exact_truth,
    finalize_reproduction_result,
    fit_pq8,
    load_frozen_reproduction,
    parse_args,
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

    def frozen_fixture(self) -> tuple[tuple[ArtifactAuthority, ...], dict[str, bytes]]:
        objects: dict[str, bytes] = {}
        pages = []
        postings = []
        for page in range(4):
            ordinals = np.arange(page * 16, (page + 1) * 16, dtype=np.uint64)
            vectors = np.zeros((16, 96), dtype=np.float32)
            vectors[np.arange(16), ordinals % 96] = 1.0
            ids = pa.array([int(value).to_bytes(8, "little") for value in ordinals], type=pa.binary(8))
            child = pa.field("element", pa.float32(), nullable=False)
            vector_array = pa.FixedSizeListArray.from_arrays(
                pa.array(vectors.ravel(), type=pa.float32()), 96
            )
            table = pa.Table.from_arrays(
                [ids, vector_array],
                schema=pa.schema(
                    [
                        pa.field("id", pa.binary(8), nullable=False),
                        pa.field("vector", pa.list_(child, 96), nullable=False),
                    ]
                ),
            )
            sink = pa.BufferOutputStream()
            with ipc.new_file(sink, table.schema) as writer:
                writer.write_table(table)
            body = sink.getvalue().to_pybytes()
            digest = hashlib.sha256(body).hexdigest()
            objects[f"s3://frozen/pages/{digest}.arrow"] = body
            pages.append(
                {
                    "encoded_bytes": len(body),
                    "ordinal": page,
                    "primary_rows": 16,
                    "replica_rows": 0,
                    "sha256": digest,
                }
            )
            postings.append((page // 2, page, digest, len(body), 16, 0))
        manifest = {
            "pages": pages,
            "primary_rows": 64,
            "replica_rows": 0,
            "schema_version": 1,
            "source_rows": 64,
            "stored_rows": 64,
        }
        manifest_bytes = json.dumps(manifest, separators=(",", ":"), sort_keys=True).encode() + b"\n"
        posting_schema = pa.schema(
            [
                pa.field("leaf_ordinal", pa.uint32(), nullable=False),
                pa.field("page_ordinal", pa.uint32(), nullable=False),
                pa.field("page_sha256", pa.string(), nullable=False),
                pa.field("encoded_bytes", pa.uint64(), nullable=False),
                pa.field("primary_rows", pa.uint16(), nullable=False),
                pa.field("replica_rows", pa.uint16(), nullable=False),
            ]
        )
        posting_table = pa.Table.from_pylist(
            [dict(zip(posting_schema.names, row, strict=True)) for row in postings],
            schema=posting_schema,
        )
        posting_sink = pa.BufferOutputStream()
        pq.write_table(posting_table, posting_sink)
        posting_bytes = posting_sink.getvalue().to_pybytes()
        leaf_child = pa.field("element", pa.float16(), nullable=False)
        leaf_vectors = np.zeros((2, 96), dtype=np.float16)
        leaf_vectors[:, :2] = np.eye(2, dtype=np.float16)
        leaf_table = pa.Table.from_arrays(
            [
                pa.array([0, 0], type=pa.uint16()),
                pa.FixedSizeListArray.from_arrays(
                    pa.array(leaf_vectors.ravel(), type=pa.float16()), 96
                ),
            ],
            schema=pa.schema(
                [
                    pa.field("root_ordinal", pa.uint16(), nullable=False),
                    pa.field("centroid", pa.list_(leaf_child, 96), nullable=False),
                ]
            ),
        )
        leaf_sink = pa.BufferOutputStream()
        with ipc.new_file(leaf_sink, leaf_table.schema) as writer:
            writer.write_table(leaf_table)
        leaf_bytes = leaf_sink.getvalue().to_pybytes()
        query_child = pa.field("element", pa.float32(), nullable=False)
        query_vectors = np.zeros((64, 96), dtype=np.float32)
        query_vectors[np.arange(64), np.arange(64)] = 1.0
        query_table = pa.Table.from_arrays(
            [pa.FixedSizeListArray.from_arrays(pa.array(query_vectors.ravel()), 96)],
            schema=pa.schema([pa.field("emb", pa.list_(query_child, 96), nullable=False)]),
        )
        query_sink = pa.BufferOutputStream()
        pq.write_table(query_table, query_sink)
        query_bytes = query_sink.getvalue().to_pybytes()
        role_bytes = {
            "pages-manifest": manifest_bytes,
            "leaf-postings": posting_bytes,
            "leaf-centroids": leaf_bytes,
            "query-parquet": query_bytes,
        }
        artifacts = []
        for role in ("pages-manifest", "leaf-postings", "leaf-centroids", "query-parquet"):
            uri = f"s3://frozen/{role}"
            body = role_bytes[role]
            objects[uri] = body
            artifacts.append(ArtifactAuthority(role, uri, hashlib.sha256(body).hexdigest(), len(body)))
        return tuple(artifacts), objects

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

    def test_v30_reproduction_pq8_training_and_adc_are_deterministic(self) -> None:
        # Break caught: the bounded reproduction uses a different codebook seed, silently
        # changes PQ geometry, or compares raw width-specific scores.
        rows = np.array(
            [[float((row + dimension) % 7) for dimension in range(8)] for row in range(16)],
            dtype=np.float32,
        )
        first = fit_pq8(
            rows,
            width_bytes=2,
            centroid_count=4,
            sample_size=8,
            iterations=2,
            batch_rows=5,
        )
        second = fit_pq8(
            rows,
            width_bytes=2,
            centroid_count=4,
            sample_size=8,
            iterations=2,
            batch_rows=5,
        )
        np.testing.assert_array_equal(first.centroids, second.centroids)
        codes, reconstruction_error = encode_pq8(first, rows, batch_rows=3)
        self.assertEqual(codes.shape, (16, 2))
        self.assertEqual(codes.dtype, np.uint8)
        self.assertTrue(np.isfinite(reconstruction_error).all())
        query = rows[5]
        scores = first.score(codes, query)
        expected = np.zeros(len(rows), dtype=np.float32)
        for subquantizer in range(2):
            start = subquantizer * 4
            centers = first.centroids[subquantizer]
            expected += ((centers[codes[:, subquantizer]] - query[start : start + 4]) ** 2).sum(axis=1)
        np.testing.assert_array_equal(scores, expected)

    def test_v30_reproduction_page_layout_is_base_only_and_one_owner(self) -> None:
        # Break caught: changing the high-fidelity population changes page membership or
        # construction emits duplicate/missing owners.
        primary_leaf = np.array([1, 0, 1, 0, 1, 0, 1, 0], dtype=np.int32)
        base_codes = np.array([[7 - row, row % 3] for row in range(8)], dtype=np.uint8)
        pages, row_page, leaf_rows = build_base_page_layout(
            primary_leaf,
            base_codes,
            leaf_count=2,
            page_rows=2,
        )
        self.assertEqual(len(pages), 4)
        self.assertEqual(sorted(np.concatenate(pages).tolist()), list(range(8)))
        self.assertEqual(sorted(np.concatenate(tuple(leaf_rows.values())).tolist()), list(range(8)))
        for page_ordinal, rows in enumerate(pages):
            self.assertTrue(np.all(row_page[rows] == page_ordinal))
            self.assertEqual(len(set(primary_leaf[rows].tolist())), 1)

    def test_v30_reproduction_evaluates_all_arms_over_one_fixed_page_layout(self) -> None:
        # Break caught: diagnostic arms rebuild pages, use approximate truth, or fetch more
        # than ten bounded exact-vector pages before reranking.
        rows = np.zeros((64, 8), dtype=np.float32)
        for row in range(64):
            rows[row, row % 8] = 1.0
            rows[row] += np.float32(row / 10_000)
            rows[row] /= np.linalg.norm(rows[row])
        queries = rows[:32].copy()
        leaves = np.zeros((1, 8), dtype=np.float32)
        primary_leaf = np.zeros(64, dtype=np.int32)
        residuals = rows.copy()
        base = fit_pq8(
            residuals,
            width_bytes=2,
            centroid_count=4,
            sample_size=32,
            iterations=2,
        )
        high = fit_pq8(
            residuals,
            width_bytes=4,
            centroid_count=4,
            sample_size=32,
            iterations=2,
        )
        truth = exact_truth(rows, queries)
        observations = evaluate_pq8_replacement_arms(
            rows,
            primary_leaf,
            leaves,
            queries,
            truth,
            base,
            high,
            page_rows=2,
            leaf_beam=1,
            candidate_depth=64,
            page_encoded_bytes=(100,) * 32,
        )
        self.assertEqual(
            tuple(observation.fidelity_fraction_ppm for observation in observations),
            (0, 50_000, 100_000, 200_000),
        )
        for observation in observations:
            self.assertEqual(observation.selected_page_counts, (10,) * 32)
            self.assertEqual(observation.maximum_encoded_bytes, 1_000)
            self.assertEqual(observation.maximum_scanned_codes, 64)

    def test_v30_reproduction_streams_strict_arrow_pages_from_exact_s3_authority(self) -> None:
        # Break caught: the worker lists/discovers pages, accepts schema drift, or persists a
        # local corpus instead of authenticating each bounded Arrow object into memory.
        artifacts, objects = self.frozen_fixture()
        calls: list[str] = []

        def get_object(uri: str) -> bytes:
            calls.append(uri)
            return objects[uri]

        loaded = load_frozen_reproduction(
            artifacts,
            page_prefix="s3://frozen/pages",
            get_object=get_object,
            expected_source_rows=64,
            expected_query_rows=64,
        )
        self.assertEqual(loaded.primary.shape, (64, 96))
        self.assertEqual(loaded.queries.shape, (32, 96))
        self.assertEqual(loaded.leaf_centroids.shape, (2, 96))
        self.assertEqual(sorted(np.unique(loaded.primary_leaf).tolist()), [0, 1])
        self.assertEqual(len(calls), 8)
        self.assertEqual(set(calls), set(objects))
        broken = dict(objects)
        page_uri = next(uri for uri in broken if uri.endswith(".arrow"))
        broken[page_uri] = broken[page_uri][:-1] + bytes([broken[page_uri][-1] ^ 1])
        with self.assertRaisesRegex(ValueError, "page byte authority"):
            load_frozen_reproduction(
                artifacts,
                page_prefix="s3://frozen/pages",
                get_object=broken.__getitem__,
                expected_source_rows=64,
                expected_query_rows=64,
            )

    def test_v30_reproduction_cli_is_explicit_and_has_no_corpus_download_mode(self) -> None:
        # Break caught: the worker discovers latest artifacts or exposes a persistent/full-corpus
        # staging option instead of one explicit in-memory S3 stream.
        arguments = ["reproduce", "--execute"]
        for artifact in self.artifacts():
            arguments.extend(
                [
                    f"--{artifact.role}-uri",
                    artifact.uri,
                    f"--{artifact.role}-sha256",
                    artifact.sha256,
                    f"--{artifact.role}-bytes",
                    str(artifact.encoded_bytes),
                ]
            )
        arguments.extend(
            [
                "--page-prefix",
                "s3://frozen/pages",
                "--evidence-parquet",
                "/tmp/evidence.parquet",
            ]
        )
        plan = parse_args(arguments)
        self.assertEqual(plan.artifacts, self.artifacts())
        self.assertEqual(plan.page_prefix, "s3://frozen/pages")
        with self.assertRaisesRegex(ValueError, "unknown"):
            parse_args([*arguments, "--download-corpus", "/tmp/corpus"])
        with self.assertRaisesRegex(ValueError, "execute"):
            parse_args([value for value in arguments if value != "--execute"])

    def test_v30_reproduction_evidence_is_parquet_and_result_binds_it(self) -> None:
        # Break caught: per-query misses/work disappear into aggregate JSON or the result can be
        # paired with a different cross-language evidence table.
        observations = tuple(
            V30ArmObservation(
                fidelity_fraction_ppm=fraction,
                hits=tuple(9 + (query != 11) for query in range(32))
                if fraction == 50_000
                else (9,) * 32,
                selected_page_counts=(10,) * 32,
                maximum_encoded_bytes=4_000_000,
                maximum_scanned_codes=100_000,
            )
            for fraction in (0, 50_000, 100_000, 200_000)
        )
        evidence = encode_reproduction_evidence(observations)
        table = pq.read_table(pa.BufferReader(evidence))
        self.assertEqual(table.num_rows, 128)
        self.assertEqual(
            table.schema,
            pa.schema(
                [
                    pa.field("fidelity_fraction_ppm", pa.uint32(), nullable=False),
                    pa.field("query_ordinal", pa.uint16(), nullable=False),
                    pa.field("hits", pa.uint8(), nullable=False),
                    pa.field("selected_pages", pa.uint8(), nullable=False),
                    pa.field("maximum_encoded_bytes", pa.uint64(), nullable=False),
                    pa.field("maximum_scanned_codes", pa.uint64(), nullable=False),
                ]
            ),
        )
        result = finalize_reproduction_result(
            observations,
            self.artifacts(),
            construction_bytes_streamed=46_761_076,
            evidence_parquet=evidence,
        )
        value = json.loads(result)
        self.assertEqual(value["evidence_parquet_sha256"], hashlib.sha256(evidence).hexdigest())
        self.assertEqual(value["evidence_parquet_bytes"], len(evidence))
        self.assertEqual(value["construction_bytes_streamed"], 46_761_076)
        self.assertEqual(len(value["artifacts"]), 4)
        self.assertEqual(result, json.dumps(value, allow_nan=False, separators=(",", ":"), sort_keys=True).encode() + b"\n")


if __name__ == "__main__":
    unittest.main()
