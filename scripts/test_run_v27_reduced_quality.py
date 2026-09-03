import hashlib
import json
import pathlib
import stat
import sys
import tempfile
import unittest

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

from scripts.run_v27_reduced_quality import (
    V27Artifact,
    V27QualifierPlan,
    V27SearchObservation,
    evaluate_v27_reduced_quality,
    load_v27_vectors,
    parse_args,
    run_v27_qualifier,
)


class V27ReducedQualityTests(unittest.TestCase):
    def vectors(self) -> tuple[np.ndarray, np.ndarray]:
        train = np.zeros((64, 96), dtype=np.float32)
        train[np.arange(64), np.arange(64)] = 1.0
        return train, train[:32].copy()

    @staticmethod
    def exact_ids(query_index: int) -> list[int]:
        return [query_index, *[ordinal for ordinal in range(64) if ordinal != query_index][:9]]

    def observation(self, query_index: int, *, miss: bool = False) -> V27SearchObservation:
        ids = self.exact_ids(query_index)
        if miss and query_index == 7:
            ids[-1] = 63
        return V27SearchObservation(
            source_ordinals=tuple(ids),
            get_count=10,
            encoded_bytes=4_000_000,
            decoded_rows=10_000,
            unique_rows=9_000,
        )

    def test_v27_reduced_quality_loads_only_authenticated_parquet_rows(self) -> None:
        # Break caught: qualification silently reads a different/full corpus or accepts a loose
        # list schema instead of the registered cross-language f32[96] Parquet boundary.
        train, _queries = self.vectors()
        child = pa.field("element", pa.float32(), nullable=False)
        vector_type = pa.list_(child, 96)
        array = pa.FixedSizeListArray.from_arrays(pa.array(train.ravel()), 96)
        table = pa.Table.from_arrays(
            [array], schema=pa.schema([pa.field("emb", vector_type, nullable=False)])
        )
        with tempfile.TemporaryDirectory() as directory:
            path = pathlib.Path(directory) / "train.parquet"
            pq.write_table(table, path)
            payload = path.read_bytes()
            loaded = load_v27_vectors(
                path,
                hashlib.sha256(payload).hexdigest(),
                len(payload),
                column="emb",
                row_limit=32,
            )
            np.testing.assert_array_equal(loaded, train[:32])
            with self.assertRaisesRegex(ValueError, "byte authority"):
                load_v27_vectors(
                    path,
                    "0" * 64,
                    len(payload),
                    column="emb",
                    row_limit=32,
                )

    def test_v27_reduced_quality_runs_the_explicit_s3_qualifier_boundary(self) -> None:
        # Break caught: the quality gate substitutes an in-process approximation instead of
        # invoking the same explicit S3 qualifier and preserving its truthful work counters.
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            program = root / "qualifier.py"
            program.write_text(
                """import json,sys
row=int(sys.argv[sys.argv.index('--query-row')+1])
value={'claim_eligible':False,'matches':[{'source_ordinal':i,'squared_distance':float(i)} for i in range(10)],'schema_version':1,'work':{'decoded_rows':10000,'encoded_bytes':4000000,'get_count':10,'routing':{'leaf_centroids_scored':64,'page_modes_scored':10,'peak_page_candidates':10,'postings_visited':128,'root_centroids_scored':64,'selected_pages':10},'unique_rows':9000}}
sys.stdout.write(json.dumps(value,allow_nan=False,separators=(',',':'),sort_keys=True)+'\\n')
"""
            )
            program.chmod(program.stat().st_mode | stat.S_IXUSR)
            artifact = V27Artifact(root / "artifact", "1" * 64, 7)
            plan = V27QualifierPlan(
                command=(sys.executable, str(program)),
                roots=artifact,
                leaves=artifact,
                postings=artifact,
                modes=artifact,
                manifest=artifact,
                query=artifact,
                s3_page_prefix="s3://bucket/run/index/pages",
                root_beam=8,
                leaf_beam=128,
                page_count=10,
            )
            observed = run_v27_qualifier(plan, 3)
            self.assertEqual(observed.source_ordinals, tuple(range(10)))
            self.assertEqual(observed.get_count, 10)
            self.assertEqual(observed.encoded_bytes, 4_000_000)
            self.assertEqual(observed.decoded_rows, 10_000)
            self.assertEqual(observed.unique_rows, 9_000)

    def test_v27_reduced_quality_cli_is_explicit_and_has_no_full_corpus_mode(self) -> None:
        # Break caught: qualification discovers latest artifacts or exposes a full-corpus local
        # download instead of requiring the bounded shard and immutable index authorities.
        values = ["quality", "--execute"]
        for role in ("train", "query", "roots", "leaves", "postings", "modes", "manifest"):
            path_flag = f"{role}-parquet" if role in {"train", "query"} else role
            values.extend(
                [
                    f"--{path_flag}",
                    f"/{role}",
                    f"--{role}-sha256",
                    "1" * 64,
                    f"--{role}-bytes",
                    "7",
                ]
            )
        values.extend(
            [
                "--qualifier-binary",
                "/qualifier",
                "--s3-page-prefix",
                "s3://bucket/run/index/pages",
                "--root-beam",
                "8",
                "--leaf-beam",
                "128",
                "--page-count",
                "10",
            ]
        )
        parsed = parse_args(values)
        self.assertEqual(parsed.train.path, pathlib.Path("/train"))
        self.assertEqual(parsed.query.path, pathlib.Path("/query"))
        self.assertEqual(parsed.qualifier.page_count, 10)
        with self.assertRaisesRegex(ValueError, "execute"):
            parse_args([value for value in values if value != "--execute"])
        with self.assertRaisesRegex(ValueError, "unknown"):
            parse_args([*values, "--full-corpus", "true"])

    def test_v27_reduced_quality_computes_exact_top_ten_and_perfect_recall(self) -> None:
        # Break caught: reduced qualification trusts full-corpus neighbors or approximate output
        # instead of computing exact top-10 truth over the bounded construction rows.
        train, queries = self.vectors()
        receipt = evaluate_v27_reduced_quality(
            train,
            queries,
            lambda query_index, _query: self.observation(query_index),
        )
        value = json.loads(receipt)
        self.assertEqual(value["queries"], 32)
        self.assertEqual(value["aggregate_recall_ppm"], 1_000_000)
        self.assertEqual(value["minimum_recall_ppm"], 1_000_000)
        self.assertEqual(value["maximum_get_count"], 10)
        self.assertEqual(value["maximum_encoded_bytes"], 4_000_000)
        self.assertEqual(value["status"], "passed")
        self.assertFalse(value["claim_eligible"])
        self.assertEqual(
            receipt,
            json.dumps(value, allow_nan=False, separators=(",", ":"), sort_keys=True).encode()
            + b"\n",
        )

    def test_v27_reduced_quality_exposes_one_miss_instead_of_averaging_it_away(self) -> None:
        # Break caught: one failed query is hidden by an aggregate-only recall threshold before
        # the expensive large-corpus campaign.
        train, queries = self.vectors()
        value = json.loads(
            evaluate_v27_reduced_quality(
                train,
                queries,
                lambda query_index, _query: self.observation(query_index, miss=True),
            )
        )
        self.assertEqual(value["aggregate_recall_ppm"], 996_875)
        self.assertEqual(value["minimum_recall_ppm"], 900_000)
        self.assertEqual(value["failed_query_ordinals"], [7])
        self.assertEqual(value["status"], "failed")


if __name__ == "__main__":
    unittest.main()
