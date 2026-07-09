"""Cross-language parity: the shared fixture must produce identical results here
and in the TypeScript binding (packages/borsuk/test/parity.test.ts)."""

import json
import tempfile
import unittest
from pathlib import Path

import borsuk

FIXTURE = (
    Path(__file__).resolve().parents[2] / "tests" / "fixtures" / "metadata_parity.json"
)


def local_uri(path: str) -> str:
    return Path(path).as_uri()


class MetadataParityTest(unittest.TestCase):
    def test_shared_fixture_matches_expected_results(self) -> None:
        spec = json.loads(FIXTURE.read_text())
        records = spec["records"]
        by_id = {record["id"]: record for record in records}

        with tempfile.TemporaryDirectory() as tmp:
            index = borsuk.create(
                uri=local_uri(tmp),
                metric=spec["metric"],
                dim=spec["dimensions"],
                segment_size=spec["segmentMaxVectors"],
            )
            index.add(
                [record["vector"] for record in records],
                ids=[record["id"] for record in records],
                metadata=[record["metadata"] for record in records],
            )

            for query in spec["queries"]:
                report = index.search_with_report(
                    query["vector"],
                    k=query["k"],
                    filter=query["filter"],
                    include_metadata=True,
                )
                actual_ids = [hit.id for hit in report.hits]
                self.assertEqual(
                    actual_ids, query["expectedIds"], msg=f"ids for {query['name']}"
                )
                for hit in report.hits:
                    self.assertEqual(
                        hit.metadata,
                        by_id[hit.id]["metadata"],
                        msg=f"metadata for {hit.id} in {query['name']}",
                    )

                # search_ids honors the same filter and returns the same members.
                ids_only = index.search_ids(
                    query["vector"], k=query["k"], filter=query["filter"]
                )
                self.assertEqual(
                    ids_only,
                    query["expectedIds"],
                    msg=f"search_ids for {query['name']}",
                )

            # get_record returns each stored vector with its metadata.
            for record in records:
                vector, meta = index.get_record(record["id"])
                self.assertEqual(vector, [float(value) for value in record["vector"]])
                self.assertEqual(meta, record["metadata"])


if __name__ == "__main__":
    unittest.main()
