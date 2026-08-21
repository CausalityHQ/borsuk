import hashlib
import io
import unittest

from scripts.seal_publication_v3_index import (
    _validate_physical_bytes,
    classify_index_object,
    seal_index_inventory,
    session_configuration,
)


class _Paginator:
    def __init__(self, client: "_FakeS3") -> None:
        self.client = client

    def paginate(self, **kwargs: object) -> list[dict[str, object]]:
        self.client.list_calls += 1
        objects = self.client.first_listing if self.client.list_calls == 1 else self.client.second_listing
        return [{"Contents": objects}]


class _FakeS3:
    def __init__(self, objects: dict[str, bytes]) -> None:
        self.objects = objects
        self.list_calls = 0
        self.first_listing = self._listing()
        self.second_listing = self._listing()

    def _listing(self) -> list[dict[str, object]]:
        return [
            {"Key": key, "Size": len(body), "ETag": f'"{index + 1:032x}"'}
            for index, (key, body) in enumerate(sorted(self.objects.items()))
        ]

    def get_paginator(self, name: str) -> _Paginator:
        if name != "list_objects_v2":
            raise AssertionError(name)
        return _Paginator(self)

    def get_object(self, *, Bucket: str, Key: str) -> dict[str, object]:
        del Bucket
        listing = {item["Key"]: item for item in self.first_listing}
        return {
            "Body": io.BytesIO(self.objects[Key]),
            "ContentLength": len(self.objects[Key]),
            "ETag": listing[Key]["ETag"],
        }


class PublicationV3IndexSealTests(unittest.TestCase):
    def test_transaction_state_requires_current_bws2_magic(self) -> None:
        path = "transactions/abcd/STATE"
        _validate_physical_bytes(path, "packed", b"BWS2state")
        with self.assertRaisesRegex(ValueError, "invalid magic"):
            _validate_physical_bytes(path, "packed", b"BWS1state")

    def test_worker_uses_instance_credentials_when_no_named_profile_is_supplied(self) -> None:
        self.assertEqual(
            session_configuration(None, "eu-central-1"),
            {"region_name": "eu-central-1"},
        )
        self.assertEqual(
            session_configuration("causality", "eu-central-1"),
            {"profile_name": "causality", "region_name": "eu-central-1"},
        )

    def test_classification_matches_real_standard_and_packed_path_families(self) -> None:
        cases = {
            "segments/L0/segment.parquet": ("data-bundle", "parquet"),
            "vectors/aa/vector.arrow": ("query-page", "arrow-ipc"),
            "global-cell-cards/v20/groups/card.arrow": (
                "query-page",
                "arrow-ipc",
            ),
            "global-cell-cards/v20/roots/aa/root-card.parquet": (
                "query-page",
                "parquet",
            ),
            "fidx/aa/filter.fidx": ("query-page", "packed"),
            "id-directory/runs/run.parquet": ("directory", "parquet"),
            "collection/CURRENT": ("control", "json"),
            "lane-log/ACTIVE": ("control", "json"),
            "positioned-log/heads/00.json": ("control", "json"),
            "transactions/abcd/STATE": ("control", "packed"),
            "id-directory/claim-pages/00/STATE": ("control", "packed"),
        }
        for path, expected in cases.items():
            with self.subTest(path=path):
                self.assertEqual(classify_index_object(path), expected)
        for path in (
            "unknown/blob",
            "segments/L0/segment.bin",
            "fidx/x.parquet",
            "global-cell-cards/v15/groups/card.arrow",
            "global-cell-cards/v15/roots/aa/root-card.parquet",
            "global-cell-cards/v16/groups/card.arrow",
            "global-cell-cards/v16/roots/aa/root-card.parquet",
            "global-cell-cards/v17/groups/card.arrow",
            "global-cell-cards/v17/roots/aa/root-card.parquet",
            "global-cell-cards/v18/groups/card.arrow",
            "global-cell-cards/v18/roots/aa/root-card.parquet",
            "global-cell-cards/v19/groups/card.arrow",
            "global-cell-cards/v19/roots/aa/root-card.parquet",
        ):
            with self.subTest(path=path), self.assertRaises(ValueError):
                classify_index_object(path)

    def test_seal_hashes_every_object_counts_segment_rows_and_detects_listing_drift(self) -> None:
        prefix = "publication/indexes/index-1/"
        objects = {
            prefix + "segments/L0/a.parquet": b"PAR1segment-aPAR1",
            prefix + "vectors/aa/a.arrow": b"ARROW1vector-aARROW1",
            prefix + "fidx/aa/a.fidx": b"a" * 96,
            prefix + "collection/CURRENT": b'{"schema_version":1}',
            prefix + "transactions/abcd/STATE": b"BWS2state",
        }
        client = _FakeS3(objects)

        roster, inventory = seal_index_inventory(
            client,
            bucket="bucket",
            prefix=prefix,
            parquet_rows=lambda path, body: 100 if path.startswith("segments/") else 0,
        )

        self.assertEqual([item["path"] for item in roster], sorted(item["path"] for item in roster))
        self.assertEqual(sum(item["rows"] for item in roster), 100)
        self.assertEqual(
            inventory,
            [
                {key: item[key] for key in ("path", "bytes", "checksum", "etag")}
                for item in roster
            ],
        )
        for item in roster:
            body = objects[prefix + item["path"]]
            self.assertEqual(item["checksum"], hashlib.sha256(body).hexdigest())
        client.list_calls = 0
        client.second_listing = client.second_listing[:-1]
        with self.assertRaisesRegex(ValueError, "changed while sealing"):
            seal_index_inventory(
                client,
                bucket="bucket",
                prefix=prefix,
                parquet_rows=lambda path, body: 100 if path.startswith("segments/") else 0,
            )


if __name__ == "__main__":
    unittest.main()
