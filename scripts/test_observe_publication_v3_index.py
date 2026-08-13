import unittest

from scripts.observe_publication_v3_index import observe_index_inventory
from scripts.test_seal_publication_v3_index import _FakeS3


class PublicationV3IndexObservationTests(unittest.TestCase):
    def test_live_listing_binds_roster_checksums_to_current_sizes_and_etags(self) -> None:
        prefix = "publication/indexes/index-1/"
        objects = {
            prefix + "segments/L0/a.parquet": b"PAR1segment-aPAR1",
            prefix + "collection/CURRENT": b'{"schema_version":1}',
        }
        client = _FakeS3(objects)
        roster = [
            {
                "role": "data-bundle" if path.startswith("segments/") else "control",
                "path": path,
                "format": "parquet" if path.endswith(".parquet") else "json",
                "bytes": len(body),
                "rows": 100 if path.startswith("segments/") else 0,
                "checksum": f"{index + 1:064x}",
                "etag": f'"{index + 1:032x}"',
            }
            for index, (path, body) in enumerate(
                (key[len(prefix) :], body) for key, body in sorted(objects.items())
            )
        ]

        inventory = observe_index_inventory(
            client, bucket="bucket", prefix=prefix, roster=roster
        )

        self.assertEqual(
            inventory,
            [
                {key: item[key] for key in ("path", "bytes", "checksum", "etag")}
                for item in roster
            ],
        )
        client.second_listing[0]["ETag"] = '"ffffffffffffffffffffffffffffffff"'
        with self.assertRaisesRegex(ValueError, "differs from sealed roster"):
            observe_index_inventory(
                client, bucket="bucket", prefix=prefix, roster=roster
            )


if __name__ == "__main__":
    unittest.main()
