"""One trust-boundary check for the V282 generation sealer."""

import json
import tempfile
import unittest
from pathlib import Path

from scripts.v282_seal_generation import digest, seal


class SealGenerationTest(unittest.TestCase):
    def test_seals_bound_files_and_rejects_tamper(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            router, pages, routing = (root / name for name in ("router", "pages", "routing"))
            for directory in (router, pages, routing):
                directory.mkdir()
            sq8_sha = "a" * 64
            sections = {}
            for name in ("summaries", "books", "codes", "low", "step"):
                raw = name.encode()
                (router / f"{name}.bin").write_bytes(raw)
                sections[name] = {"bytes": len(raw), "sha256": digest(router / f"{name}.bin")}
            (router / "manifest.json").write_text(json.dumps({
                "schema": "borsuk-source-router-v2", "generation": 1,
                "sq8_sha256": sq8_sha,
                "geometry": {"rows": 256, "dimensions": 768, "page_rows": 256},
                "sections": sections,
            }))
            (pages / "pages.sha256").write_bytes(b"p" * 32)
            (pages / "manifest.json").write_text(json.dumps({
                "schema": "borsuk-v115-sq8-page-authority-v2", "generation": 1,
                "rows": 256, "dimensions": 768, "page_rows": 256,
                "object_sha256": sq8_sha,
                "page_digest_sha256": digest(pages / "pages.sha256"),
            }))
            for name in ("centroids", "graph"):
                (routing / f"{name}.bin").write_bytes(name.encode())
            (routing / "build.json").write_text(json.dumps({
                "schema": "borsuk-v282-routing-build-v1", "rows": 256,
                "dimensions": 768, "unit_rows": 32, "page_rows": 256,
                "sq8_sha256": sq8_sha,
                "centroids_sha256": digest(routing / "centroids.bin"),
                "graph_sha256": digest(routing / "graph.bin"),
                "graph_resident_bytes": 1,
            }))
            output = root / "sealed"
            object_key = f"tenant/g1/objects/{sq8_sha}"
            expected = seal(router, pages, routing, '"etag"', output,
                            object_key=object_key)
            self.assertEqual(expected, digest(output / "manifest.json"))
            manifest = json.loads((output / "manifest.json").read_text())
            self.assertEqual(manifest["sq8_etag"], '"etag"')
            self.assertEqual(manifest["sq8_object_key"], object_key)
            self.assertEqual(manifest["schema"], "borsuk-object-native-generation-v2")
            with self.assertRaisesRegex(ValueError, "object key"):
                seal(router, pages, routing, '"etag"', root / "unsafe",
                     object_key=f"tenant/../objects/{sq8_sha}")
            (routing / "graph.bin").write_bytes(b"tampered")
            with self.assertRaisesRegex(ValueError, "routing blob differs"):
                seal(router, pages, routing, '"etag"', root / "rejected",
                     object_key=object_key)


if __name__ == "__main__":
    unittest.main()
