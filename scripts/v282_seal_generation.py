"""Seal existing corpus-only router, graph and SQ8 page metadata into one root."""

from __future__ import annotations

import argparse
import hashlib
import json
import shutil
import tempfile
from pathlib import Path


def digest(path: Path) -> str:
    value = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(4 * 1024 * 1024), b""):
            value.update(chunk)
    return value.hexdigest()


def seal(router: Path, pages: Path, routing: Path, etag: str, output: Path) -> str:
    if output.exists() or not etag or any(ord(c) < 32 for c in etag):
        raise ValueError("output or ETag differs")
    router_manifest = json.loads((router / "manifest.json").read_bytes())
    page_manifest = json.loads((pages / "manifest.json").read_bytes())
    graph_manifest = json.loads((routing / "build.json").read_bytes())
    geometry = router_manifest["geometry"]
    if (router_manifest["schema"] != "borsuk-source-router-v2"
            or page_manifest["schema"] != "borsuk-v115-sq8-page-authority-v2"
            or graph_manifest["schema"] != "borsuk-v282-routing-build-v1"
            or router_manifest["generation"] != page_manifest["generation"]
            or router_manifest["sq8_sha256"] != page_manifest["object_sha256"]
            or graph_manifest["sq8_sha256"] != router_manifest["sq8_sha256"]
            or (geometry["rows"], geometry["dimensions"], geometry["page_rows"])
            != (page_manifest["rows"], page_manifest["dimensions"], page_manifest["page_rows"])
            or (graph_manifest["rows"], graph_manifest["dimensions"],
                graph_manifest["page_rows"], graph_manifest["unit_rows"])
            != (geometry["rows"], geometry["dimensions"], geometry["page_rows"], 32)
            or graph_manifest["graph_resident_bytes"] <= 0):
        raise ValueError("generation geometry or identity differs")
    if set(router_manifest["sections"]) != {"summaries", "books", "codes", "low", "step"}:
        raise ValueError("router sections differ")
    for name, section in router_manifest["sections"].items():
        path = router / f"{name}.bin"
        if path.stat().st_size != section["bytes"] or digest(path) != section["sha256"]:
            raise ValueError(f"router section differs: {name}")
    sidecar = pages / "pages.sha256"
    if (sidecar.stat().st_size != ((geometry["rows"] + 255) // 256) * 32
            or digest(sidecar) != page_manifest["page_digest_sha256"]):
        raise ValueError("page digest sidecar differs")
    for name, field in (("centroids.bin", "centroids_sha256"),
                        ("graph.bin", "graph_sha256")):
        if digest(routing / name) != graph_manifest[field]:
            raise ValueError(f"routing blob differs: {name}")
    root_manifest = {
        "schema": "borsuk-object-native-generation-v1",
        "generation": router_manifest["generation"],
        "rows": geometry["rows"], "dimensions": geometry["dimensions"],
        "page_rows": geometry["page_rows"], "unit_rows": graph_manifest["unit_rows"],
        "router_manifest_sha256": digest(router / "manifest.json"),
        "page_manifest_sha256": digest(pages / "manifest.json"),
        "centroids_sha256": graph_manifest["centroids_sha256"],
        "graph_sha256": graph_manifest["graph_sha256"],
        "graph_resident_bytes": graph_manifest["graph_resident_bytes"],
        "sq8_object_sha256": page_manifest["object_sha256"],
        "sq8_object_key": f"objects/{page_manifest['object_sha256']}",
        "sq8_etag": etag,
    }
    with tempfile.TemporaryDirectory(prefix=output.name + ".tmp-", dir=output.parent) as temporary:
        pending = Path(temporary)
        shutil.copytree(router, pending / "router")
        shutil.copy2(pages / "manifest.json", pending / "page_manifest.json")
        shutil.copy2(sidecar, pending / "page_digests.bin")
        shutil.copy2(routing / "centroids.bin", pending / "centroids.bin")
        shutil.copy2(routing / "graph.bin", pending / "graph.bin")
        (pending / "manifest.json").write_text(
            json.dumps(root_manifest, sort_keys=True, separators=(",", ":")) + "\n"
        )
        root_sha = digest(pending / "manifest.json")
        pending.rename(output)
    return root_sha


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--router", type=Path, required=True)
    parser.add_argument("--pages", type=Path, required=True)
    parser.add_argument("--routing", type=Path, required=True)
    parser.add_argument("--etag", required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    print(seal(args.router, args.pages, args.routing, args.etag, args.output))
