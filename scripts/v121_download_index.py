"""Authenticate V120's corpus-only index before any query or GT download."""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
from urllib.parse import urlsplit

V120_COMMIT = "b919685cf1db6c14912c2118d5226c0fb426226b"
V119_SOURCE_SHA256 = "8f88122f412554107d97c07f440352f9043b8cb4b58fe08434ac75f4b90776ee"
REQUIRED = (
    "built/manifest.json", "built/layout.npy", "built/sq8.bin",
    "router/manifest.json", "router/summaries.bin", "router/books.bin",
    "router/codes.bin", "router/low.bin", "router/step.bin",
    "mirror/manifest.json", "mirror/source.json", "mirror/blocks.sha256",
    "authority/manifest.json", "authority/pages.sha256",
)


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        while block := source.read(4 * 1024 * 1024):
            digest.update(block)
    return digest.hexdigest()


def _valid_sha(value: object) -> bool:
    return type(value) is str and len(value) == 64 and all(
        character in "0123456789abcdef" for character in value
    )


def validate_terminal(terminal: dict) -> dict[str, dict]:
    if (type(terminal) is not dict
            or terminal.get("schema") != "borsuk-v120-source-index-spot-v1"
            or terminal.get("source_commit") != V120_COMMIT
            or terminal.get("status") != "complete"
            or terminal.get("phase") != "complete"
            or terminal.get("exit_code") != 0):
        raise ValueError("V120 terminal is not a complete source-only build")
    artifacts = terminal.get("artifacts")
    if type(artifacts) is not dict or any(name not in artifacts for name in REQUIRED):
        raise ValueError("V120 terminal artifact set differs")
    selected = {name: artifacts[name] for name in REQUIRED}
    if any(type(item) is not dict
           or type(item.get("bytes")) is not int or item["bytes"] <= 0
           or not _valid_sha(item.get("sha256")) for item in selected.values()):
        raise ValueError("V120 terminal artifact identity differs")
    return selected


def verify_bindings(root: Path) -> None:
    """Check that independently hashed planes name one source and generation."""
    built = json.loads((root / "built/manifest.json").read_text())
    router = json.loads((root / "router/manifest.json").read_text())
    if (built.get("source_sha256") != V119_SOURCE_SHA256
            or built.get("rows") != 9_990_000
            or built.get("dimensions") != 96
            or built.get("query_or_truth_used") is not False):
        raise ValueError("V120 built manifest source binding differs")
    expected_geometry = {"rows": 9_990_000, "dimensions": 96,
                         "page_rows": 256, "blocks_per_page": 2,
                         "subspaces": 64, "pq_width": 2,
                         "pq_partition": "balanced_floor_v1"}
    if (router.get("schema") != "borsuk-source-router-v2"
            or router.get("generation") != 1
            or router.get("source_sha256") != built["source_sha256"]
            or router.get("layout_sha256") != built.get("layout_sha256")
            or router.get("sq8_sha256") != built.get("sq8_sha256")
            or router.get("geometry") != expected_geometry):
        raise ValueError("V120 router manifest binding differs")
    mirror = json.loads((root / "mirror/manifest.json").read_text())
    if (mirror.get("format_version") != 1
            or mirror.get("generation") != 1
            or mirror.get("max_nominees", 0) < 512
            or mirror.get("geometry") != {"rows": 9_990_000, "dimensions": 96}
            or mirror.get("object_sha256") != built["sq8_sha256"]
            or mirror.get("block_digest_sha256") != _sha256_file(
                root / "mirror/blocks.sha256")):
        raise ValueError("V120 SQ8 mirror binding differs")
    mirror_source = json.loads((root / "mirror/source.json").read_text())
    if (mirror_source.get("source_sha256") != built["source_sha256"]
            or mirror_source.get("sq8_sha256") != built["sq8_sha256"]
            or mirror_source.get("manifest_sha256") != _sha256_file(
                root / "mirror/manifest.json")):
        raise ValueError("V120 mirror source binding differs")
    authority = json.loads((root / "authority/manifest.json").read_text())
    if (authority.get("schema") != "borsuk-v115-sq8-page-authority-v2"
            or authority.get("generation") != 1
            or authority.get("rows") != 9_990_000
            or authority.get("dimensions") != 96
            or authority.get("page_rows") != 256
            or authority.get("object_sha256") != built["sq8_sha256"]
            or authority.get("page_digest_sha256") != _sha256_file(
                root / "authority/pages.sha256")):
        raise ValueError("V120 SQ8 page authority binding differs")
    if (_sha256_file(root / "built/layout.npy") != built["layout_sha256"]
            or _sha256_file(root / "sq8.bin") != built["sq8_sha256"]):
        raise ValueError("V120 physical layout or SQ8 digest differs")


def download_index(index_prefix: str, terminal_sha256: str, output_root: Path) -> None:
    import boto3

    if not _valid_sha(terminal_sha256) or output_root != Path("."):
        raise ValueError("V120 index consumer invocation differs")
    parsed = urlsplit(index_prefix.rstrip("/"))
    if (parsed.scheme != "s3" or not parsed.netloc
            or V120_COMMIT not in parsed.path
            or not parsed.path.endswith("/a0001")):
        raise ValueError("V120 index prefix differs")
    bucket, prefix = parsed.netloc, parsed.path.lstrip("/")
    s3 = boto3.client("s3", region_name="eu-central-1")
    raw = s3.get_object(Bucket=bucket, Key=prefix + "/terminal.json")["Body"].read()
    if hashlib.sha256(raw).hexdigest() != terminal_sha256:
        raise ValueError("V120 terminal SHA-256 differs")
    artifacts = validate_terminal(json.loads(raw))
    for name, expected in artifacts.items():
        destination = Path("sq8.bin") if name == "built/sq8.bin" else Path(name)
        destination.parent.mkdir(parents=True, exist_ok=True)
        if destination.exists():
            raise ValueError(f"V120 destination already exists: {destination}")
        pending = destination.with_name(destination.name + ".pending")
        s3.download_file(bucket, prefix + "/artifacts/" + name, str(pending))
        if (pending.stat().st_size != expected["bytes"]
                or _sha256_file(pending) != expected["sha256"]):
            raise ValueError(f"V120 artifact SHA-256 or size differs: {name}")
        pending.rename(destination)
        print(json.dumps({"artifact": name, **expected}, sort_keys=True), flush=True)
    verify_bindings(output_root)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--index-prefix", required=True)
    parser.add_argument("--terminal-sha256", required=True)
    parser.add_argument("--output-root", required=True, type=Path)
    args = parser.parse_args()
    download_index(args.index_prefix, args.terminal_sha256, args.output_root)


if __name__ == "__main__":
    main()
