"""Source-only page digests for one authenticated S3 SQ8 range wave."""

from __future__ import annotations

import hashlib
import json
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while block := handle.read(1024 * 1024):
            digest.update(block)
    return digest.hexdigest()


def _hex64(value: object) -> bool:
    return type(value) is str and len(value) == 64 and all(
        character in "0123456789abcdef" for character in value
    )


@dataclass(frozen=True)
class PageAuthority:
    generation: int
    rows: int
    dimensions: int
    page_rows: int
    object_sha256: str
    page_digest_sha256: str
    digests: tuple[bytes, ...]

    @property
    def row_bytes(self) -> int:
        return self.dimensions + 12

    @property
    def object_bytes(self) -> int:
        return self.rows * self.row_bytes

    @property
    def page_count(self) -> int:
        return (self.rows + self.page_rows - 1) // self.page_rows


def build_page_authority(
    object_path: Path, authority_dir: Path, *,
    expected_object_sha256: str, rows: int, dimensions: int,
    page_rows: int, generation: int,
) -> None:
    """Seal exact SQ8 page digests without accepting query or GT inputs."""
    if (
        not _hex64(expected_object_sha256)
        or rows <= 0 or dimensions <= 0 or page_rows <= 0 or generation <= 0
        or authority_dir.exists()
        or object_path.stat().st_size != rows * (dimensions + 12)
    ):
        raise ValueError("SQ8 page authority geometry differs")
    page_bytes = page_rows * (dimensions + 12)
    object_digest = hashlib.sha256()
    sidecar_digest = hashlib.sha256()
    with tempfile.TemporaryDirectory(prefix=authority_dir.name + ".tmp-",
                                     dir=authority_dir.parent) as temporary:
        pending = Path(temporary)
        with object_path.open("rb") as source, (pending / "pages.sha256").open("wb") as sidecar:
            for _ in range((rows + page_rows - 1) // page_rows):
                page = source.read(page_bytes)
                if not page:
                    raise ValueError("SQ8 page authority object is short")
                object_digest.update(page)
                digest = hashlib.sha256(page).digest()
                sidecar.write(digest)
                sidecar_digest.update(digest)
            if source.read(1) or object_digest.hexdigest() != expected_object_sha256:
                raise ValueError("SQ8 page authority object SHA-256 differs")
        manifest = {
            "schema": "borsuk-v115-sq8-page-authority-v2",
            "generation": generation,
            "rows": rows,
            "dimensions": dimensions,
            "page_rows": page_rows,
            "object_sha256": expected_object_sha256,
            "page_digest_sha256": sidecar_digest.hexdigest(),
        }
        (pending / "manifest.json").write_text(
            json.dumps(manifest, sort_keys=True, separators=(",", ":")) + "\n"
        )
        pending.rename(authority_dir)


def load_page_authority(authority_dir: Path) -> PageAuthority:
    """Check internal geometry/digests; caller authenticates manifest authority."""
    manifest = json.loads((authority_dir / "manifest.json").read_text())
    if (
        type(manifest) is not dict
        or set(manifest) != {"schema", "generation", "rows", "dimensions", "page_rows",
                                "object_sha256", "page_digest_sha256"}
        or manifest["schema"] != "borsuk-v115-sq8-page-authority-v2"
        or any(type(manifest[key]) is not int or manifest[key] <= 0
               for key in ("generation", "rows", "dimensions", "page_rows"))
        or not _hex64(manifest["object_sha256"])
        or not _hex64(manifest["page_digest_sha256"])
    ):
        raise ValueError("SQ8 page authority manifest differs")
    sidecar = (authority_dir / "pages.sha256").read_bytes()
    page_count = (manifest["rows"] + manifest["page_rows"] - 1) // manifest["page_rows"]
    if (len(sidecar) != page_count * 32
            or hashlib.sha256(sidecar).hexdigest() != manifest["page_digest_sha256"]):
        raise ValueError("SQ8 page authority sidecar differs")
    return PageAuthority(
        manifest["generation"], manifest["rows"], manifest["dimensions"],
        manifest["page_rows"], manifest["object_sha256"],
        manifest["page_digest_sha256"],
        tuple(sidecar[index:index + 32] for index in range(0, len(sidecar), 32)),
    )


class S3RangeClient(Protocol):
    def get_object(self, *, Bucket: str, Key: str, Range: str, IfMatch: str) -> dict[str, Any]: ...


def read_verified_pages(
    client: S3RangeClient, authority: PageAuthority, *,
    bucket: str, key: str, first_page: int, last_page: int, etag: str,
) -> bytes:
    """Read one exact page interval and authenticate every returned page."""
    if (
        not bucket or not key or not etag
        or type(first_page) is not int or type(last_page) is not int
        or first_page < 0 or first_page > last_page or last_page >= authority.page_count
    ):
        raise ValueError("SQ8 page interval differs")
    start = first_page * authority.page_rows * authority.row_bytes
    stop = min((last_page + 1) * authority.page_rows, authority.rows) * authority.row_bytes
    response = client.get_object(
        Bucket=bucket, Key=key, Range=f"bytes={start}-{stop - 1}", IfMatch=etag,
    )
    body = response["Body"]
    try:
        if (
            response.get("ResponseMetadata", {}).get("HTTPStatusCode") != 206
            or response.get("ContentRange") != f"bytes {start}-{stop - 1}/{authority.object_bytes}"
            or response.get("ContentLength") != stop - start
            or response.get("ETag") != etag
        ):
            raise ValueError("SQ8 S3 range response differs")
        payload = body.read(stop - start + 1)
    finally:
        body.close()
    if len(payload) != stop - start:
        raise ValueError("SQ8 S3 range length differs")
    for page in range(first_page, last_page + 1):
        page_start = page * authority.page_rows * authority.row_bytes - start
        page_stop = min((page + 1) * authority.page_rows, authority.rows) * authority.row_bytes - start
        if hashlib.sha256(payload[page_start:page_stop]).digest() != authority.digests[page]:
            raise ValueError(f"SQ8 S3 page digest differs at {page}")
    return payload
