"""Source-only sectioned PQ64 router artifact for measured runtime serving."""

from __future__ import annotations

import argparse
import hashlib
import json
import tempfile
from pathlib import Path

import numpy as np

from scripts.v115_page_authority import _hex64, _sha256_file
from scripts.v77_export_manifest import block_means, lloyd

_SECTIONS = ("summaries", "books", "codes", "low", "step")


def _shapes(rows: int, dimensions: int, page_rows: int,
            blocks_per_page: int) -> dict[str, tuple[int, ...]]:
    pages = (rows + page_rows - 1) // page_rows
    return {
        "summaries": (pages * blocks_per_page, dimensions),
        "books": (64, 256, (dimensions + 63) // 64),
        "codes": (rows, 64),
        "low": (dimensions,),
        "step": (dimensions,),
    }


def write_source_router(
    output_dir: Path, *, rows: int, dimensions: int,
    page_rows: int, blocks_per_page: int,
    source_sha256: str, layout_sha256: str, sq8_sha256: str,
    generation: int, summaries: np.ndarray, books: np.ndarray,
    codes: np.ndarray, low: np.ndarray, step: np.ndarray,
) -> dict[str, object]:
    """Seal only source-trained routing planes; accepts no query or GT data."""
    if (
        rows <= 0 or dimensions <= 0 or page_rows <= 0
        or blocks_per_page <= 0 or generation <= 0
        or output_dir.exists()
        or any(not _hex64(value) for value in
               (source_sha256, layout_sha256, sq8_sha256))
    ):
        raise ValueError("source router geometry or identity differs")
    shapes = _shapes(rows, dimensions, page_rows, blocks_per_page)
    planes = {"summaries": summaries, "books": books, "codes": codes,
              "low": low, "step": step}
    for name, value in planes.items():
        expected_dtype = np.uint8 if name == "codes" else np.float32
        if (not isinstance(value, np.ndarray) or value.dtype != expected_dtype
                or value.shape != shapes[name]
                or (name != "codes" and not np.isfinite(value).all())):
            raise ValueError(f"source router {name} differs")
    if (step <= 0).any():
        raise ValueError("source router step differs")
    with tempfile.TemporaryDirectory(prefix=output_dir.name + ".tmp-",
                                     dir=output_dir.parent) as temporary:
        pending = Path(temporary)
        sections = {}
        for name in _SECTIONS:
            value = planes[name]
            raw = np.ascontiguousarray(value, dtype="u1" if name == "codes" else "<f4").tobytes()
            (pending / f"{name}.bin").write_bytes(raw)
            sections[name] = {"bytes": len(raw), "sha256": hashlib.sha256(raw).hexdigest()}
        manifest: dict[str, object] = {
            "schema": "borsuk-v115-source-router-v1",
            "generation": generation,
            "source_sha256": source_sha256,
            "layout_sha256": layout_sha256,
            "sq8_sha256": sq8_sha256,
            "geometry": {"rows": rows, "dimensions": dimensions,
                         "page_rows": page_rows, "blocks_per_page": blocks_per_page,
                         "subspaces": 64, "pq_width": shapes["books"][2]},
            "sections": sections,
        }
        (pending / "manifest.json").write_text(
            json.dumps(manifest, sort_keys=True, separators=(",", ":")) + "\n"
        )
        pending.rename(output_dir)
    return manifest


def load_source_router(output_dir: Path) -> tuple[dict[str, object], dict[str, np.ndarray]]:
    """Check section digests; caller authenticates the generation manifest."""
    manifest = json.loads((output_dir / "manifest.json").read_text())
    if (
        type(manifest) is not dict
        or set(manifest) != {"schema", "generation", "source_sha256", "layout_sha256",
                                 "sq8_sha256", "geometry", "sections"}
        or manifest["schema"] != "borsuk-v115-source-router-v1"
        or type(manifest["generation"]) is not int or manifest["generation"] <= 0
        or any(not _hex64(manifest[key]) for key in
               ("source_sha256", "layout_sha256", "sq8_sha256"))
        or type(manifest["geometry"]) is not dict
        or set(manifest["geometry"]) != {"rows", "dimensions", "page_rows",
                                         "blocks_per_page", "subspaces", "pq_width"}
        or any(type(manifest["geometry"][key]) is not int or manifest["geometry"][key] <= 0
               for key in manifest["geometry"])
    ):
        raise ValueError("source router manifest differs")
    geometry = manifest["geometry"]
    if (geometry["subspaces"] != 64
            or geometry["pq_width"] != (geometry["dimensions"] + 63) // 64
            or type(manifest["sections"]) is not dict
            or set(manifest["sections"]) != set(_SECTIONS)):
        raise ValueError("source router geometry differs")
    shapes = _shapes(geometry["rows"], geometry["dimensions"],
                     geometry["page_rows"], geometry["blocks_per_page"])
    arrays = {}
    for name in _SECTIONS:
        section = manifest["sections"][name]
        dtype = np.dtype("u1" if name == "codes" else "<f4")
        expected_bytes = int(np.prod(shapes[name])) * dtype.itemsize
        path = output_dir / f"{name}.bin"
        if (
            type(section) is not dict or set(section) != {"bytes", "sha256"}
            or section["bytes"] != expected_bytes or not _hex64(section["sha256"])
            or path.stat().st_size != expected_bytes
            or _sha256_file(path) != section["sha256"]
        ):
            raise ValueError(f"source router {name} section differs")
        arrays[name] = np.fromfile(path, dtype=dtype).reshape(shapes[name])
    if not (np.isfinite(arrays["summaries"]).all()
            and np.isfinite(arrays["books"]).all()
            and np.isfinite(arrays["low"]).all()
            and np.isfinite(arrays["step"]).all()
            and (arrays["step"] > 0).all()):
        raise ValueError("source router numerical plane differs")
    return manifest, arrays


def _train_books(data: np.ndarray, subspaces: int, *, seed: int,
                 sample_rows: int = 100_000) -> np.ndarray:
    """Apply V77's source-only sample and ten-iteration Lloyd rule."""
    rows, dimensions = data.shape
    width = (dimensions + subspaces - 1) // subspaces
    generator = np.random.default_rng(seed)
    selected = data[generator.choice(rows, min(rows, sample_rows), replace=False)]
    sample = np.zeros((len(selected), width * subspaces), dtype=np.float32)
    sample[:, :dimensions] = selected
    books = np.empty((subspaces, 256, width), dtype=np.float32)
    for subspace in range(subspaces):
        first = subspace * width
        trained = lloyd(np.ascontiguousarray(sample[:, first:first + width]),
                        256, 10, seed + subspace)
        books[subspace, :trained.shape[0]] = trained
        if trained.shape[0] < 256:
            books[subspace, trained.shape[0]:] = trained[-1]
    return books


def _encode(data: np.ndarray, books: np.ndarray) -> np.ndarray:
    rows, dimensions = data.shape
    subspaces, _, width = books.shape
    padded = np.zeros((rows, subspaces * width), dtype=np.float32)
    padded[:, :dimensions] = data
    codes = np.empty((rows, subspaces), dtype=np.uint8)
    for subspace in range(subspaces):
        first = subspace * width
        book = books[subspace]
        norms = np.einsum("ij,ij->i", book, book)
        for start in range(0, rows, 16_384):
            stop = min(start + 16_384, rows)
            block = padded[start:stop, first:first + width]
            codes[start:stop, subspace] = np.argmin(
                norms[None, :] - 2.0 * (block @ book.T), axis=1,
            )
    return codes


def _decode(codes: np.ndarray, books: np.ndarray, dimensions: int) -> np.ndarray:
    subspaces, _, width = books.shape
    reconstructed = np.empty((codes.shape[0], subspaces * width), np.float32)
    for subspace in range(subspaces):
        first = subspace * width
        reconstructed[:, first:first + width] = books[subspace][codes[:, subspace]]
    return reconstructed[:, :dimensions].copy()


def build_source_router(
    source: Path, layout: Path, output_dir: Path, *,
    expected_source_sha256: str, expected_layout_sha256: str,
    sq8_sha256: str, rows: int, dimensions: int,
    page_rows: int, blocks_per_page: int, generation: int,
) -> dict[str, object]:
    """Fit and seal a PQ64 router from corpus rows and layout alone."""
    import pyarrow as pa
    import pyarrow.parquet as pq

    if (
        not _hex64(expected_source_sha256)
        or not _hex64(expected_layout_sha256)
        or not _hex64(sq8_sha256)
        or rows <= 0 or dimensions <= 0 or page_rows <= 0
        or blocks_per_page <= 0 or page_rows % blocks_per_page
        or _sha256_file(source) != expected_source_sha256
        or _sha256_file(layout) != expected_layout_sha256
    ):
        raise ValueError("source router input identity or geometry differs")
    table = pq.read_table(source, columns=["feature_row_id", "embedding"])
    embedding_type = table.schema.field("embedding").type
    if (
        table.num_rows != rows
        or not pa.types.is_integer(table.schema.field("feature_row_id").type)
        or not pa.types.is_fixed_size_list(embedding_type)
        or embedding_type.list_size != dimensions
        or embedding_type.value_type != pa.float32()
    ):
        raise ValueError("source router Parquet geometry differs")
    ids = table["feature_row_id"].combine_chunks().to_numpy(zero_copy_only=False)
    if np.unique(ids).size != rows:
        raise ValueError("source router row IDs differ")
    values = table["embedding"].combine_chunks().values.to_numpy(zero_copy_only=False)
    vectors = np.asarray(values, np.float32).reshape(rows, dimensions)
    if not np.isfinite(vectors).all():
        raise ValueError("source router contains a nonfinite vector")
    order = np.asarray(np.load(layout, allow_pickle=False), dtype=np.int64)
    if (order.shape != (rows,)
            or not np.array_equal(np.sort(order), np.arange(rows, dtype=np.int64))):
        raise ValueError("source router layout is not a row permutation")
    ordered = np.ascontiguousarray(vectors[order])
    low = ordered.min(axis=0).astype(np.float32)
    span = np.maximum(ordered.max(axis=0) - low, 1e-12).astype(np.float32)
    step = np.asarray(span / 255.0, np.float32)
    summary_subspaces = min(192, dimensions)
    summary_books = _train_books(ordered, summary_subspaces, seed=6801)
    means = block_means(ordered, page_rows // blocks_per_page)
    summaries = _decode(_encode(means, summary_books), summary_books, dimensions)
    wanted = ((rows + page_rows - 1) // page_rows) * blocks_per_page
    if summaries.shape[0] < wanted:
        summaries = np.concatenate(
            [summaries, np.repeat(summaries[-1:], wanted - summaries.shape[0], axis=0)],
            axis=0,
        )
    if summaries.shape != (wanted, dimensions):
        raise ValueError("source router summary padding differs")
    books = _train_books(ordered, 64, seed=7301)
    codes = _encode(ordered, books)
    return write_source_router(
        output_dir, rows=rows, dimensions=dimensions, page_rows=page_rows,
        blocks_per_page=blocks_per_page, source_sha256=expected_source_sha256,
        layout_sha256=expected_layout_sha256, sq8_sha256=sq8_sha256,
        generation=generation, summaries=summaries, books=books,
        codes=codes, low=low, step=step,
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", required=True, type=Path)
    parser.add_argument("--layout", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--source-sha256", required=True)
    parser.add_argument("--layout-sha256", required=True)
    parser.add_argument("--sq8-sha256", required=True)
    parser.add_argument("--rows", required=True, type=int)
    parser.add_argument("--dimensions", required=True, type=int)
    parser.add_argument("--page-rows", required=True, type=int)
    parser.add_argument("--blocks-per-page", required=True, type=int)
    parser.add_argument("--generation", required=True, type=int)
    args = parser.parse_args()
    build_source_router(
        args.source, args.layout, args.output,
        expected_source_sha256=args.source_sha256,
        expected_layout_sha256=args.layout_sha256,
        sq8_sha256=args.sq8_sha256,
        rows=args.rows, dimensions=args.dimensions,
        page_rows=args.page_rows, blocks_per_page=args.blocks_per_page,
        generation=args.generation,
    )


if __name__ == "__main__":
    main()
