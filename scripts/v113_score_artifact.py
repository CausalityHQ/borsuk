"""Generation-bound, hash-checked V113 resident score screen artifact."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

import numpy as np

from scripts.v113_100k_score_screen import BuiltArrays
from scripts.v113_resident_score import Corrections

SCHEMA = "borsuk-v113-resident-score-artifact-v1"
ARRAYS = (
    "ids", "low", "span_step", "sq8_codes", "sq8_norm",
    "pq_books", "pq_codes", "residual_books", "residual_codes",
    "nu_codes", "alpha_codes", "residual_maximum_error",
)


def _canonical(record: dict[str, object]) -> bytes:
    return (json.dumps(record, sort_keys=True, separators=(",", ":")) + "\n").encode()


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while block := handle.read(1024 * 1024):
            digest.update(block)
    return digest.hexdigest()


def _valid_hash(value: object) -> bool:
    return (
        type(value) is str and len(value) == 64
        and all(character in "0123456789abcdef" for character in value)
    )


def _array_fields(built: BuiltArrays) -> dict[str, np.ndarray]:
    return {
        "ids": built.ids,
        "low": built.low,
        "span_step": built.span_step,
        "sq8_codes": built.sq8_codes,
        "sq8_norm": built.sq8_norm,
        "pq_books": built.pq_books,
        "pq_codes": built.pq_codes,
        "residual_books": built.residual_books,
        "residual_codes": built.residual_codes,
        "nu_codes": built.corrections.nu_codes,
        "alpha_codes": built.corrections.alpha_codes,
        "residual_maximum_error": built.residual_maximum_error,
    }


def write_artifact(
    root: Path, built: BuiltArrays, *, source_sha256: str,
    pq_seed: int, residual_seed: int, sample_rows: int, iterations: int,
) -> None:
    """Write source-only planes before a worker can read query rows."""
    if not _valid_hash(source_sha256) or sample_rows < 256 or iterations < 1:
        raise ValueError("V113 artifact source or training declaration differs")
    root.mkdir(parents=True, exist_ok=False)
    files: dict[str, dict[str, object]] = {}
    for name, values in _array_fields(built).items():
        path = root / f"{name}.npy"
        np.save(path, values, allow_pickle=False)
        files[name] = {
            "bytes": path.stat().st_size,
            "sha256": _sha256_file(path),
            "dtype": str(values.dtype),
            "shape": list(values.shape),
        }
    record: dict[str, object] = {
        "schema": SCHEMA,
        "source_sha256": source_sha256,
        "rows": int(built.ids.size),
        "dimensions": int(built.low.size),
        "pq_seed": pq_seed,
        "residual_seed": residual_seed,
        "sample_rows": sample_rows,
        "iterations": iterations,
        "nu_scale": built.corrections.nu_scale,
        "alpha_scale": built.corrections.alpha_scale,
        "residual_cast_error_max": built.corrections.residual_cast_error_max,
        "partition": "balanced-floor-boundaries-pad-each-block",
        "alpha_clip": [-1.0, 1.0],
        "files": files,
    }
    (root / "seal.json").write_bytes(_canonical(record))


def read_artifact(
    root: Path, *, expected_source_sha256: str,
    expected_sample_rows: int = 100_000, expected_iterations: int = 10,
) -> BuiltArrays:
    """Reject any codebook, code plane, scale or source identity mismatch."""
    if not _valid_hash(expected_source_sha256):
        raise ValueError("V113 expected source identity differs")
    body = (root / "seal.json").read_bytes()
    try:
        seal = json.loads(body)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ValueError("V113 artifact seal differs") from error
    if (
        type(seal) is not dict or body != _canonical(seal)
        or seal.get("schema") != SCHEMA
        or seal.get("source_sha256") != expected_source_sha256
        or seal.get("partition") != "balanced-floor-boundaries-pad-each-block"
        or seal.get("alpha_clip") != [-1.0, 1.0]
        or seal.get("pq_seed") != 7301
        or seal.get("residual_seed") != 113031
        or seal.get("sample_rows") != expected_sample_rows
        or seal.get("iterations") != expected_iterations
        or type(seal.get("rows")) is not int or seal["rows"] < 256
        or type(seal.get("dimensions")) is not int or seal["dimensions"] < 64
        or type(seal.get("files")) is not dict
        or set(seal["files"]) != set(ARRAYS)
    ):
        raise ValueError("V113 artifact authority differs")
    arrays: dict[str, np.ndarray] = {}
    for name in ARRAYS:
        path = root / f"{name}.npy"
        record = seal["files"][name]
        if (
            type(record) is not dict
            or set(record) != {"bytes", "sha256", "dtype", "shape"}
            or not _valid_hash(record["sha256"])
            or path.stat().st_size != record["bytes"]
            or _sha256_file(path) != record["sha256"]
        ):
            raise ValueError(f"V113 {name} identity differs")
        values = np.load(path, mmap_mode="r", allow_pickle=False)
        if str(values.dtype) != record["dtype"] or list(values.shape) != record["shape"]:
            raise ValueError(f"V113 {name} shape differs")
        arrays[name] = values
    rows = seal["rows"]
    dimensions = seal["dimensions"]
    expected_shapes = {
        "ids": (rows,), "low": (dimensions,), "span_step": (dimensions,),
        "sq8_codes": (rows, dimensions), "sq8_norm": (rows,),
        "pq_books": (64, 256, (dimensions + 63) // 64),
        "pq_codes": (rows, 64),
        "residual_books": (12, 256, (dimensions + 11) // 12),
        "residual_codes": (rows, 12),
        "nu_codes": (rows,), "alpha_codes": (rows,),
        "residual_maximum_error": (12, 256),
    }
    expected_dtypes = {
        "ids": np.int64, "low": np.float32, "span_step": np.float32,
        "sq8_codes": np.uint8, "sq8_norm": np.float32,
        "pq_books": np.float32, "pq_codes": np.uint8,
        "residual_books": np.float32, "residual_codes": np.uint8,
        "nu_codes": np.int16, "alpha_codes": np.int16,
        "residual_maximum_error": np.float64,
    }
    if any(
        arrays[name].shape != expected_shapes[name]
        or arrays[name].dtype != expected_dtypes[name]
        for name in ARRAYS
    ):
        raise ValueError("V113 code plane geometry differs")
    nu_scale = seal.get("nu_scale")
    alpha_scale = seal.get("alpha_scale")
    cast_error = seal.get("residual_cast_error_max")
    if (
        any(type(value) not in (int, float) or not np.isfinite(value)
            for value in (nu_scale, alpha_scale, cast_error))
        or nu_scale <= 0 or alpha_scale != 1.0 / 32767.0
        or cast_error < 0
        or not np.isfinite(arrays["residual_maximum_error"]).all()
        or (arrays["residual_maximum_error"] < 0).any()
    ):
        raise ValueError("V113 score bound differs")
    corrections = Corrections(
        nu_scale, alpha_scale, arrays["nu_codes"], arrays["alpha_codes"],
        np.empty((0, 0), dtype=np.float32), cast_error,
    )
    return BuiltArrays(
        arrays["ids"], arrays["low"], arrays["span_step"],
        arrays["sq8_codes"], arrays["sq8_norm"], arrays["pq_books"],
        arrays["pq_codes"], arrays["residual_books"],
        arrays["residual_codes"], corrections,
        arrays["residual_maximum_error"],
    )
