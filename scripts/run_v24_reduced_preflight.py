#!/usr/bin/env python3
"""Run the fixed V24 reduced determinism preflight in separate processes."""

from __future__ import annotations

import argparse
import dataclasses
import hashlib
import json
import pathlib
import shutil
import subprocess
import sys
from collections.abc import Sequence

from scripts import build_v24_reduced_fixture as fixture_builder
from scripts.run_v24_witness_page_router import (
    MonitorLimits,
    cleanup_known_files,
    monitor_process_group,
    offline_environment,
)

_GENERATION = "generation-v24-reduced-preflight"
_SERVING_BYTES = 1_644_167_168
_LOWER_HEX = frozenset("0123456789abcdef")


@dataclasses.dataclass(frozen=True)
class ReducedPreflightRequest:
    """Exact local authority for one reduced determinism comparison."""

    binary: pathlib.Path
    binary_sha256: str
    binary_bytes: int
    root: pathlib.Path
    source_commit: str
    source_rows: int = 65_536
    witness_count: int = 4_096
    page_count: int = 64
    worker_counts: tuple[int, ...] = (1, 4)


def canonical_json_bytes(value: object) -> bytes:
    """Return recursively key-sorted compact JSON with one trailing newline."""

    return (
        json.dumps(value, allow_nan=False, separators=(",", ":"), sort_keys=True).encode()
        + b"\n"
    )


def sha256_file(path: pathlib.Path) -> str:
    """Hash one regular file without loading it into memory."""

    digest = hashlib.sha256()
    with path.open("rb") as stream:
        while chunk := stream.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def _validate_request(request: ReducedPreflightRequest) -> None:
    if (
        not request.binary.is_absolute()
        or request.binary.is_symlink()
        or not request.binary.is_file()
        or request.binary.stat().st_size != request.binary_bytes
        or len(request.binary_sha256) != 64
        or any(character not in _LOWER_HEX for character in request.binary_sha256)
        or sha256_file(request.binary) != request.binary_sha256
        or not request.root.is_absolute()
        or request.root.is_symlink()
        or not request.root.is_dir()
        or any(request.root.iterdir())
        or len(request.source_commit) != 40
        or any(character not in _LOWER_HEX for character in request.source_commit)
        or request.source_rows < 257
        or not 2 <= request.witness_count <= request.source_rows
        or request.page_count < 8
        or request.worker_counts != (1, 4)
    ):
        raise ValueError("V24 reduced preflight authority differs")


def _run_phase(
    *,
    binary: pathlib.Path,
    workers: int,
    phase: str,
    manifest: pathlib.Path,
    input_dir: pathlib.Path,
    output_dir: pathlib.Path,
    phase_flag: str,
) -> bytes:
    output_dir.mkdir(mode=0o700)
    scratch = output_dir.parent / f"{output_dir.name}-scratch"
    scratch.mkdir(mode=0o700)
    stdout_path = output_dir.parent / f"{output_dir.name}.stdout"
    stderr_path = output_dir.parent / f"{output_dir.name}.stderr"
    command = [
        str(binary),
        "--manifest",
        str(manifest),
        "--input-dir",
        str(input_dir),
        "--output-dir",
        str(output_dir),
        phase_flag,
        "--execute",
    ]
    try:
        with stdout_path.open("xb") as stdout, stderr_path.open("xb") as stderr:
            process = subprocess.Popen(  # noqa: S603
                command,
                stdout=stdout,
                stderr=stderr,
                start_new_session=True,
                env=offline_environment(
                    scratch, {"RAYON_NUM_THREADS": str(workers)}
                ),
            )
            status, stop = monitor_process_group(
                process.pid,
                MonitorLimits.for_phase(phase),
                progress_path=output_dir / "progress.json",
                progress_phase={
                    "train-witnesses": "witness-training",
                    "build-postings": "posting-construction",
                    "evaluate-development": "development-evaluation",
                }[phase],
            )
            process.returncode = status
        if stop is not None or status != 0:
            diagnostic = stderr_path.read_text(encoding="utf-8", errors="replace")[-4096:]
            raise RuntimeError(
                f"V24 reduced {phase} failed: status={status} stop={stop}: {diagnostic}"
            )
        result = (output_dir / "result.json").read_bytes()
        if stdout_path.read_bytes() != result:
            raise ValueError("V24 reduced phase stdout differs")
        return result
    finally:
        cleanup_known_files(scratch, ())


def _evaluation_evidence(raw: bytes) -> str:
    value = json.loads(raw)
    if raw != canonical_json_bytes(value) or type(value) is not dict:  # noqa: E721
        raise ValueError("V24 reduced development result differs")

    def evaluation(item: dict[str, object]) -> dict[str, object]:
        return {
            key: item[key]
            for key in (
                "cell",
                "quality",
                "samples",
                "scalar_page_ordinals",
                "scalar_simd_pages_equal",
                "serving_bytes",
            )
        }

    evidence = {
        "distance_backend": value["distance_backend"],
        "evaluated_cells": [evaluation(item) for item in value["evaluated_cells"]],
        "exact_control": value["exact_control"],
        "identities": value["identities"],
        "page_body_reads": value["page_body_reads"],
        "serving": evaluation(value["serving"]),
    }
    return hashlib.sha256(canonical_json_bytes(evidence)).hexdigest()


def _run_once(request: ReducedPreflightRequest, workers: int) -> dict[str, object]:
    root = request.root / f"worker-{workers}"
    root.mkdir(mode=0o700)
    fixture_builder.build_reduced_fixture(
        root,
        source_rows=request.source_rows,
        witness_count=request.witness_count,
        page_count=request.page_count,
        query_count=32,
        generation=_GENERATION,
    )
    training_input = root / "training-input"
    training_input.mkdir(mode=0o700)
    shutil.copyfile(
        root / "construction-rows.parquet",
        training_input / "construction-rows.parquet",
    )
    training_output = root / "training-output"
    training_result = _run_phase(
        binary=request.binary,
        workers=workers,
        phase="train-witnesses",
        manifest=root / "training-manifest.json",
        input_dir=training_input,
        output_dir=training_output,
        phase_flag="--train-witnesses",
    )
    posting_manifest = fixture_builder.prepare_posting_phase(root, training_output)
    posting_output = root / "posting-output"
    posting_result = _run_phase(
        binary=request.binary,
        workers=workers,
        phase="build-postings",
        manifest=posting_manifest,
        input_dir=root / "posting-input",
        output_dir=posting_output,
        phase_flag="--build-postings",
    )
    development_manifest = fixture_builder.prepare_development_phase(
        root, training_output, posting_output
    )
    development_output = root / "development-output"
    development_result = _run_phase(
        binary=request.binary,
        workers=workers,
        phase="evaluate-development",
        manifest=development_manifest,
        input_dir=root / "development-input",
        output_dir=development_output,
        phase_flag="--evaluate-development",
    )
    deterministic_paths = {
        "construction_rows": root / "construction-rows.parquet",
        "neighbors": root / "neighbors.parquet",
        "page_rows": root / "page-rows.parquet",
        "posting_result": posting_output / "result.json",
        "queries": root / "queries.parquet",
        "training_result": training_output / "result.json",
        "witness_graph": training_output / "witness-graph.arrow",
        "witness_postings": posting_output / "witness-postings.arrow",
        "witnesses": training_output / "witnesses.arrow",
    }
    return {
        "artifact_sha256": {
            role: sha256_file(path) for role, path in sorted(deterministic_paths.items())
        },
        "development_result_sha256": hashlib.sha256(development_result).hexdigest(),
        "evaluation_evidence_sha256": _evaluation_evidence(development_result),
        "posting_result_sha256": hashlib.sha256(posting_result).hexdigest(),
        "training_result_sha256": hashlib.sha256(training_result).hexdigest(),
        "workers": workers,
    }


def run_reduced_preflight(request: ReducedPreflightRequest) -> bytes:
    """Run both registered worker counts and require deterministic evidence."""

    _validate_request(request)
    runs = [_run_once(request, workers) for workers in request.worker_counts]
    if (
        runs[0]["artifact_sha256"] != runs[1]["artifact_sha256"]
        or runs[0]["evaluation_evidence_sha256"]
        != runs[1]["evaluation_evidence_sha256"]
    ):
        raise RuntimeError("V24 reduced worker determinism differs")
    receipt = {
        "binary_bytes": request.binary_bytes,
        "binary_sha256": request.binary_sha256,
        "claim_eligible": False,
        "page_count": request.page_count,
        "runs": runs,
        "schema": "borsuk-v24-reduced-preflight-v1",
        "serving_bytes": _SERVING_BYTES,
        "source_commit": request.source_commit,
        "source_rows": request.source_rows,
        "witness_count": request.witness_count,
        "worker_counts": list(request.worker_counts),
    }
    raw = canonical_json_bytes(receipt)
    (request.root / "preflight-receipt.json").write_bytes(raw)
    return raw


def argument_parser() -> argparse.ArgumentParser:
    """Construct the fixed-shape command-line parser."""

    parser = argparse.ArgumentParser(allow_abbrev=False)
    parser.add_argument("--binary", required=True, type=pathlib.Path)
    parser.add_argument("--binary-sha256", required=True)
    parser.add_argument("--binary-bytes", required=True, type=int)
    parser.add_argument("--root", required=True, type=pathlib.Path)
    parser.add_argument("--source-commit", required=True)
    parser.add_argument("--execute-reduced-preflight", action="store_true", required=True)
    return parser


def parse_args(arguments: Sequence[str] | None = None) -> ReducedPreflightRequest:
    """Parse the fixed 65,536-row, 1-vs-4-worker preflight."""

    values = argument_parser().parse_args(arguments)
    return ReducedPreflightRequest(
        binary=values.binary,
        binary_sha256=values.binary_sha256,
        binary_bytes=values.binary_bytes,
        root=values.root,
        source_commit=values.source_commit,
    )


def main(arguments: Sequence[str] | None = None) -> int:
    """Execute once and write the canonical receipt to stdout."""

    sys.stdout.buffer.write(run_reduced_preflight(parse_args(arguments)))
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as error:  # noqa: BLE001
        print(error, file=sys.stderr)
        raise SystemExit(1) from error
