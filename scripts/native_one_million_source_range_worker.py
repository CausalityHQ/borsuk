#!/usr/bin/env python3
"""Create-only Spot worker for the 1M source-score final-range diagnostic."""

from __future__ import annotations

from scripts.native_one_million_data_range_worker import _once, data_range_worker_script
from scripts.native_one_million_page_oracle_worker import _download
from scripts.native_one_million_selector_cell import SOURCE_IDENTITIES


def source_range_worker_script(plan: object) -> str:
    """Keep the sealed phase and resource broker; score authenticated source rows."""
    script = data_range_worker_script(plan)
    script = _once(script, "/mnt/native-one-million-data-range", "/mnt/native-one-million-source-range")
    script = _once(
        script, "borsuk-one-million-opq8-data-range-terminal-v1",
        "borsuk-one-million-source-range-terminal-v1",
    )
    source = SOURCE_IDENTITIES["source"]
    script = _once(
        script, "phase=construct\n",
        _download(source.uri, source.sha256, source.bytes, "source.parquet") + "\nphase=construct\n",
    )
    for phase in ("construct", "plan", "evaluate"):
        script = _once(
            script,
            f"scripts.native_one_million_data_range_cell {phase}",
            f"scripts.native_one_million_source_range_cell {phase}",
        )
    script = _once(
        script, "chmod 0444 generation.json base.arrow delta.arrow prior-*.json codes.bin",
        "chmod 0444 source.parquet generation.json base.arrow delta.arrow prior-*.json codes.bin",
    )
    for old, new in (
        ("range-plans.json", "source-range-plans.json"),
        ("range-plan-seal.json", "source-range-plan-seal.json"),
        ("range-evidence.json", "source-range-evidence.json"),
        ("range-result.json", "source-range-result.json"),
        ("range-validation.json", "source-range-validation.json"),
    ):
        if old not in script:
            raise ValueError(f"source-range worker template anchor differs: {old}")
        script = script.replace(old, new)
    script = _once(
        script, "scripts.validate_native_one_million_data_range_cell",
        "scripts.validate_native_one_million_source_range_cell",
    )
    if len(script.encode()) > 16_384:
        raise ValueError("source-range Spot worker exceeds EC2 user-data limit")
    return script
