"""Bounded Spot worker for PQ192 code-only returned-rank G0c."""

from __future__ import annotations

from scripts.launch_native_geometric_layout_spot import SpotLayoutPlan
from scripts.native_two_bit_returned_spot_worker import (
    ARTIFACT_FILES as COMMON_ARTIFACT_FILES,
)
from scripts.native_two_bit_returned_spot_worker import returned_worker_script

ARTIFACT_FILES = {
    **COMMON_ARTIFACT_FILES,
    "pq-books": "evaluation/pq-books.npy",
    "pq-codes": "evaluation/pq-codes.npy",
}


def pq_worker_script(plan: SpotLayoutPlan) -> str:
    return returned_worker_script(
        plan,
        evaluation_module="scripts.native_pq192_returned",
        validation_module="scripts.validate_native_pq192_returned",
        artifact_files=ARTIFACT_FILES,
        terminal_schema="borsuk-pq192-returned-terminal-v1",
        advance_decision="advance-pq-fidelity-only",
        stop_decision="stop-pq192-sole-scorer",
        extra_evaluation_files=("evaluation/pq-books.npy", "evaluation/pq-codes.npy"),
    )
