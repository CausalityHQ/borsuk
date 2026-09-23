"""Create-only Spot worker for the one preregistered PQ96 rescue."""

from __future__ import annotations

import json
import re

from scripts.launch_native_geometric_layout_spot import SpotLayoutPlan
from scripts.native_hundred_thousand_sign96_worker import (
    ARTIFACT_FILES as SIGN_ARTIFACT_FILES,
)
from scripts.native_hundred_thousand_sign96_worker import sign96_worker_script

ARTIFACT_FILES = {
    role.replace("sign-mean", "pq-model").replace("sign-", "pq-"): path.replace("sign96", "pq96").replace(
        "pq96/mean.bin", "pq96/model.bin",
    )
    for role, path in SIGN_ARTIFACT_FILES.items()
}


def pq96_worker_script(plan: SpotLayoutPlan) -> str:
    """Instantiate the reviewed phase engine with the PQ96 codec and roster."""
    script = sign96_worker_script(plan)
    script = script.replace("sign96", "pq96")
    script = script.replace("pq96/mean.bin", "pq96/model.bin")
    script = script.replace('"sign-mean"', '"pq-mean"')
    script = script.replace('"sign-groups"', '"pq-groups"')
    script = script.replace('"sign-seal"', '"pq-seal"')
    script = script.replace('"pq-mean"', '"pq-model"')
    embedded = re.search(r"^files=(\{.*\})$", script, re.MULTILINE)
    if (
        embedded is None or json.loads(embedded.group(1)) != ARTIFACT_FILES
        or "sign96" in script or "sign-mean" in script
        or len(script.encode()) > 16_384
        or "-m scripts.native_hundred_thousand_pq96_cell construct" not in script
        or "-m scripts.validate_native_hundred_thousand_pq96_cell" not in script
    ):
        raise ValueError("PQ96 worker substitution differs")
    return script
