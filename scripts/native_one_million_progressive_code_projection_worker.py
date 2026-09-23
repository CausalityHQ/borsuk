"""Create-only Spot worker for the 1M page-local progressive code-wave projection."""

from __future__ import annotations

from scripts.native_one_million_page_oracle_worker import (
    _download,
    page_oracle_worker_script,
)
from scripts.native_one_million_progressive_code_projection_cell import (
    PRIOR_PLAN_SEAL_SHA256,
    PRIOR_PLAN_SHA256,
    PRIOR_PREFIX,
    PRIOR_RANGE_SEAL_SHA256,
    PRIOR_TERMINAL_SHA256,
)


def _once(script: str, old: str, new: str) -> str:
    if script.count(old) != 1:
        raise ValueError(f"code-wave worker template anchor differs: {old[:64]}")
    return script.replace(old, new, 1)


def progressive_code_wave_worker_script(plan: object) -> str:
    """Reuse the sealed source map, then separate query-plan and truth phases."""
    script = page_oracle_worker_script(plan)
    script = _once(script, "/mnt/native-one-million-page-oracle", "/mnt/native-progressive-code-wave")
    script = _once(
        script, "borsuk-one-million-opq8-page-oracle-terminal-v1",
        "borsuk-one-million-progressive-code-wave-terminal-v1",
    )
    downloads = "\n".join(
        _download(PRIOR_PREFIX + source, digest, size, local)
        for source, digest, size, local in (
            ("terminal.json", PRIOR_TERMINAL_SHA256, 5580, "prior-range-terminal.json"),
            ("artifacts/range-plans.json", PRIOR_PLAN_SHA256, 40009368,
             "prior-range-plans.json"),
            ("artifacts/range-plan-seal.json", PRIOR_PLAN_SEAL_SHA256, 501,
             "prior-range-plan-seal.json"),
            ("artifacts/range-seal.json", PRIOR_RANGE_SEAL_SHA256, 682,
             "prior-range-seal.json"),
        )
    )
    script = _once(script, "phase=construct\n", downloads + "\nphase=construct\n")
    plan_phase = """phase=plan
mkdir planning && chown nobody:nobody planning
run_capped /usr/bin/time -v -o plan-resources.txt timeout --foreground @WALL@ unshare --net --fork setpriv --reuid=nobody --regid=nobody --clear-groups env -i PATH="$PATH" PYTHONPATH="$root/repo" OPENBLAS_NUM_THREADS=1 OMP_NUM_THREADS=1 "$root/.venv/bin/python" -m scripts.native_one_million_progressive_code_projection_cell plan --root "$root" --out "$root/planning"
mv planning/progressive-code-wave-plans.json progressive-code-wave-plans.json
mv planning/progressive-code-wave-plan-seal.json progressive-code-wave-plan-seal.json
rmdir planning
check_resources plan-resources.txt
chmod 0444 progressive-code-wave-plans.json progressive-code-wave-plan-seal.json
for name in progressive-code-wave-plans.json progressive-code-wave-plan-seal.json plan-resources.txt; do publish_artifact "$name"; done
""".replace("@WALL@", str(plan.wall_seconds))
    script = _once(
        script,
        "for name in page-map.bin page-groups.bin page-bytes.bin seal.json construct-resources.txt; do publish_artifact \"$name\"; done\nphase=evaluate\n",
        "for name in page-map.bin page-groups.bin page-bytes.bin seal.json construct-resources.txt; do publish_artifact \"$name\"; done\n"
        + plan_phase + "phase=evaluate\n",
    )
    script = _once(
        script,
        "scripts.native_one_million_page_oracle_cell evaluate",
        "scripts.native_one_million_progressive_code_projection_cell evaluate",
    )
    for old, new in (
        ("mv evaluation/evidence.json evidence.json", "mv evaluation/progressive-code-wave-evidence.json progressive-code-wave-evidence.json"),
        ("mv evaluation/result.json result.json", "mv evaluation/progressive-code-wave-result.json progressive-code-wave-result.json"),
        ("for name in evidence.json result.json evaluate-resources.txt; do publish_artifact \"$name\"; done",
         "for name in progressive-code-wave-evidence.json progressive-code-wave-result.json evaluate-resources.txt; do publish_artifact \"$name\"; done"),
        ("scripts.validate_native_one_million_page_oracle",
         "scripts.validate_native_one_million_progressive_code_projection"),
        ("for name in validation.json validate-resources.txt; do publish_artifact \"$name\"; done",
         "for name in progressive-code-wave-validation.json validate-resources.txt; do publish_artifact \"$name\"; done"),
        ("construct-resources.txt evaluate-resources.txt validate-resources.txt; do",
         "construct-resources.txt plan-resources.txt evaluate-resources.txt validate-resources.txt; do"),
    ):
        script = _once(script, old, new)
    if (
        len(script.encode()) > 16_384
        or "scripts.native_one_million_progressive_code_projection_cell plan" not in script
        or "phase=plan\n" not in script
        or "phase=evaluate\n" not in script
    ):
        raise ValueError("code-wave Spot worker differs")
    return script
