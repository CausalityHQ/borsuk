#!/usr/bin/env python3
"""Create-only Spot worker for the sealed 1M query-only data-range gate."""

from __future__ import annotations

from scripts.native_one_million_data_range_cell import (
    PRIOR_CODES,
    PRIOR_MEMBERSHIP,
    PRIOR_MODEL,
)
from scripts.native_one_million_page_oracle_cell import PRIOR_PREFIX
from scripts.native_one_million_page_oracle_worker import (
    _download,
    page_oracle_worker_script,
)
from scripts.native_one_million_selector_cell import DEVELOPMENT_IDENTITIES


def _once(body: str, old: str, new: str) -> str:
    if body.count(old) != 1:
        raise ValueError(f"data-range worker template anchor differs: {old[:60]}")
    return body.replace(old, new, 1)


def data_range_worker_script(plan: object) -> str:
    """Keep the existing resource/terminal broker; insert a sealed query phase."""
    script = page_oracle_worker_script(plan)
    script = _once(script, "/mnt/native-one-million-page-oracle", "/mnt/native-one-million-data-range")
    script = _once(
        script, "borsuk-one-million-opq8-page-oracle-terminal-v1",
        "borsuk-one-million-opq8-data-range-terminal-v1",
    )
    source_commands = "\n".join(
        _download(PRIOR_PREFIX + "artifacts/" + source, identity[0], identity[1], target)
        for source, target, identity in (
            ("codes.bin", "codes.bin", PRIOR_CODES),
            ("model.bin", "model.bin", PRIOR_MODEL),
            ("membership.bin", "prior-membership.bin", PRIOR_MEMBERSHIP),
        )
    )
    script = _once(script, "phase=construct\n", source_commands + "\nphase=construct\n")
    script = _once(
        script, "scripts.native_one_million_page_oracle_cell construct",
        "scripts.native_one_million_data_range_cell construct",
    )
    script = _once(
        script,
        "chmod 0444 generation.json base.arrow delta.arrow prior-*.json page-map.bin page-groups.bin page-bytes.bin seal.json\n"
        "for name in page-map.bin page-groups.bin page-bytes.bin seal.json construct-resources.txt; do publish_artifact \"$name\"; done",
        "chmod 0444 generation.json base.arrow delta.arrow prior-*.json codes.bin model.bin prior-membership.bin "
        "page-map.bin page-groups.bin page-bytes.bin seal.json row-pages.bin range-seal.json\n"
        "for name in page-map.bin page-groups.bin page-bytes.bin seal.json row-pages.bin range-seal.json "
        "construct-resources.txt; do publish_artifact \"$name\"; done",
    )
    query = DEVELOPMENT_IDENTITIES["queries"]
    truth = DEVELOPMENT_IDENTITIES["truth"]
    query_command = _download(query.uri, query.sha256, query.bytes, "queries.parquet")
    truth_command = _download(truth.uri, truth.sha256, truth.bytes, "truth.parquet")
    evaluation_line = next(
        line for line in script.splitlines()
        if "scripts.native_one_million_page_oracle_cell evaluate " in line
    )
    plan_line = (
        evaluation_line.replace("evaluate-resources.txt", "plan-resources.txt")
        .replace("scripts.native_one_million_page_oracle_cell evaluate", "scripts.native_one_million_data_range_cell plan")
        .replace('"$root/evaluation"', '"$root/planning"')
    )
    new_evaluation_line = evaluation_line.replace(
        "scripts.native_one_million_page_oracle_cell evaluate",
        "scripts.native_one_million_data_range_cell evaluate",
    )
    script = _once(
        script,
        "phase=evaluate\n" + query_command + "\n" + truth_command
        + "\nchmod 0444 queries.parquet truth.parquet\nmkdir evaluation && chown nobody:nobody evaluation\n"
        + evaluation_line,
        "phase=plan\n" + query_command + "\nchmod 0444 queries.parquet\n"
        "mkdir planning && chown nobody:nobody planning\n" + plan_line + "\n"
        "mv planning/range-plans.json range-plans.json\n"
        "mv planning/range-plan-seal.json range-plan-seal.json\n"
        "rmdir planning\ncheck_resources plan-resources.txt\n"
        "chmod 0444 range-plans.json range-plan-seal.json\n"
        "for name in range-plans.json range-plan-seal.json plan-resources.txt; do publish_artifact \"$name\"; done\n"
        "phase=evaluate\n" + truth_command + "\nchmod 0444 truth.parquet\n"
        "mkdir evaluation && chown nobody:nobody evaluation\n" + new_evaluation_line,
    )
    script = _once(
        script,
        "mv evaluation/evidence.json evidence.json\n"
        "mv evaluation/result.json result.json\n"
        "rmdir evaluation\ncheck_resources evaluate-resources.txt\n"
        "for name in evidence.json result.json evaluate-resources.txt; do publish_artifact \"$name\"; done",
        "mv evaluation/range-evidence.json range-evidence.json\n"
        "mv evaluation/range-result.json range-result.json\n"
        "rmdir evaluation\ncheck_resources evaluate-resources.txt\n"
        "for name in range-evidence.json range-result.json evaluate-resources.txt; do publish_artifact \"$name\"; done",
    )
    script = _once(
        script, "scripts.validate_native_one_million_page_oracle",
        "scripts.validate_native_one_million_data_range_cell",
    )
    script = _once(
        script, "for name in validation.json validate-resources.txt; do publish_artifact \"$name\"; done",
        "for name in range-validation.json validate-resources.txt; do publish_artifact \"$name\"; done",
    )
    script = _once(
        script,
        "for resource in construct-resources.txt evaluate-resources.txt validate-resources.txt; do",
        "for resource in construct-resources.txt plan-resources.txt evaluate-resources.txt validate-resources.txt; do",
    )
    if len(script.encode()) > 16_384:
        raise ValueError("data-range Spot worker exceeds EC2 user-data limit")
    return script
