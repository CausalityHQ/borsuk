"""Spot script keeps query plans sealed before development truth arrives."""

import subprocess

from scripts.launch_native_geometric_layout_spot import SourceArchiveIdentity
from scripts.launch_native_one_million_selector_spot import (
    artifact_names,
    build_launch_specs,
    build_plan,
    worker_script,
)


def test_data_range_worker_separates_plan_and_truth_and_publishes_terminal() -> None:
    commit = "12" * 20
    plan = build_plan(
        source_commit=commit,
        source_archive=SourceArchiveIdentity("s3://frozen/source.tar.gz", "34" * 32, 1234),
        requirements_sha256="56" * 32,
        output_prefix=(
            "s3://borsuk-bench-453182569524-euc1/research/"
            f"native-one-million-data-range-selector/{commit}/runs/relaion-1m-dev1000-a0001"
        ),
        selector_kind="data_range",
    )
    script = worker_script(plan)
    checked = subprocess.run(["bash", "-n"], input=script, text=True, capture_output=True)
    assert checked.returncode == 0, checked.stderr
    assert len(script.encode()) <= 16_384
    assert "truth.parquet" not in script[script.index("phase=source") : script.index("phase=evaluate")]
    assert script.index("range-plan-seal.json") < script.index("phase=evaluate")
    assert "scripts.native_one_million_data_range_cell plan" in script
    assert "scripts.validate_native_one_million_data_range_cell" in script
    assert "--if-none-match '*'" in script
    assert "swapoff -a" in script
    assert len(artifact_names("data_range")) == 15
    assert all(
        item["InstanceMarketOptions"]["MarketType"] == "spot"
        for item in build_launch_specs(plan)
    )
