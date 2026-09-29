"""Check controller scope before any dataset, S3 or native execution."""

import copy
import json
from pathlib import Path
import sys
import tempfile
from unittest.mock import patch

from scripts import run_native_fresh_rank16_dev64 as driver


class ScopeAccepted(Exception):
    pass


def check():
    base = dict(rows=1_000_000, dimensions=768, first=0, count=64, offered_qps=8,
                setting_order=[10, 100, 100, 10], prospective_ordinals_sealed=[64, 999],
                fresh_cohort_used=True, code_sha256={}, binaries={},
                gates=dict(native_mean_recall_at_10_minimum=.95,
                           incoming_http_p90_ms_exclusive_maximum=444,
                           successful_qps_minimum=8, all_offered_success=True))
    with tempfile.TemporaryDirectory() as directory:
        path = Path(directory) / "config.json"

        def accepted(config):
            path.write_text(json.dumps(config))
            with patch.dict("os.environ", BORSUK_NATIVE_MEMORY_BYTES="1073741824"), \
                 patch.object(sys, "argv", ["driver", str(path), driver.sha(path),
                                            directory + "/out", directory, "prefix"]), \
                 patch.object(Path, "mkdir", side_effect=ScopeAccepted), \
                 patch.object(driver.subprocess, "run", side_effect=AssertionError("data execution")):
                try:
                    driver.main()
                except ScopeAccepted:
                    return True
                except ValueError:
                    return False
            raise AssertionError("scope did not terminate")

        for family in ["rank16", "cohere"]:
            for mode in ["dev64", "confirm936"]:
                config = copy.deepcopy(base)
                config["schema"] = f"borsuk-fresh-{family}-1m-{mode}-v1"
                if mode == "confirm936":
                    config.update(first=64, count=936, prospective_ordinals_sealed=[])
                assert accepted(config), config["schema"]
        base["schema"] = "borsuk-fresh-cohere-1m-dev64-v1"
        for field, value in [("count", 63), ("rows", 100_000), ("offered_qps", 9),
                             ("prospective_ordinals_sealed", [])]:
            config = copy.deepcopy(base)
            config[field] = value
            assert not accepted(config), field
        for field, value in [("native_mean_recall_at_10_minimum", .94),
                             ("incoming_http_p90_ms_exclusive_maximum", 445),
                             ("all_offered_success", False)]:
            config = copy.deepcopy(base)
            config["gates"][field] = value
            assert not accepted(config), field
    result = dict(passed=True, accepted_fixed_scopes=4, rejected_gate_or_shape_changes=7,
                  dataset_s3_or_native_execution=False,
                  controller_sha256=driver.sha("scripts/run_native_fresh_rank16_dev64.py"),
                  shared_http_sha256=driver.sha("scripts/run_native_current_1m_offered_http.py"))
    if len(sys.argv) == 2:
        Path(sys.argv[1]).write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps(result))


if __name__ == "__main__":
    check()
