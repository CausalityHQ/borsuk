import ast
from contextlib import redirect_stdout
import copy
import io
import json
from pathlib import Path
import platform
import resource
import signal
import sys
import tempfile
from unittest.mock import patch

resource.setrlimit(resource.RLIMIT_AS, (256 << 20, 256 << 20))
signal.alarm(55)
repo = Path.cwd()
sys.path.insert(0, str(repo))
from scripts import run_cohere_fixed48_fresh_falsifier as h
config_path = repo / h.BASE / "scientific-execution/driver-config.json"
config = h.read_json(config_path)
original_config = copy.deepcopy(config)
config["code_sha256"] = {n: h.identity(repo / n)["sha256"] for n in h.CODE}
old = h.offline.decode(h.retained.archived.read_ref(repo, h.retained.FIXED["archived_config"]))
original_old = h.canonical(old)
metadata = h.panel_authority(config, repo, old)
selected = metadata["panel"]["selected_sha256"]
assert selected == "dfe569e0198f35d690e5aa6a248aa8eee73b6d59a569b194729368e84aa2f61c"
controller = ast.parse((repo / "scripts/launch_cohere_fixed48_fresh_scientific_spot.py").read_text())
inspect = next(ast.literal_eval(n.value) for n in controller.body
               if isinstance(n, ast.Assign) and any(isinstance(t, ast.Name) and t.id == "INSPECT" for t in n.targets))
version_function = platform.python_version
with tempfile.TemporaryDirectory(prefix="fixed48-metadata-only-") as temporary:
    fresh_config = Path(temporary) / "config.json"
    fresh_config.write_bytes(h.canonical(config))
    with patch.object(platform, "python_version", return_value="3.12.0"):
        assert h.read_config(fresh_config, h.identity(fresh_config)["sha256"], repo) == config
        assert h.panel_authority(config, repo, old) == metadata
        coverage = sys.modules["select_cohere_fresh64_coverage"]
        a, first, pins = coverage.authenticate(repo, historical_metadata_replay=True)
        assert a["selection"]["python"] == {"implementation": "CPython", "version": "3.14.4"}
        for call in (lambda: coverage.authenticate(repo), lambda: coverage.prepare(repo),
                     lambda: coverage.original.select_panel(a)):
            try:
                call()
            except ValueError as error:
                assert str(error) == "frozen Python implementation/version differs"
            else:
                raise AssertionError("fresh cross-version sampler admitted")
        try:
            h.read_config(config_path, h.identity(config_path)["sha256"], repo)
        except ValueError as error:
            assert "authenticated length/hash differs" in str(error)
        else:
            raise AssertionError("stale source/config pin admitted")
        output = io.StringIO()
        with patch.object(sys, "argv", ["-c", str(fresh_config), h.identity(fresh_config)["sha256"], str(repo)]), redirect_stdout(output):
            exec(compile(inspect, "controller-INSPECT", "exec"), {})
        inspected = json.loads(output.getvalue())
        assert inspected["selected_sha256"] == selected
        assert inspected["native_source_identity_sha256"] == h.SOURCE_ID
        assert len(inspected["acquisition_objects"]) == 67
assert platform.python_version is version_function
assert h.canonical(old) == original_old and h.read_json(config_path) == original_config
assert h.panel_authority(config, repo, old) == metadata
assert "numpy" not in sys.modules and "pyarrow" not in sys.modules
print(json.dumps(dict(passed=True, replay_runtime="3.12.0", sampling_provenance=a["selection"]["python"],
    selected_sha256=selected, sealed_locators=len(metadata["panel"]["selected"]),
    new_sampling_host_mismatch_rejected=True, stale_source_config_rejected=True,
    controller_INSPECT_passed=True, runtime_panel_authority_passed=True,
    acquisition_objects=len(inspected["acquisition_objects"]), native_source_identity_sha256=h.SOURCE_ID,
    restoration_verified=True, metadata_only=True, network_requests=0,
    peak_rss_kib=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss), sort_keys=True))
