import tempfile
import unittest
from pathlib import Path
from unittest import mock

from scripts.publication_v3_attestation import (
    _current_cgroup_root,
    _measured_source_revision,
    collect_runtime_attestation,
    validate_runtime_attestation,
)
from scripts.publication_v3_protocol import build_schedule_document, validate_manifest
from scripts.test_publication_v3_protocol import paid_v3_manifest


class PublicationV3AttestationTests(unittest.TestCase):
    def test_frozen_archive_revision_marker_is_runtime_authority(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            source = Path(directory)
            (source / ".borsuk-source-revision").write_text("1" * 40 + "\n")
            self.assertEqual(_measured_source_revision(source), "1" * 40)

    def test_process_cgroup_must_resolve_on_a_cgroup_v2_filesystem(self) -> None:
        with tempfile.TemporaryDirectory() as root:
            mount = Path(root) / "cgroup"
            mount.mkdir()
            process = Path(root) / "self.cgroup"
            process.write_text("0::/runtime.slice\n", encoding="utf-8")
            (mount / "runtime.slice").mkdir()
            with (
                mock.patch(
                    "scripts.publication_v3_attestation._filesystem_magic",
                    return_value=0xEF53,
                ),
                self.assertRaisesRegex(ValueError, "cgroup v2"),
            ):
                _current_cgroup_root(proc_self_cgroup=process, cgroup_mount=mount)

    def test_collector_reads_cgroup_v2_and_cache_filesystem_evidence(self) -> None:
        cell = next(
            cell
            for cell in build_schedule_document(validate_manifest(paid_v3_manifest()))[
                "cells"
            ]
            if cell["system"] == "borsuk" and cell["workload"]["kind"] == "read-recall"
        )
        with tempfile.TemporaryDirectory() as root:
            cgroup = Path(root) / "cgroup"
            cache = Path(root) / "cache"
            cgroup.mkdir()
            cache.mkdir()
            (cgroup / "memory.max").write_text(str(8 * 1024**3), encoding="utf-8")
            (cgroup / "memory.peak").write_text(str(2 * 1024**3), encoding="utf-8")
            (cgroup / "memory.swap.max").write_text("0", encoding="utf-8")
            (cgroup / "memory.swap.current").write_text("0", encoding="utf-8")
            (cgroup / "memory.events").write_text(
                "low 0\nhigh 0\nmax 0\noom 0\noom_kill 0\noom_group_kill 0\n",
                encoding="utf-8",
            )
            (cgroup / "cpuset.cpus.effective").write_text("0-1,4-5", encoding="utf-8")
            proc_cgroup = Path(root) / "self.cgroup"
            proc_cgroup.write_text("0::/\n", encoding="utf-8")
            runtime = {"steps": [{"env": {"BORSUK_BENCH_CACHE": str(cache)}}]}
            with (
                mock.patch(
                    "scripts.publication_v3_attestation._filesystem_magic",
                    return_value=0x63677270,
                ),
                mock.patch(
                    "scripts.publication_v3_attestation._instance_identity_document",
                    return_value={
                        "instanceId": "i-runtime-01",
                        "instanceType": "c7g.xlarge",
                    },
                ),
                mock.patch(
                    "scripts.publication_v3_attestation._measured_source_revision",
                    return_value="1" * 40,
                ),
                mock.patch(
                    "scripts.publication_v3_attestation.platform.machine",
                    return_value="aarch64",
                ),
            ):
                observed = collect_runtime_attestation(
                    cell=cell,
                    attempt_id="attempt-01",
                    runtime=runtime,
                    source_root=Path(root),
                    proc_self_cgroup=proc_cgroup,
                    cgroup_mount=cgroup,
                )
        self.assertEqual(observed["vcpus"], 4)
        self.assertEqual(observed["memory_max_bytes"], 8 * 1024**3)
        self.assertEqual(observed["swap_max_bytes"], 0)
        self.assertEqual(observed["swap_peak_bytes"], 0)
        self.assertEqual(observed["cache_limit_bytes"], 1024**3)
        self.assertRegex(observed["cache_device"], r"^[0-9]+:[0-9]+$")
        self.assertRegex(observed["root_device"], r"^[0-9]+:[0-9]+$")

    def test_small_runtime_requires_cgroup_swap_and_cache_attestation(self) -> None:
        cell = next(
            cell
            for cell in build_schedule_document(validate_manifest(paid_v3_manifest()))[
                "cells"
            ]
            if cell["system"] == "borsuk" and cell["workload"]["kind"] == "read-recall"
        )
        client = cell["environment_contract"]["runtime_clients"]["borsuk"]
        value = {
            "schema_version": 1,
            "cell_id": cell["cell_id"],
            "attempt_id": "attempt-01",
            "instance_id": "i-runtime-01",
            "instance_type": client["instance_type"],
            "architecture": "aarch64",
            "vcpus": client["vcpus"],
            "memory_max_bytes": client["memory_mib"] * 1024 * 1024,
            "memory_peak_bytes": 1024 * 1024 * 1024,
            "swap_max_bytes": 0,
            "swap_current_bytes": 0,
            "swap_peak_bytes": 0,
            "oom_events": 0,
            "oom_kill_events": 0,
            "cache_limit_bytes": client["disk_cache_limit_mib"] * 1024 * 1024,
            "cache_filesystem_bytes": 32 * 1024 * 1024 * 1024,
            "cache_device": "259:1",
            "root_device": "259:0",
            "cache_is_mount": True,
            "source_revision": "1" * 40,
        }
        self.assertEqual(
            validate_runtime_attestation(value, cell=cell, attempt_id="attempt-01"),
            value,
        )
        for mutation in (
            {**value, "cell_id": "foreign-cell"},
            {**value, "attempt_id": "replayed-attempt"},
            {**value, "instance_type": "r7g.8xlarge"},
            {**value, "memory_max_bytes": value["memory_max_bytes"] + 1},
            {**value, "memory_max_bytes": value["memory_max_bytes"] - 1},
            {**value, "swap_max_bytes": 1024},
            {**value, "swap_current_bytes": 1},
            {**value, "swap_peak_bytes": 1},
            {**value, "oom_events": 1},
            {**value, "oom_kill_events": 1},
            {**value, "cache_limit_bytes": value["cache_limit_bytes"] + 1},
            {**value, "cache_filesystem_bytes": value["cache_filesystem_bytes"] + 1},
            {**value, "cache_filesystem_bytes": value["cache_limit_bytes"] - 1},
            {**value, "source_revision": "2" * 40},
            {**value, "cache_is_mount": False},
            {**value, "cache_device": value["root_device"]},
        ):
            with self.subTest(mutation=mutation), self.assertRaises(ValueError):
                validate_runtime_attestation(
                    mutation, cell=cell, attempt_id="attempt-01"
                )


if __name__ == "__main__":
    unittest.main()
