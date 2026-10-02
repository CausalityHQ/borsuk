#!/usr/bin/env python3
"""Retain one exact CoHere semantic generation; root owns config and launch.

CONFIG SHA REPO NEW_EXTERNAL_OUTPUT; --replay CONFIG SHA REPO OUTPUT;
--self-check (stdlib synthetic bodies, no native/data/network execution).

Config is EXPECTED plus exact code_sha256/CODE, refs/FIXED, archived corpus and
builder, payloads=expected_payloads(authenticated metadata), a fresh
sq8_object_key, and execution_source={commit,archive_sha256} frozen by root.
The controller authenticates that source archive; this helper authenticates
its code closure and reuses only the archived builder's native assurance.
All 30 RETAINED_FILES survive success; COMPLETE.json is synced last. Replay
hashes these local bodies, including transported copies, without any builder,
query, truth or network operation. Admission/measurement concerns preparation;
no current native-tree, recall, cold-I/O or launch authority is supplied.
"""
import copy
from contextlib import contextmanager, nullcontext
import hashlib
import importlib
import json
import os
from pathlib import Path
import platform
import re
import signal
import stat
import sys
import tempfile
import time
from types import SimpleNamespace
from unittest.mock import patch

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from scripts import prepare_cohere_top32_coverage as archived
from scripts import check_semantic_router_coverage as primitives

require, canonical = archived.require, archived.canonical
identity, authenticate, publish = archived.identity, archived.authenticate, archived.publish

SCHEMA = "borsuk-cohere-semantic-artifact-reproduction-v1"
OWN = "scripts/reproduce_cohere_semantic_artifacts.py"
BASE = archived.prior.BASE
FIXED = {
    "archived_config": dict(path=BASE + "cohere-top32-coverage/a0002/helper-config.json", bytes=26989,
        sha256="7e4dc4aec01e127fb9a25d2095b870678b7e61589c08931b3d6b0f91d7011874"),
    "original_builder": dict(path=BASE + "cohere-top32-coverage/a0002/screen/builder-config.json", bytes=33501,
        sha256="294c184703a9cffaeb9c6f515dba50e32736262a44d44a2660d117305c162610"),
    "original_generation": dict(path=BASE + "cohere-top32-coverage/a0002/screen/generation-manifest.json", bytes=34552,
        sha256="439005046f048e4f5dab707bacdca533d1c96191a075f1b71da497de7f7c55a4"),
    "historical_root": dict(path=BASE + "cohere-input-authorities/historical-source-root.json", bytes=33963,
        sha256="a4eb4851c545e828f3d08181a0c3e9cca09ed9341032ee3e69c511b9b7caf67e"),
    "original_plane": dict(path=BASE + "fixed48/artifact-admission/original-plane-manifest.json", bytes=549,
        sha256="379c7e778672251a362ec2819e9745ec7ed206659e74104bc1a493ce343501b2"),
    "page_authority": dict(path=BASE + "fixed48/artifact-admission/page-manifest.json", bytes=280,
        sha256="dd18a4538064cd717aaa1a8a9bb1cae9b14189b171dd60113bff413584fd9142"),
    "admission": dict(path=BASE + "fixed48/artifact-admission/verification.json", bytes=1566,
        sha256="cc3994183d487afb74fe4ab0d9f485f317079dccb352ca45148f543e6dcf6cb7"),
    "preregister": dict(path=BASE + "fixed48/artifact-reproduction-preregister.md", bytes=3347,
        sha256="ff9275db5d2386ea7b02dce9b0348dae654430f00c59a9663b2d074401fcdc6f")}
LIMITS = dict(memory_bytes=12 << 30, scratch_bytes=16 << 30, cpu=2, threads=2,
              swap_bytes=0, max_requests=256)
EXPECTED = dict(schema=SCHEMA, authority_pending=False, bucket=archived.EXPECTED["bucket"],
    region="eu-central-1", rows=1_000_000, dimensions=768, profile="fresh1m", generation=1,
    base_epoch=0, payload_bytes=512 << 20, stage_limit_seconds=1800,
    service_limit_seconds=1860, machine_limit_seconds=3600, tasks_max=512,
    limits=LIMITS, query_or_truth_used=False, retune_allowed=False)
CODE = tuple(sorted(set((*archived.CODE, OWN))))
THREAD_ENV = ("OPENBLAS_NUM_THREADS", "OMP_NUM_THREADS", "MKL_NUM_THREADS", "BLIS_NUM_THREADS", "NUMEXPR_NUM_THREADS",
              "VECLIB_MAXIMUM_THREADS", "RAYON_NUM_THREADS", "TOKIO_WORKER_THREADS")
INPUT_FIELDS = dict(raw="source.raw", order="source-order.u64", root_manifest="source-root.json",
                    sq8="source-sq8.bin")
INPUT_FILES = tuple(INPUT_FIELDS.values())
GENERATION_FILES = ("manifest.json", "canonical.bin", "centroids.bin", "router/root.bin",
    "router/membership.bin", "router/leaves.bin", "plane/manifest.json", "plane/mean.bin",
    "plane/records.bin", "plane/page_digests.bin", "page_manifest.json", "page_digests.bin")
ARTIFACTS = ("config.json", "source-qualification.json", "input-hashes.json", "local-sq8-head.json",
    "sq8-ordinal-check.json", "builder-config.json", "build.log", "build-resources.txt",
    "build-resources.json", "payload-verification.json", "provenance.json", "cleanup.json", "resources.json")
RETAINED_FILES = (*INPUT_FILES, "builder", *("generation/" + n for n in GENERATION_FILES), *ARTIFACTS)


def regular_path(path):
    path = Path(path).absolute()
    require(".." not in path.parts and not any(p.is_symlink() for p in (path, *path.parents)), "symlink/traversal path")
    return path


def read_json(path):
    path = regular_path(path)
    require(path.is_file() and 0 < path.stat().st_size <= 1 << 20, "bounded regular metadata required")
    return primitives.decode(path.read_bytes())


def read_config(path, sha, repo):
    require(__debug__, "optimized Python disables authority checks")
    path = regular_path(path)
    require(path.is_file() and path.stat().st_size <= 1 << 20, "bounded regular config required")
    require(identity(path)["sha256"] == sha, "config body identity differs")
    config = primitives.decode(path.read_bytes())
    extra = {"code_sha256", "refs", "corpus", "builder", "payloads", "sq8_object_key", "execution_source"}
    require(set(config) == set(EXPECTED) | extra
            and all(canonical(config.get(k)) == canonical(v) for k, v in EXPECTED.items()), "configuration contract differs")
    require(Path(__file__).resolve() == repo / OWN, "helper origin differs")
    require(config["refs"] == FIXED and set(config["code_sha256"]) == set(CODE), "exact source/ref roster differs")
    for name, digest in config["code_sha256"].items():
        require(identity(regular_path(archived.prior.repo_path(repo, name)))["sha256"] == digest,
                "code identity differs: " + name)
    source = config["execution_source"]
    require(set(source) == {"commit", "archive_sha256"}
            and re.fullmatch("[0-9a-f]{40}", source["commit"])
            and re.fullmatch("[0-9a-f]{64}", source["archive_sha256"]), "execution source authority differs")
    key = config["sq8_object_key"]
    require(type(key) is str and len(key.encode()) <= 1024 and key.endswith("/objects/" + config["corpus"]["sq8"]["sha256"])
            and key == key.strip("/") and ".." not in key.split("/")
            and not any(c.isspace() or ord(c) < 32 for c in key)
            and not key.startswith(("coverage/", "quality/", "research/native-union/")), "new SQ8 envelope key required")
    return config


def metadata_helper(repo):
    # Use the existing load_inputs path without loading vector/oracle dependencies.
    for path in (repo, repo / "scripts"):
        if str(path) not in sys.path:
            sys.path.insert(0, str(path))
    helper = archived.isolated_module(repo, "prepare_cohere_semantic_1m_panel")
    helper.selector = archived.isolated_module(repo, "select_cohere_1m_fresh64")
    helper.shared = SimpleNamespace(authenticate=authenticate)
    return helper


def expected_payloads(data):
    root, plane, page = (data[n] for n in ("original_generation", "original_plane", "page_authority"))
    d = root["discovery"]; rows, dims = root["canonical"]["rows"], root["canonical"]["dimensions"]
    result = {"router/" + n + ".bin": dict(bytes=d[n + "_bytes"], sha256=d[n + "_sha256"])
              for n in ("root", "membership", "leaves")}
    result.update({"canonical.bin": {k: root["canonical"][k] for k in ("bytes", "sha256")},
        "centroids.bin": dict(bytes=32 + ((rows + 31) // 32) * dims * 2, sha256=d["centroids_sha256"]),
        "plane/mean.bin": dict(bytes=dims * 4, sha256=d["mean_sha256"]),
        "plane/records.bin": dict(bytes=rows * plane["record_bytes"], sha256=d["records_sha256"]),
        "plane/manifest.json": dict(sha256=root["plane_manifest_sha256"]),
        "plane/page_digests.bin": dict(bytes=((rows + 31) // 32) * 32,
            sha256_from="plane/manifest.json#page_digest_sha256"),
        "page_manifest.json": {k: FIXED["page_authority"][k] for k in ("bytes", "sha256")},
        "page_digests.bin": dict(bytes=((rows + 255) // 256) * 32, sha256=page["page_digest_sha256"])})
    return result


def authorities(config, repo):
    bodies = {n: archived.read_ref(repo, p) for n, p in FIXED.items()}
    data = {n: primitives.decode(b) for n, b in bodies.items() if n != "preregister"}
    old = data["archived_config"]
    archived.load_inputs(old, repo, metadata_helper(repo))
    require(config["corpus"] == old["corpus"] and config["builder"] == old["builder"], "immutable source/builder differs")
    historical, root, builder = (data[n] for n in ("historical_root", "original_generation", "original_builder"))
    require(root["schema"] == "borsuk-two-bit-generation-v8" and historical["schema"] == "borsuk-two-bit-generation-v4"
            and root["generation"] == historical["generation"] == builder["generation"] == 1
            and root["base_epoch"] == historical["base_epoch"] == builder["base_epoch"] == 0
            and root["low"] == historical["low"] == builder["low"]
            and root["step"] == historical["step"] == builder["step"]
            and all(len(root[n]) == 768 for n in ("low", "step")), "original encoding/format differs")
    require(root["discovery"]["input_root_sha256"] == root["plane_manifest_sha256"]
            == data["admission"]["required_plane_sha256"]
            and config["payloads"] == expected_payloads(data), "exact historical payload expectations differ")
    binary, proof = archived.builder_authority(repo, {
        n: primitives.decode(archived.read_ref(repo, archived.FIXED[n]))
        for n in ("quality_config", "quality_source", "quality_terminal")})
    return data, binary, proof


def dependencies(config, repo):
    for name in THREAD_ENV:
        os.environ[name] = "2"
    helper = metadata_helper(repo)
    helper.shared = importlib.import_module("scripts.prepare_semantic_1m_fresh_panel")
    helper.shared.pa.set_cpu_count(2)
    helper.shared.pa.set_io_thread_count(1)
    native = importlib.import_module("scripts.run_native_semantic_1m_quality")
    for name in native.THREAD_ENV:
        require(os.environ.get(name) == "2", "unsupported numerical thread control")
    for module in tuple(sys.modules.values()):
        origin = getattr(module, "__file__", None)
        if origin and Path(origin).resolve().is_relative_to(repo / "scripts"):
            require(str(Path(origin).resolve().relative_to(repo)) in config["code_sha256"], "unpinned imported helper")
    return helper, native


def admission(config):
    require(platform.machine() == "x86_64", "archived builder requires x86_64")
    release = platform.freedesktop_os_release()
    require(release["ID"] == "ubuntu" and release["VERSION_ID"] == "24.04", "qualified worker OS differs")
    group = archived.current_group()
    archived.prior.cgroup_limits(group, config["limits"])
    tasks = (group / "pids.max").read_text().strip()
    require(tasks != "max" and 0 < int(tasks) <= config["tasks_max"], "worker task cap differs")
    require(int((group / "memory.max").read_text()) == config["limits"]["memory_bytes"], "frozen 12GiB memory envelope differs")
    events = dict(line.split() for line in (group / "memory.events").read_text().splitlines())
    require(all(int(events.get(k, 0)) == 0 for k in ("oom", "oom_kill", "max")), "unclean worker memory admission")
    return group


def file_paths(out):
    require(out.is_dir() and not out.is_symlink(), "owned output directory required")
    result = {}
    for path in sorted(out.rglob("*")):
        regular_path(path)
        require(path.is_dir() or stat.S_ISREG(path.stat().st_mode), "nonregular output")
        if path.is_file():
            result[str(path.relative_to(out))] = path
    return result


def roster(out):
    result = {}
    for name, path in file_paths(out).items():
        pin = identity(path)
        result[name] = {k: pin[k] for k in ("bytes", "sha256")}
    return result


def check_generation(out, config, data):
    generation = out / "generation"
    require(set(file_paths(generation)) == set(GENERATION_FILES), "exact generation roster differs")
    root = read_json(generation / "manifest.json")
    builder = read_json(out / "builder-config.json")
    head = read_json(out / "local-sq8-head.json")
    check_head(head, config)
    recorded_out = Path(builder["raw"]).parent
    require(recorded_out.is_absolute() and builder == builder_config(config, recorded_out,
            data["historical_root"], head), "original build configuration/head binding differs")
    expected = copy.deepcopy(data["original_generation"])
    expected["sq8_etag"] = builder["sq8_etag"]
    expected["sq8_object_key"] = config["sq8_object_key"]
    expected["canonical"]["object_key"] = config["sq8_object_key"].rsplit("/", 1)[0] + "/" + expected["canonical"]["sha256"]
    require(root == expected, "historical v8 payload fields/new envelope differ")
    require((out / "build.log").stat().st_size <= 1 << 20
            and (out / "build.log").read_text().splitlines()[-1] == identity(generation / "manifest.json")["sha256"],
            "builder root digest differs")
    require(identity(generation / "plane/manifest.json")["sha256"]
            == config["payloads"]["plane/manifest.json"]["sha256"], "historical whole-plane SHA differs")
    plane = read_json(generation / "plane/manifest.json")
    require(plane["schema"] == "borsuk-two-bit-plane-v3" and plane["page_rows"] == 32
            and plane["query_or_truth_used"] is False
            and {k: v for k, v in plane.items() if k not in ("schema", "page_rows", "page_digest_sha256")}
                == {k: v for k, v in data["original_plane"].items() if k != "schema"}, "SOURCE plane authority differs")
    matched = {}
    for name, pin in config["payloads"].items():
        got = identity(generation / name)
        require(all(got[k] == v for k, v in pin.items() if k != "sha256_from"), "payload mismatch: " + name)
        if "sha256_from" in pin:
            require(pin["sha256_from"] == "plane/manifest.json#page_digest_sha256"
                    and got["sha256"] == plane["page_digest_sha256"], "SOURCE page digest mismatch")
        matched[name] = {k: got[k] for k in ("bytes", "sha256")}
    return dict(schema=SCHEMA + "-payloads", passed=True, historical_payloads_equal=True,
                original_root=FIXED["original_generation"], new_root=identity(generation / "manifest.json"), payloads=matched)


def builder_config(config, out, historical, head):
    return dict(discovery="semantic", semantic_profile="fresh1m", rows=config["rows"],
        dimensions=config["dimensions"], generation=1, base_epoch=0, raw=str(out / "source.raw"),
        raw_sha256=config["corpus"]["raw"]["sha256"], sq8=str(out / "source-sq8.bin"),
        sq8_sha256=config["corpus"]["sq8"]["sha256"],
        order=dict(path=str(out / "source-order.u64"), sha256=config["corpus"]["order"]["sha256"]),
        low=historical["low"], step=historical["step"], sq8_object_key=config["sq8_object_key"], sq8_etag=head["etag"])


def check_head(head, config):
    require(set(head) == {"backend", "version", "inode", "mtime_ns", "mtime_microseconds", "bytes", "etag",
                         "conditional_production_read_required", "archived_s3_etag_used"}
            and head["backend"] == "object_store.LocalFileSystem" and head["version"] == "0.14.1"
            and all(type(head[n]) is int and head[n] >= 0 for n in ("inode", "mtime_ns", "mtime_microseconds", "bytes"))
            and head["bytes"] == config["corpus"]["sq8"]["bytes"]
            and head["mtime_microseconds"] == head["mtime_ns"] // 1000
            and head["etag"] == '"{:x}-{:x}-{:x}"'.format(head["inode"], head["mtime_microseconds"], head["bytes"])
            and head["conditional_production_read_required"] is True and head["archived_s3_etag_used"] is False,
            "actual local SQ8 metadata authority differs")


def resource_report(accounting, passed, cleanup):
    return dict(accounting.report(passed, cleanup), schema=SCHEMA + "-resources",
        coverage_only=False, query_or_truth_used=False, retune_allowed=False,
        resource_measurements_synthetic=False, stage_resources=accounting.stage_resources,
        tasks_max=accounting.config["tasks_max"], actual_tasks_limit=None if accounting.group is None else
            (accounting.group / "pids.max").read_text().strip(),
        stage_peak_semantics="RSS/cgroup peaks are cumulative since process/cgroup creation; never reset",
        thread_environment={n: os.environ.get(n) for n in THREAD_ENV})


@contextmanager
def phase(accounting, name):
    with archived.prior.Accounting.stage(accounting, name):
        try:
            yield
        finally:
            report = accounting.report(False)
            accounting.stage_resources[name] = {k: report[k] for k in (
                "process_max_rss_kib", "child_max_rss_kib", "aggregate_memory_peak_bytes",
                "actual_scratch_bytes", "peak_scratch_bytes", "http_request_dispatch_attempts",
                "received_object_body_bytes", "get_response_content_length_bytes", "aws_errors")}


def check_resources(report, config):
    require(report["passed"] is True and report["build_invocations"] == 1
            and report["oracle_invocations"] == report["scorer_invocations"] == 0
            and report["prospective_preparation_limits"] == config["limits"]
            and report["resource_failure"] is None and report["transport_accounting_complete"] is True
            and report["aws_errors"] == 0 and report["aws_max_attempts"] == 1
            and report["tasks_max"] == config["tasks_max"]
            and all(report["thread_environment"][n] == "2" for n in THREAD_ENV)
            and all(int(report["memory_events"].get(n, 0)) == 0 for n in ("oom", "oom_kill", "max"))
            and report["process_max_rss_kib"] * 1024 <= config["limits"]["memory_bytes"]
            and report["child_max_rss_kib"] * 1024 <= config["limits"]["memory_bytes"]
            and report["peak_scratch_bytes"] <= config["limits"]["scratch_bytes"]
            and report["wall_seconds"] <= config["stage_limit_seconds"]
            and sum(report["http_request_dispatch_attempts"].values()) <= config["limits"]["max_requests"],
            "artifact reproduction resources failed")
    group = report["cgroup"]
    if group is not None:
        quota, period = group["cpu.max"].split()
        require(int(group["memory.max"]) == config["limits"]["memory_bytes"]
                and int(group["memory.peak"]) <= config["limits"]["memory_bytes"]
                and quota != "max" and 0 < int(quota) <= config["limits"]["cpu"] * int(period)
                and group["memory.swap.max"] == group["memory.swap.peak"] == "0"
                and report["actual_tasks_limit"] != "max"
                and 0 < int(report["actual_tasks_limit"]) <= config["tasks_max"], "cgroup cap/swap/tasks failure")


def run(config_path, sha, repo, out):
    started = time.monotonic()
    repo, out = Path(repo).resolve(), regular_path(out)
    config = read_config(config_path, sha, repo)
    data, binary, proof = authorities(config, repo)
    require(not out.exists() and not out.resolve().is_relative_to(repo), "new external output required")
    group = admission(config)
    helper, native = dependencies(config, repo)
    out.mkdir()
    accounting = archived.Accounting(config, out, group)
    accounting.stage_resources = {}
    accounting.admission = dict(cgroup_path=None if group is None else str(group),
        prospective_limits=config["limits"], tasks_max=config["tasks_max"],
        thread_environment={n: os.environ.get(n) for n in THREAD_ENV})
    cleanup = dict(schema=SCHEMA + "-cleanup", process_cleanup=True, retained=True,
                   removed_files=[], build_invocations=0)
    try:
        # One accounting stage owns the entire acquisition/build/retention deadline.
        with accounting.stage("artifact_reproduction"):
            accounting.deadline = min(accounting.deadline, started + config["stage_limit_seconds"])
            signal.setitimer(signal.ITIMER_REAL, accounting.remaining())
            publish(out / "config.json", Path(config_path).read_bytes())
            publish(out / "source-qualification.json", proof)
            with phase(accounting, "authenticated_inputs"), helper.accounted_helpers(accounting):
                for field, name in INPUT_FIELDS.items():
                    helper.download(config, config["corpus"][field], out / name, accounting)
                    authenticate(out / name, config["corpus"][field])
            inputs = {name: identity(out / name) for name in INPUT_FILES}
            publish(out / "input-hashes.json", inputs)
            historical = read_json(out / "source-root.json")
            require(historical == data["historical_root"] and historical["schema"] == "borsuk-two-bit-generation-v4",
                    "local historical root differs")
            head = native.local_head(out / "source-sq8.bin")
            check_head(head, config)
            publish(out / "local-sq8-head.json", head)
            with phase(accounting, "sq8_order_parity"):
                publish(out / "sq8-ordinal-check.json", native.ordinal_check(out / "source-sq8.bin",
                    out / "source-order.u64", rows=config["rows"], dimensions=config["dimensions"]))
            builder = builder_config(config, out, historical, head)
            require(len(canonical(builder)) <= 65536, "archived builder configuration byte cap")
            publish(out / "builder-config.json", builder)
            publish(out / "builder", binary); (out / "builder").chmod(0o500)
            authenticate(out / "builder", config["builder"])
            with phase(accounting, "one_archived_native_build"):
                accounting.build_invocations += 1
                cleanup.update(build_invocations=1, process_cleanup=False)
                signal.setitimer(signal.ITIMER_REAL, 0)
                accounting.native_active = True
                try:
                    result = native.run_process([str(out / "builder"), str(out / "builder-config.json"),
                        identity(out / "builder-config.json")["sha256"], str(config["payload_bytes"]),
                        str(out / "generation")], out / "build.log", accounting.remaining(), out / "build-resources.txt")
                    cleanup["process_cleanup"] = result["process_cleanup"]
                except Exception:
                    # The reused runner reaps its process group in finally, including timeout.
                    cleanup["process_cleanup"] = True
                    raise
                finally:
                    accounting.native_active = False
            publish(out / "build-resources.json", result)
            signal.setitimer(signal.ITIMER_REAL, accounting.remaining())
            require(result["exit_status"] == 0 and result["process_cleanup"] is True
                    and result["wall_seconds"] <= config["stage_limit_seconds"]
                    and result["process_peak_rss_kib"] * 1024 <= config["limits"]["memory_bytes"], "bounded native build failed")
            require(native.local_head(out / "source-sq8.bin") == head, "actual SQ8 metadata changed")
            with phase(accounting, "unchanged_inputs_and_exact_payloads"):
                for name, pin in inputs.items():
                    authenticate(out / name, pin)
                read_config(config_path, sha, repo)
                matched = check_generation(out, config, data)
            publish(out / "payload-verification.json", matched)
            publish(out / "provenance.json", dict(schema=SCHEMA + "-provenance", config_sha256=sha,
                execution_source=config["execution_source"], code_sha256=config["code_sha256"], refs=config["refs"],
                original_root=FIXED["original_generation"], new_root=matched["new_root"], actual_sq8_head=head,
                original_payloads_equal=True, archived_builder=identity(out / "builder"),
                native_rebuilt=False, query_or_truth_used=False, quality_measured=False, cold_http_measured=False,
                retained_generation=True, external_references={}))
            accounting.checkpoint()
            publish(out / "cleanup.json", cleanup)
            # Authenticate the complete retained bodies while the resource observer is active.
            with phase(accounting, "retained_roster_and_sync"):
                files = roster(out)
                require(set(files) == set(RETAINED_FILES) - {"resources.json"}, "exact retained roster differs")
                for path in file_paths(out).values():
                    path.chmod(0o444)
                    with path.open("rb") as stream:
                        os.fsync(stream.fileno())
        report = resource_report(accounting, True, cleanup)
        report["wall_seconds"] = time.monotonic() - started
        check_resources(report, config)
        publish(out / "resources.json", report)
        pin = identity(out / "resources.json")
        files["resources.json"] = {k: pin[k] for k in ("bytes", "sha256")}
        for directory in (out / "generation/plane", out / "generation/router", out / "generation", out):
            fd = os.open(directory, os.O_RDONLY | os.O_DIRECTORY)
            try:
                os.fsync(fd)
            finally:
                os.close(fd)
        marker = dict(schema=SCHEMA + "-complete", passed=True, config_sha256=sha, files=files,
            roster_sha256=archived.prior.value_sha(files), build_invocations=1,
            query_or_truth_used=False, quality_measured=False, cold_http_measured=False,
            process_cleanup=True, retained=True, new_root=matched["new_root"])
        accounting.checkpoint(len(canonical(marker)))
        require(time.monotonic() - started <= config["stage_limit_seconds"], "whole-helper deadline exceeded")
        publish(out / "COMPLETE.json", marker)
        return marker
    except BaseException as error:
        marker = out / "COMPLETE.json"
        revoked = marker.exists()
        if revoked:
            marker.unlink()  # A failed directory sync must not leave completion authority.
        if not (out / "failure-resources.json").exists():
            publish(out / "failure-resources.json", resource_report(accounting, False, cleanup))
        if not (out / "cleanup.json").exists():
            publish(out / "cleanup.json", cleanup)
        publish(out / "failure.json", dict(schema=SCHEMA + "-failure", passed=False,
            error_type=type(error).__name__, error=str(error), build_invocations=accounting.build_invocations,
            process_cleanup=cleanup["process_cleanup"], diagnostics_preserved=True, completion_revoked=revoked))
        raise


def replay(config_path, sha, repo, out):
    repo, out = Path(repo).resolve(), regular_path(out)
    config = read_config(config_path, sha, repo)
    data, binary, proof = authorities(config, repo)
    marker = read_json(out / "COMPLETE.json")
    require(marker["schema"] == SCHEMA + "-complete" and marker["passed"] is True
            and marker["config_sha256"] == sha and marker["build_invocations"] == 1
            and marker["process_cleanup"] is marker["retained"] is True
            and marker["query_or_truth_used"] is marker["quality_measured"] is marker["cold_http_measured"] is False,
            "completion authority differs")
    files = roster(out); files.pop("COMPLETE.json")
    require(set(files) == set(RETAINED_FILES) and files == marker["files"]
            and archived.prior.value_sha(files) == marker["roster_sha256"], "retained body/roster identity differs")
    require((out / "config.json").read_bytes() == Path(config_path).read_bytes()
            and read_json(out / "source-qualification.json") == proof, "config/archived proof drift")
    for field, name in INPUT_FIELDS.items():
        authenticate(out / name, config["corpus"][field])
    authenticate(out / "builder", config["builder"])
    matched = check_generation(out, config, data)
    # Stored path labels are historical provenance; replay opens only this retained roster.
    recorded = read_json(out / "payload-verification.json")
    require(matched == recorded,
            "payload receipt replay differs")
    check_resources(read_json(out / "resources.json"), config)
    result = read_json(out / "build-resources.json")
    require(result["exit_status"] == 0 and result["process_cleanup"] is True
            and result["process_peak_rss_kib"] * 1024 <= config["limits"]["memory_bytes"], "builder resource replay failed")
    require(marker["new_root"] == identity(out / "generation/manifest.json"), "completion root drift")
    return marker


def _self_check():
    """Exercise retained authority, not quality or native resource measurements."""
    def rejected(call):
        try:
            call()
        except (ValueError, FileExistsError, OSError, archived.subprocess.TimeoutExpired):
            return
        raise AssertionError("negative check admitted")
    repo = Path(__file__).resolve().parents[1]
    with tempfile.TemporaryDirectory(prefix="artifact-check-") as temporary:
        base = Path(temporary)
        refs = {n: primitives.decode(archived.read_ref(repo, p)) for n, p in FIXED.items() if n != "preregister"}
        frozen = dict(EXPECTED, refs=FIXED,
            code_sha256={n: identity(repo / n)["sha256"] for n in CODE},
            corpus=refs["archived_config"]["corpus"], builder=refs["archived_config"]["builder"],
            payloads=expected_payloads(refs), execution_source=dict(commit="a" * 40, archive_sha256="b" * 64),
            sq8_object_key="synthetic-contract/objects/" + refs["archived_config"]["corpus"]["sq8"]["sha256"])
        frozen_path = base / "frozen-config.json"
        frozen_path.write_bytes(canonical(frozen)); frozen_sha = identity(frozen_path)["sha256"]
        require(read_config(frozen_path, frozen_sha, repo) == frozen, "positive configuration authority")
        rejected(lambda: read_config(frozen_path, "0" * 64, repo))
        for field in ("limits", "code_sha256", "refs", "execution_source", "query_or_truth_used"):
            changed = copy.deepcopy(frozen)
            if field == "limits":
                changed[field]["memory_bytes"] = 8 << 30
            elif field == "code_sha256":
                changed[field][OWN] = "0" * 64
            elif field == "refs":
                changed[field]["original_generation"]["sha256"] = "0" * 64
            elif field == "execution_source":
                changed[field]["archive_sha256"] = "unknown"
            else:
                changed[field] = True
            frozen_path.write_bytes(canonical(changed))
            rejected(lambda: read_config(frozen_path, identity(frozen_path)["sha256"], repo))
        frozen_path.write_bytes(canonical(frozen))
        authenticated, binary, archived_proof = authorities(frozen, repo)
        require(hashlib.sha256(binary).hexdigest() == archived.BUILDER_SHA
                and archived_proof["original_full_workspace_execution_reused"] is True
                and archived_proof["native_rebuilt"] is False, "archived qualified binary authority")
        changed = copy.deepcopy(frozen); changed["payloads"]["plane/page_digests.bin"]["sha256_from"] = "invented"
        rejected(lambda: authorities(changed, repo))
        changed = copy.deepcopy(frozen); changed["corpus"]["raw"]["sha256"] = "0" * 64
        rejected(lambda: authorities(changed, repo))
        ref_reader = archived.read_ref
        full_pin = primitives.decode(ref_reader(repo, archived.FIXED["quality_config"]))["refs"]["full_verification"]
        def bad_proof(root, pointer):
            body = ref_reader(root, pointer)
            if pointer == full_pin:
                proof = primitives.decode(body); proof["qualified"] = False
                return canonical(proof)
            return body
        with patch.object(archived, "read_ref", bad_proof):
            rejected(lambda: authorities(frozen, repo))
        group = base / "cgroup"; group.mkdir()
        for name, value in {"memory.max": str(12 << 30), "memory.swap.max": "0",
                "cpu.max": "200000 100000", "pids.max": "512", "memory.events": "oom 0\noom_kill 0\nmax 0\n"}.items():
            (group / name).write_text(value)
        with patch.object(archived, "current_group", lambda: group), patch.object(platform, "machine", lambda: "x86_64"), patch.object(
                platform, "freedesktop_os_release", lambda: dict(ID="ubuntu", VERSION_ID="24.04")):
            require(admission(frozen) == group, "explicit prospective resource envelope")
            for name, bad in (("memory.max", str(8 << 30)), ("memory.swap.max", "1"),
                    ("cpu.max", "300000 100000"), ("pids.max", "max"), ("memory.events", "oom_kill 1\n")):
                path = group / name; original_value = path.read_text(); path.write_text(bad)
                rejected(lambda: admission(frozen)); path.write_text(original_value)
        bodies = {name: ("synthetic " + name).encode() for name in GENERATION_FILES
                  if name not in ("manifest.json", "plane/manifest.json", "page_manifest.json")}
        pin = lambda body: dict(bytes=len(body), sha256=hashlib.sha256(body).hexdigest())
        plane = dict(schema="borsuk-two-bit-plane-v3", rows=2, dimensions=2, seed=17,
            record_bytes=200, source_sha256=pin(b"raw")["sha256"],
            sq8_sha256=pin(b"sq8")["sha256"], source_order_sha256=pin(b"order")["sha256"],
            mean_sha256=pin(bodies["plane/mean.bin"])["sha256"],
            records_sha256=pin(bodies["plane/records.bin"])["sha256"], page_rows=32,
            page_digest_sha256=pin(bodies["plane/page_digests.bin"])["sha256"], query_or_truth_used=False)
        bodies["plane/manifest.json"] = canonical(plane)
        page = dict(schema="borsuk-v115-sq8-page-authority-v2", rows=2, dimensions=2,
            generation=1, page_rows=256, object_sha256=plane["sq8_sha256"],
            page_digest_sha256=pin(bodies["page_digests.bin"])["sha256"])
        bodies["page_manifest.json"] = canonical(page)
        discovery = dict(mode="semantic", profile="fresh1m", input_schema=plane["schema"],
            input_root_sha256=pin(bodies["plane/manifest.json"])["sha256"],
            centroids_sha256=pin(bodies["centroids.bin"])["sha256"],
            source_sha256=plane["source_sha256"], source_order_sha256=plane["source_order_sha256"],
            sq8_sha256=plane["sq8_sha256"], mean_sha256=plane["mean_sha256"],
            records_sha256=plane["records_sha256"])
        for name in ("root", "membership", "leaves"):
            discovery.update({name + "_" + k: v for k, v in pin(bodies["router/" + name + ".bin"]).items()})
        original = dict(schema="borsuk-two-bit-generation-v8", generation=1, base_epoch=0,
            discovery=discovery, low=[0., 0.], step=[1., 1.], sq8_etag='"historical"',
            sq8_object_key="old/objects/" + plane["sq8_sha256"], sq8_object_sha256=plane["sq8_sha256"],
            plane_manifest_sha256=discovery["input_root_sha256"],
            page_manifest_sha256=pin(bodies["page_manifest.json"])["sha256"],
            canonical=dict(pin(bodies["canonical.bin"]), rows=2, dimensions=2,
                object_key="old/objects/" + pin(bodies["canonical.bin"])["sha256"]))
        historical = dict(original, schema="borsuk-two-bit-generation-v4")
        source_bodies = dict(zip(INPUT_FILES, (b"raw", b"order", canonical(historical), b"sq8")))
        corpus = {field: dict(pin(source_bodies[name]), key="original/" + name)
                  for field, name in INPUT_FIELDS.items()}
        config = dict(EXPECTED, rows=2, dimensions=2, corpus=corpus, builder=pin(b"binary"),
            code_sha256={}, refs={}, payloads={n: pin(b) for n, b in bodies.items()},
            sq8_object_key="new/objects/" + corpus["sq8"]["sha256"],
            execution_source=dict(commit="a" * 40, archive_sha256="b" * 64))
        config["payloads"]["plane/page_digests.bin"] = dict(bytes=len(bodies["plane/page_digests.bin"]),
            sha256_from="plane/manifest.json#page_digest_sha256")
        config["limits"] = dict(LIMITS, memory_bytes=256 << 20, scratch_bytes=1 << 20)
        old_plane = {k: v for k, v in plane.items() if k not in ("page_rows", "page_digest_sha256")}
        old_plane["schema"] = "borsuk-two-bit-plane-v2"
        data = dict(original_generation=original, historical_root=historical, original_plane=old_plane)
        proof = dict(builder_binary=config["builder"], native_rebuilt=False,
                     current_whole_tree_full_execution=False)
        calls, downloaded, publications = [], [], []
        def download(conf, pointer, target, accounting):
            downloaded.append(target.name)
            target.write_bytes(source_bodies[target.name])
        def build(args, log, seconds, timing):
            calls.append(args)
            target = Path(args[-1]); target.mkdir()
            builder = json.loads(Path(args[1]).read_bytes())
            fresh = copy.deepcopy(original)
            fresh["sq8_etag"] = builder["sq8_etag"]
            fresh["sq8_object_key"] = builder["sq8_object_key"]
            fresh["canonical"]["object_key"] = builder["sq8_object_key"].rsplit("/", 1)[0] + "/" + fresh["canonical"]["sha256"]
            for name, body in dict(bodies, **{"manifest.json": canonical(fresh)}).items():
                path = target / name; path.parent.mkdir(parents=True, exist_ok=True); path.write_bytes(body)
            log.write_text(identity(target / "manifest.json")["sha256"] + "\n")
            timing.write_text("synthetic timing\n")
            return dict(command=args, exit_status=0, process_cleanup=True, wall_seconds=.001,
                        process_peak_rss_kib=1)
        def synthetic_head(path):
            st = path.stat(); micros = st.st_mtime_ns // 1000
            return dict(backend="object_store.LocalFileSystem", version="0.14.1", inode=st.st_ino,
                mtime_ns=st.st_mtime_ns, mtime_microseconds=micros, bytes=st.st_size,
                etag='"{:x}-{:x}-{:x}"'.format(st.st_ino, micros, st.st_size),
                conditional_production_read_required=True, archived_s3_etag_used=False)
        native = SimpleNamespace(run_process=build, local_head=synthetic_head,
            ordinal_check=lambda *a, **k: dict(id_matches_order=True, complete_bijection=True))
        config_path = base / "config.json"; config_path.write_bytes(canonical(config))
        digest = identity(config_path)["sha256"]
        measurements = resource_report
        publishing = publish
        def observed_publish(path, value):
            result = publishing(path, value)
            publications.append(path.name)
            return result
        with patch.dict(os.environ, {n: "2" for n in THREAD_ENV}), patch.multiple(sys.modules[__name__], read_config=lambda *a: config,
                authorities=lambda *a: (data, b"binary", proof),
                dependencies=lambda *a: (SimpleNamespace(download=download, accounted_helpers=lambda a: nullcontext()), native),
                admission=lambda *a: None,
                publish=observed_publish,
                resource_report=lambda *a: dict(measurements(*a), resource_measurements_synthetic=True)):
            out = base / "success"
            run(config_path, digest, repo, out)
            require(len(calls) == 1 and downloaded == list(INPUT_FILES), "one corpus-only invocation")
            require(all((out / n).is_file() for n in RETAINED_FILES), "retained source/generation roster")
            require(publications[-1] == "COMPLETE.json" and all(
                (out / n).stat().st_mode & 0o222 == 0 for n in RETAINED_FILES), "read-only retention/marker-last")
            require(replay(config_path, digest, repo, out)["passed"], "retained replay")
            copied = base / "transported"
            archived.shutil.copytree(out, copied)
            require(replay(config_path, digest, repo, copied)["passed"], "transported retained replay")
            rejected(lambda: run(config_path, digest, repo, out))
            require(len(calls) == 1, "overwrite triggered builder")
            corrupt = out / "generation/router/root.bin"
            corrupt.chmod(0o600); saved = corrupt.read_bytes(); corrupt.write_bytes(b"tampered")
            rejected(lambda: replay(config_path, digest, repo, out))
            corrupt.write_bytes(saved); corrupt.chmod(0o444)
            (out / "unexpected").write_bytes(b"extra")
            rejected(lambda: replay(config_path, digest, repo, out))
            (out / "unexpected").unlink()
            for failure in ("input", "payload", "plane", "source-page", "exit", "cleanup", "rss", "root", "head", "scratch", "timeout", "marker"):
                bad_out = base / failure
                def bad_download(conf, pointer, target, accounting):
                    download(conf, pointer, target, accounting)
                    if failure == "input" and target.name == "source.raw":
                        target.write_bytes(b"wrong")
                    if failure == "scratch" and target.name == "source.raw":
                        accounting.limits["scratch_bytes"] = 1
                def bad_build(*args):
                    result = build(*args)
                    if failure == "payload":
                        (bad_out / "generation/router/leaves.bin").write_bytes(b"wrong")
                    if failure in ("plane", "source-page"):
                        (bad_out / ("generation/plane/manifest.json" if failure == "plane"
                                    else "generation/plane/page_digests.bin")).write_bytes(b"wrong")
                    if failure in ("exit", "cleanup", "rss"):
                        result[{"exit": "exit_status", "cleanup": "process_cleanup", "rss": "process_peak_rss_kib"}[failure]] = {"exit": 1, "cleanup": False, "rss": 1 << 30}[failure]
                    if failure == "root":
                        args[1].write_text("wrong root digest\n")
                    if failure == "head":
                        (bad_out / "source-sq8.bin").write_bytes(b"changed")
                    if failure == "timeout":
                        raise archived.subprocess.TimeoutExpired(args[0], .001)
                    return result
                before = len(calls)
                def bad_publish(path, value):
                    result = observed_publish(path, value)
                    if failure == "marker" and path.name == "COMPLETE.json":
                        raise OSError("synthetic directory fsync failure after link")
                    return result
                with patch.object(native, "run_process", bad_build), patch.object(
                        sys.modules[__name__], "dependencies", lambda *a: (SimpleNamespace(download=bad_download,
                            accounted_helpers=lambda a: nullcontext()), native)), patch.object(
                        sys.modules[__name__], "publish", bad_publish):
                    rejected(lambda: run(config_path, digest, repo, bad_out))
                require(len(calls) - before == (0 if failure in ("input", "scratch") else 1), "failure retried builder")
                require(not (bad_out / "COMPLETE.json").exists()
                        and (bad_out / "failure.json").is_file()
                        and (bad_out / "failure-resources.json").is_file(), "failure completion/diagnostics")
                require((bad_out / "source.raw").exists(), "failure removed input diagnostics")
                if failure == "timeout":
                    require(read_json(bad_out / "cleanup.json")["process_cleanup"] is True
                            and (bad_out / "build.log").is_file(), "timeout cleanup/diagnostic contract")
    print("PASS retained artifacts: config/source/proof/caps; one invocation; fail-closed input/payload/resources/cleanup/root/head; retention/replay/transport/tamper/overwrite; no query/truth/subprocess/network")


def self_check():
    repo = Path(__file__).resolve().parents[1]
    opened, read_bytes = Path.open, Path.read_bytes
    def no_data(path):
        require(not (path.resolve().is_relative_to(repo) and (path.suffix in (".raw", ".bin", ".u32", ".i64", ".parquet")
            or path.name in ("requests.jsonl", "nomination.json", "coverage.json"))), "self-check opened real data")
    def guarded_open(path, *a, **kw):
        no_data(path)
        return opened(path, *a, **kw)
    def guarded_read(path, *a, **kw):
        no_data(path)
        return read_bytes(path, *a, **kw)
    def forbidden(*a, **kw):
        raise AssertionError("self-check executed subprocess/network")
    with patch.object(Path, "open", guarded_open), patch.object(Path, "read_bytes", guarded_read), patch(
            "subprocess.run", forbidden), patch("subprocess.Popen", forbidden), patch("socket.socket", forbidden):
        _self_check()


if __name__ == "__main__":
    if sys.argv[1:] == ["--self-check"]:
        self_check()
    elif len(sys.argv) == 6 and sys.argv[1] == "--replay":
        replay(*sys.argv[2:])
        print("PASS retained artifact replay; no native/query/truth execution")
    elif len(sys.argv) == 5:
        run(*sys.argv[1:])
        print("PASS retained semantic artifacts; no quality/cold measurement")
    else:
        raise SystemExit("usage: CONFIG SHA REPO NEW_EXTERNAL_OUTPUT | --replay CONFIG SHA REPO OUTPUT | --self-check")
