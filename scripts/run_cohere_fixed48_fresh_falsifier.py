#!/usr/bin/env python3
"""One frozen fixed48 run: CONFIG SHA REPO NEW_EXTERNAL_OUTPUT.

--replay CONFIG SHA REPO OUTPUT authenticates/reduces retained evidence only.
--self-check uses bounded stdlib synthetic bodies and mocked external seams.
Root owns qualification, configuration, transport, and any actual launch.
"""
from contextlib import contextmanager
import copy
import hashlib
import importlib
import json
import os
from pathlib import Path
import re
import resource
import signal
import struct
import sys
import tempfile
import time
from types import SimpleNamespace
from unittest.mock import patch

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from scripts import reproduce_cohere_semantic_artifacts as retained
from scripts import evaluate_semantic_frozen_diagnostics as offline
from scripts import launch_native_workspace_execution_spot as qualification

require, canonical = retained.require, retained.canonical
identity, authenticate, publish = retained.identity, retained.authenticate, retained.publish
regular_path, read_json = retained.regular_path, retained.read_json
SCHEMA = "borsuk-cohere-fixed48-fresh-falsifier-v1"
OWN = "scripts/run_cohere_fixed48_fresh_falsifier.py"
BASE = retained.BASE + "fixed48/"
SOURCE_ID = "addf62bce23ceee034f22e4d1dfc0b318564d924c6e0fcee34b9336d1532813e"
ROOT_PIN = dict(path=BASE + "root-metadata-freeze.json", bytes=2073,
    sha256="a4be4935b97ff6530bce3c212cf522064b28e20b6fcaffee8ce5d9d4198494d4")
REFS = {
    "authority.json": dict(path=BASE + "panel-tools/authority.json", bytes=224514,
        sha256="6c6508887785d380436a91e1d06d33a9292d80de1dd5fdb8ac56050cdf97ce9e"),
    "panel.json": dict(path=BASE + "panel-tools/panel.json", bytes=79074,
        sha256="a137588d5e17703610d9142f468e75362d1aac61d84a0b72f7a8dbcbd1373392"),
    "root-freeze.json": dict(path=BASE + "panel-tools/root-freeze.json", bytes=1413,
        sha256="8b69a199151a32c7b900fa0bb8bcdd03b2f28fbcda8d55ada4efb7b6868cabb5"),
    "verification.json": dict(path=BASE + "panel-tools/verification.json", bytes=9794,
        sha256="1fe5c381eb3cb3224a1963573442abae551cdd4d3cf7634000229d7de9c4519b"),
    "root_metadata_freeze": ROOT_PIN,
    "selector_protocol": dict(path=BASE + "selector-protocol.json", bytes=8573,
        sha256="f521c49f1abdcf9bcf72f445186a5349a4831972aeb4933e86afca6f726b37c2")}
CODE = tuple(sorted(set((*retained.CODE, *qualification.FULL_CODE, OWN,
    "scripts/check_fixed48_implementation.sh", "scripts/check_rust_test_build.sh",
    "scripts/select_cohere_fixed48_fresh64.py", "scripts/evaluate_semantic_frozen_diagnostics.py",
    "scripts/prepare_semantic_positive_inputs.py"))))
EXPECTED = dict(schema=SCHEMA, authority_pending=False, bucket=retained.EXPECTED["bucket"],
    region="eu-central-1", rows=1_000_000, dimensions=768, queries=64, k=100,
    profile="fresh1m", generation=1, base_epoch=0, scorer_memory_bytes=512 << 20,
    stage_limit_seconds=3600, service_limit_seconds=3660, machine_limit_seconds=4500,
    tasks_max=512, limits=retained.LIMITS, quality_peek_allowed=False,
    retune_allowed=False, complete_historical_coverage=False,
    physical_s3_measured=False, cold_http_measured=False)
EXTRA = {"code_sha256", "refs", "retained", "qualification", "execution_source", "generation_prefix"}
OUTPUT_FILES = ("config.json", "source-qualification.json", "panel.json", "duplicate-audit.json",
    "queries.raw", "requests.jsonl", "input-hashes.json", "original-generation-root.json", "rehost.json",
    "local-sq8-head.json", "sq8-ordinal-check.json", "scorer", "scorer-config.json", "score.log",
    "score-resources.txt", "score-resources.json", "records.jsonl", "measurement-resources.json",
    "measurement-cleanup.json", "measurement-receipt.json", "measurement-sequence.json", "truth.u32", "truth.i64",
    "oracle.json", "offline-config.json", "offline-result.json", "resources.json", "cleanup.json",
    "stage-sequence.json", "decision.json", "source.raw", "source-order.u64", "source-root.json",
    "consumed-queries.raw", "prior-queries.raw", "test-queries.raw")


def safe_key(value):
    require(type(value) is str and 0 < len(value.encode()) <= 1024 and all(
        part and part not in (".", "..") and re.fullmatch(r"[A-Za-z0-9_.-]+", part)
        for part in value.split("/")), "unsafe ObjectStore key")
    return value


def descriptor(path):
    return dict(identity(regular_path(path)), path=str(Path(path).absolute()))


def read_pointer(pin, cap=1 << 20):
    offline.pointer(pin, cap)
    return offline.read(dict(pin, path=str(regular_path(pin["path"]))), cap)


def check_code(config, repo):
    require(set(config["code_sha256"]) == set(CODE), "exact import closure differs")
    for name, digest in config["code_sha256"].items():
        offline.digest(digest)
        authenticate(regular_path(retained.archived.prior.repo_path(repo, name)),
                     dict(bytes=identity(repo / name)["bytes"], sha256=digest))
    script_names = {Path(n).stem for n in CODE if n.startswith("scripts/") and n.endswith(".py")}
    for name, module in tuple(sys.modules.items()):
        origin = getattr(module, "__file__", None)
        if name.startswith("scripts.") or name in script_names:
            require(origin and Path(origin).resolve().is_relative_to(repo / "scripts"),
                    "imported script origin differs: " + name)
        if origin and Path(origin).resolve().is_relative_to(repo / "scripts"):
            require(str(Path(origin).resolve().relative_to(repo)) in config["code_sha256"],
                    "unpinned imported script: " + str(origin))


def read_config(path, sha, repo):
    require(__debug__, "optimized Python disables authority checks")
    config = offline.decode(offline.authenticated(regular_path(path), sha, 1 << 20))
    require(type(config) is dict and set(config) == set(EXPECTED) | EXTRA
            and all(canonical(config.get(k)) == canonical(v) for k, v in EXPECTED.items()),
            "configuration differs or authority pending")
    require(Path(__file__).resolve() == repo / OWN, "driver origin differs")
    require(config["refs"] == REFS, "immutable panel/protocol refs differ")
    offline.fields(config["execution_source"], "commit archive_sha256", "execution source")
    require(re.fullmatch("[0-9a-f]{40}", config["execution_source"]["commit"]), "full source commit required")
    offline.digest(config["execution_source"]["archive_sha256"])
    safe_key(config["generation_prefix"])
    check_code(config, repo)
    return config


def panel_authority(config, repo, old):
    data = {n: offline.decode(retained.archived.read_ref(repo, p)) for n, p in REFS.items()}
    a, p, f, v, root = (data[n] for n in (
        "authority.json", "panel.json", "root-freeze.json", "verification.json", "root_metadata_freeze"))
    require(root["schema"] == "borsuk-fixed48-root-metadata-freeze-v1"
            and root["metadata_qualified"] is True and root["actual_selection_count"] == 1
            and root["exact_interval_membership"] is root["shard_locator_mapping_verified"] is True
            and root["ground_truth_constructed"] is root["ann_measured"] is False
            and root["artifacts"] == {n: REFS[n] for n in ("authority.json", "panel.json", "root-freeze.json", "verification.json")}
            and root["protocol"] == REFS["selector_protocol"], "independent metadata freeze missing")
    if str(repo / "scripts") not in sys.path:
        sys.path.insert(0, str(repo / "scripts"))
    selector = retained.archived.isolated_module(repo, "select_cohere_fixed48_fresh64")
    # The selector recorded its sampling interpreter. Replay never samples and
    # authenticates that original value rather than substituting the worker's.
    selector.FIXED_PROTOCOL = dict(selector.FIXED_PROTOCOL, python=data["selector_protocol"]["python"])
    selector.load_protocol(repo, REFS["selector_protocol"]["path"], REFS["selector_protocol"]["sha256"])
    original, first, pins = selector.previous.authenticate(repo)
    second = selector.authenticate_top32(repo, original, first, pins)
    priors = [first, second]
    require(p["prior_panels_vector_hashes"] == a["prior_panels_vector_hashes"] == priors,
            "both authenticated prior64 hashsets required")
    require(p["selection"] == a["selection"] == selector.selection_spec(original, priors)
            and p["corpus"] == a["corpus"] == original["corpus"] == old["corpus"]
            and a["ordered_train_shards"] == original["ordered_train_shards"]
            and a["proofs"] == original["proofs"]
            and a["old_consumed_panel"] == original["old_consumed_panel"], "original population differs")
    for body in (a, p, f, v):
        require(canonical({k: body[k] for k in selector.SCOPE}) == canonical(selector.SCOPE),
                "metadata/historical FAIL scope differs")
    selected = p["selected"]
    require(len(selected) == len({r["source_ordinal"] for r in selected}) == 64
            and root["selected_sha256"] == f["selected_locators_sha256"] == v["selected_sha256"]
                == p["selected_sha256"] == retained.archived.prior.value_sha(selected)
            and f["authority_sha256"] == REFS["authority.json"]["sha256"]
            and f["panel_sha256"] == REFS["panel.json"]["sha256"], "fixed64 selection binding differs")
    for ordinal, row in enumerate(selected):
        source = selector.original.rank_to_ordinal(row["eligible_rank"], p["selection"]["eligible_intervals"])
        index, local = selector.original.locate(source, a["ordered_train_shards"])
        shard = a["ordered_train_shards"][index]
        require(row == dict(query_ordinal=ordinal, eligible_rank=row["eligible_rank"], source_ordinal=source,
            shard_ordinal=index, shard_key=shard["key"], shard_sha256=shard["sha256"],
            shard_bytes=shard["bytes"], local_row=local, embedding_column="emb"), "fixed locator differs")
    return dict(metadata_authority=a, panel=p)


def closed_qualification(config):
    """Validate the original closed campaign, independently of current Python."""
    q = config["qualification"]
    offline.fields(q, "directory files", "closed qualification")
    require(type(q["directory"]) is str and Path(q["directory"]).is_absolute(), "absolute qualification directory")
    directory = regular_path(q["directory"])
    require(directory.is_absolute() and directory.is_dir(), "qualification directory required")
    with qualification.execution_mode(fixed48=True):
        names = {*qualification.ARTIFACTS, "aws-reservation.json", "aws-closeout.json", "aws-terminal.json",
                 "collection-replay.json", "root-verification.json"}
        require(set(q["files"]) == names, "full qualification closure required")
        for name, pin in q["files"].items():
            safe_key(name); offline.fields(pin, "bytes sha256", "qualification pin")
            offline.integer(pin["bytes"], 1, 16 << 20, "qualification bytes"); offline.digest(pin["sha256"])
            path = regular_path(directory / name)
            require(path.is_file() and path.stat().st_size == pin["bytes"], "qualification length differs")
            authenticate(path, pin)
        reservation, terminal, closeout, collected, root = (read_json(directory / n) for n in (
            "aws-reservation.json", "aws-terminal.json", "aws-closeout.json", "collection-replay.json", "root-verification.json"))
        proof = read_json(directory / "source-qualification.json")
        require(reservation["schema"] == terminal["schema"] == qualification.SCHEMA
                and reservation["qualification"] == proof and closeout["state"] == "terminated"
                and terminal["instance_id"] in {n["instance_id"] for n in closeout["nodes"].values()}
                and terminal["phase"] == terminal["status"] == "complete"
                and type(terminal["exit_code"]) is type(terminal["original_exit_code"]) is int
                and terminal["exit_code"] == terminal["original_exit_code"] == 0, "original campaign not closed/qualified")
        for name, pattern in (("source_commit", "[0-9a-f]{40}"), ("source_archive_sha256", "[0-9a-f]{64}")):
            require(re.fullmatch(pattern, reservation[name]) and reservation[name] == terminal[name], "original source/archive binding")
        for key in qualification.TERMINAL_IDENTITIES:
            require(terminal[key] == proof[key], "original terminal identity: " + key)
        require(proof["schema"] == "borsuk-fixed48-implementation-gates-qualification-v1"
                and proof["actual_full_workspace_execution"] is False and proof["source_file_count"] == 399
                and len(proof["source_sha256"]) == 399
                and qualification.worker.source_identity(proof["source_sha256"]) == proof["source_identity_sha256"] == SOURCE_ID
                and set(proof["code_sha256"]) == set(qualification.CODE)
                and all(offline.digest(h) for h in proof["code_sha256"].values())
                and proof["code_identity_sha256"] == offline.sha(qualification.encoded(proof["code_sha256"]))
                and proof["artifact_roster_sha256"] == offline.sha(qualification.encoded(qualification.ARTIFACTS)),
                "frozen original code/native authority differs")
        require(set(terminal["artifacts"]) == set(qualification.ARTIFACTS)
                and all(terminal["artifacts"][n] == q["files"][n] for n in qualification.ARTIFACTS)
                and terminal["source_qualification_sha256"] == q["files"]["source-qualification.json"]["sha256"],
                "full16 original terminal body closure differs")
        old_config, manifest = read_json(directory / "config.json"), read_json(directory / "native-source-manifest.json")
        require(old_config["controller_authority_pending"] is False
                and all(canonical(old_config[k]) == canonical(v) for k, v in qualification.FIXED.items())
                and old_config["controller_code_sha256"] == proof["code_sha256"]
                and old_config["controller_source_commit"] == proof["controller_source_commit"]
                and old_config["native_source_manifest"] == proof["native_source_manifest"]
                and old_config["native_source_manifest"]["sha256"] == q["files"]["native-source-manifest.json"]["sha256"]
                and {k: old_config["native_source_manifest"][k] for k in ("bytes", "sha256")} == q["files"]["native-source-manifest.json"]
                and reservation["config_sha256"] == proof["config_sha256"] == q["files"]["config.json"]["sha256"],
                "original config/code-map/manifest bridge differs")
        require(manifest["schema"] == "borsuk-fixed48-native-source-manifest-v1"
                and manifest["source_sha256"] == proof["source_sha256"]
                and manifest["source_file_count"] == 399 and manifest["source_identity_sha256"] == SOURCE_ID
                and manifest["native_source_commit"] == proof["native_source_commit"]
                and manifest["candidate_delta_paths"] == proof["candidate_delta_paths"] == list(qualification.FIXED48_DELTA),
                "original full399 manifest binding differs")
        # Unlike replay(), this authenticates the original seven stages and
        # cgroup without comparing its frozen Python bodies to today's helpers.
        receipt = qualification.validate_receipt(directory, proof)
        result = dict(qualified=True, actual_full_workspace_execution=False, source_identity_sha256=SOURCE_ID,
                      exit_status=0, execution_kind="implementation-gates")
        require(collected == dict(result=result, terminal_sha256=q["files"]["aws-terminal.json"]["sha256"])
                and root["qualified"] is True and root["actual_full_workspace_execution"] is False
                and root["source_identity_sha256"] == SOURCE_ID and root["source_file_count"] == 399
                and root["authenticated_terminal_artifacts"] == 16
                and root["focused_tests_passed"] == sum(s["tests_run"] for s in receipt["stages"][:4])
                and root["instance_state_verified"] == "terminated" and root["new_performance_measurement"] is False,
                "original collection/root qualification differs")
        return proof


def native_authority(config, repo):
    original = closed_qualification(config)
    source = qualification.worker.source_hashes(repo)
    require(source == original["source_sha256"] and len(source) == 399
            and qualification.worker.source_identity(source) == SOURCE_ID, "exact combined native source differs")
    q = config["qualification"]
    binary = regular_path(q["directory"]) / "binaries/check_semantic_router_scorer"
    return dict(qualification=q, source_identity_sha256=SOURCE_ID, native_source_sha256=source,
        binary=descriptor(binary), scorer_source_sha256=source["crates/borsuk/src/bin/check_semantic_router_scorer.rs"],
        router_source_sha256=source["crates/borsuk/src/semantic_unit_router.rs"],
        original_execution_source={k: read_json(regular_path(q["directory"]) / "aws-reservation.json")[k]
                                   for k in ("source_commit", "source_archive_sha256")},
        native_qualification_passed=True, native_rebuilt=False)


def authorities(config, repo):
    r = config["retained"]
    offline.fields(r, "directory config complete", "retained authority")
    require(type(r["directory"]) is str and Path(r["directory"]).is_absolute(), "absolute retained directory")
    directory = regular_path(r["directory"])
    require(directory.is_absolute() and directory.is_dir()
            and r["config"]["path"] == str(directory / "config.json")
            and r["complete"]["path"] == str(directory / "COMPLETE.json"), "retained paths differ")
    read_pointer(r["config"]); read_pointer(r["complete"])
    # Authenticate the entire 30-body retained closure; this never invokes a builder.
    marker = retained.replay(Path(r["config"]["path"]), r["config"]["sha256"], repo, directory)
    old = offline.decode(retained.archived.read_ref(repo, retained.FIXED["archived_config"]))
    data = panel_authority(config, repo, old)
    proof = native_authority(config, repo)
    data.update(old=old, retained_directory=directory, retained_marker=marker)
    return data, proof


def dependencies(config, repo, data):
    helper, native = retained.dependencies(config, repo)
    helper.np = importlib.import_module("numpy")
    helper.pa = helper.shared.pa
    helper.pq = importlib.import_module("pyarrow.parquet")
    helper.previous_unit = importlib.import_module("scripts.v271_fresh_frontier").unit
    versions = data["old"]["versions"]
    require(versions == helper.VERSIONS and helper.np.__version__ == versions["numpy"]
            and helper.pa.__version__ == versions["pyarrow"]
            and helper.shared.NORMALIZATION == data["old"]["normalization"], "original numeric pins differ")
    check_code(config, repo)
    return helper, native


def duplicate_audit(queries, helper, data, out, accounting):
    raw, normalized = helper.shared.vector_hashes(queries)
    priors = data["panel"]["prior_panels_vector_hashes"]
    require(len(priors) == 2, "both prior64 panels required")
    for previous in priors:
        require(not set(raw).intersection(previous["raw_sha256"])
                and not set(normalized).intersection(previous["normalized_sha256"]),
                "raw/f64 duplicate of consumed prior64; STOP without replacement")
    report = helper.audit(queries, out, accounting=accounting)
    report.update(schema=SCHEMA + "-duplicates", both_prior64_rows_audited=128,
                  prior_panels_vector_hashes=priors, selected_sha256=data["panel"]["selected_sha256"])
    return report


def materialize(source, target):
    source, target = regular_path(source), regular_path(target)
    require(source.is_file() and not target.exists(), "fresh regular materialization required")
    target.parent.mkdir(parents=True, exist_ok=True)
    # Large retained payloads share storage; a cross-device link fails before scoring.
    if source.stat().st_size > 1 << 20:
        os.link(source, target)
    else:
        publish(target, source.read_bytes())
    with target.open("rb") as stream:
        os.fsync(stream.fileno())
    authenticate(target, identity(source))
    return descriptor(target)


def sync_directory(directory):
    fd = os.open(directory, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def rehost(config, data, native, out):
    directory = data["retained_directory"]
    root_path = directory / "generation/manifest.json"
    original = read_json(root_path)
    store = out / "store"; prefix = config["generation_prefix"]
    sq8_key, canonical_key = original["sq8_object_key"], original["canonical"]["object_key"]
    for key, digest in ((sq8_key, original["sq8_object_sha256"]), (canonical_key, original["canonical"]["sha256"])):
        safe_key(key)
        require(key.split("/")[-2:] == ["objects", digest], "content-addressed object key differs")
    files = {}
    for name in ("source.raw", "source-order.u64", "source-root.json"):
        materialize(directory / name, out / name)
    for name in retained.GENERATION_FILES:
        if name != "manifest.json":
            key = safe_key(prefix + "/" + name)
            files[key] = materialize(directory / "generation" / name, store / key)
    for name, key in (("source-sq8.bin", sq8_key), ("generation/canonical.bin", canonical_key)):
        require(key not in files, "object alias collision")
        files[key] = materialize(directory / name, store / key)
    head = native.local_head(store / sq8_key)
    retained.check_head(head, dict(corpus=data["old"]["corpus"]))
    fresh = copy.deepcopy(original); fresh["sq8_etag"] = head["etag"]
    publish(out / "original-generation-root.json", root_path.read_bytes())
    key = prefix + "/manifest.json"
    files[key] = publish(store / key, fresh)
    for directory_path in sorted((p for p in store.rglob("*") if p.is_dir()), reverse=True):
        sync_directory(directory_path)
    sync_directory(store); sync_directory(out)
    receipt = dict(schema=SCHEMA + "-rehost", old_root=descriptor(out / "original-generation-root.json"),
        new_root=files[key], delta=dict(sq8_etag=dict(old=original["sq8_etag"], new=head["etag"])),
        actual_sq8_head=head, sq8_object_key=sq8_key, canonical_object_key=canonical_key,
        canonical_alias=prefix + "/canonical.bin", payloads_unchanged=True, files=files)
    publish(out / "local-sq8-head.json", head)
    publish(out / "rehost.json", receipt)
    check_rehost(config, out, native, receipt)
    return receipt


def check_rehost(config, out, native, receipt):
    old = offline.decode(read_pointer(receipt["old_root"]))
    new = offline.decode(read_pointer(receipt["new_root"]))
    expected = copy.deepcopy(old); expected["sq8_etag"] = receipt["actual_sq8_head"]["etag"]
    require(new == expected and receipt["delta"] == dict(sq8_etag=dict(old=old["sq8_etag"], new=new["sq8_etag"]))
            and receipt["payloads_unchanged"] is True, "only outer v8 SQ8 ETag may change")
    for key, pin in receipt["files"].items():
        safe_key(key)
        require(pin["path"] == str(out / "store" / key), "local ObjectStore binding differs")
        authenticate(regular_path(pin["path"]), pin)
    sq8 = out / "store" / safe_key(new["sq8_object_key"])
    require(native.local_head(sq8) == receipt["actual_sq8_head"]
            and receipt["new_root"]["path"] == str(out / "store" / config["generation_prefix"] / "manifest.json")
            and identity(out / "store" / receipt["canonical_alias"])["sha256"] == new["canonical"]["sha256"]
            and identity(out / "store" / new["canonical"]["object_key"])["sha256"] == new["canonical"]["sha256"],
            "actual local ETag/canonical alias differs")


def scorer_config(config, out, rehost_receipt):
    return dict(schema="borsuk-semantic-router-scorer-config-v3", dataset="CoHere", profile="fresh1m",
        rows=config["rows"], dimensions=config["dimensions"], first=0, count=64,
        store_root=str(out / "store"), generation_prefix=config["generation_prefix"],
        generation_root_sha256=rehost_receipt["new_root"]["sha256"], scratch_parent=str(out / "native-scratch"),
        requests=descriptor(out / "requests.jsonl"), order=descriptor(out / "source-order.u64"),
        max_memory_bytes=config["scorer_memory_bytes"])


def resources(accounting, config, passed, cleanup):
    return dict(retained.resource_report(accounting, passed, cleanup), schema=SCHEMA + "-resources",
        scorer_invocations=accounting.scorer_invocations, oracle_invocations=accounting.oracle_invocations,
        build_invocations=0, query_or_truth_used=accounting.oracle_invocations > 0,
        physical_s3_measured=False, cold_http_measured=False)


def check_resources(report, config, oracle_count):
    limits = config["limits"]
    require(report["passed"] is True and report["build_invocations"] == 0 and report["scorer_invocations"] == 1
            and report["oracle_invocations"] == oracle_count and report["resource_failure"] is None
            and report["prospective_preparation_limits"] == limits
            and report["transport_accounting_complete"] is True and report["aws_errors"] == 0
            and report["aws_max_attempts"] == 1 and report["tasks_max"] == config["tasks_max"]
            and report["failure_cleanup"]["process_cleanup"] is report["failure_cleanup"]["native_scratch_empty"] is True
            and all(report["thread_environment"][n] == "2" for n in retained.THREAD_ENV)
            and all(int(report["memory_events"].get(n, 0)) == 0 for n in ("oom", "oom_kill", "max"))
            and 0 <= report["process_max_rss_kib"] * 1024 <= limits["memory_bytes"]
            and 0 <= report["child_max_rss_kib"] * 1024 <= limits["memory_bytes"]
            and 0 <= report["actual_scratch_bytes"] <= report["peak_scratch_bytes"] <= limits["scratch_bytes"]
            and 0 <= report["wall_seconds"] <= config["stage_limit_seconds"]
            and sum(report["http_request_dispatch_attempts"].values()) <= limits["max_requests"], "driver resources failed")
    group = report["cgroup"]
    if group is not None:
        quota, period = group["cpu.max"].split()
        require(int(group["memory.max"]) == limits["memory_bytes"]
                and 0 <= int(group["memory.peak"]) <= limits["memory_bytes"]
                and group["memory.swap.max"] == group["memory.swap.peak"] == "0"
                and quota != "max" and 0 < int(quota) <= limits["cpu"] * int(period)
                and report["actual_tasks_limit"] != "max"
                and 0 < int(report["actual_tasks_limit"]) <= config["tasks_max"], "worker cgroup failed")


def output_roster(config, root):
    return {*OUTPUT_FILES, *("store/" + config["generation_prefix"] + "/" + n for n in retained.GENERATION_FILES),
            "store/" + safe_key(root["sq8_object_key"]), "store/" + safe_key(root["canonical"]["object_key"])}


@contextmanager
def phase(accounting, sequence, name):
    sequence.append(dict(phase=name, ordinal=len(sequence), started_ns=time.monotonic_ns()))
    with retained.phase(accounting, name):
        yield
    sequence[-1]["finished_ns"] = time.monotonic_ns()


def measurement_receipt(config, proof, out, c, c_pin, result, accounting, cleanup):
    require(result["exit_status"] == 0 and type(result["exit_status"]) is int
            and result["process_cleanup"] is True and cleanup["process_cleanup"] is True
            and not any((out / "native-scratch").iterdir())
            and result["process_peak_rss_kib"] * 1024 <= config["scorer_memory_bytes"]
            and 0 <= result["wall_seconds"] <= config["stage_limit_seconds"], "scorer execution/RSS/cleanup failed")
    path = regular_path(out / "records.jsonl")
    require(0 < path.stat().st_size <= offline.CAPS["measurements"], "measurement cap")
    with path.open("rb") as stream:
        os.fchmod(stream.fileno(), 0o444); os.fsync(stream.fileno())
    inputs = dict(scorer_config=c_pin, measurements=descriptor(path), order=c["order"])
    receipt = dict(schema=offline.RECEIPT_SCHEMA, **inputs, requests=c["requests"],
        binary_sha256=proof["binary"]["sha256"], scorer_source_sha256=proof["scorer_source_sha256"],
        router_source_sha256=proof["router_source_sha256"], generation_root_sha256=c["generation_root_sha256"],
        generation_prefix=c["generation_prefix"], exit_status=0, resource_gate_passed=True,
        cleanup_complete=True, native_qualification_passed=True, measurement_sealed_before_truth=True)
    offline.validate_receipt(receipt, c, inputs)
    queries, _, _ = offline.validate_measurements(read_pointer(inputs["measurements"], offline.CAPS["measurements"]), c, inputs, receipt)
    inverse = offline.inverse_order(read_pointer(c["order"], offline.CAPS["order"]), c["rows"])
    for q in queries:
        require(all(any(r["start"] <= inverse[i] * (c["dimensions"] + 12) < r["end"]
                        for r in q["ranges"]) for i in q["returned_ids"]), "returned ID/range binding")
    accounting.checkpoint()
    report = resources(accounting, config, True, cleanup)
    check_resources(report, config, 0)
    publish(out / "measurement-resources.json", report)
    publish(out / "measurement-cleanup.json", cleanup)
    require(not (out / "truth.u32").exists() and not (out / "truth.i64").exists()
            and accounting.oracle_invocations == 0, "truth before complete measurement seal")
    pin = publish(out / "measurement-receipt.json", receipt)
    sync_directory(out)
    return pin


def verify_seal(receipt_pin, c, c_pin):
    receipt = offline.decode(read_pointer(receipt_pin, 65536))
    inputs = {k: receipt[k] for k in ("scorer_config", "measurements", "order")}
    require(inputs["scorer_config"] == c_pin, "sealed scorer config changed")
    read_pointer(c_pin, 65536)
    offline.validate_receipt(receipt, c, inputs)
    offline.validate_measurements(read_pointer(inputs["measurements"], offline.CAPS["measurements"]), c, inputs, receipt)
    return receipt


def write_truth(helper, out, truth):
    require(truth.shape == (64, 100) and truth.dtype == helper.np.dtype("<u4"), "oracle geometry/dtype differs")
    publish(out / "truth.u32", truth.tobytes())
    publish(out / "truth.i64", truth.astype("<i8").tobytes())
    helper.shared.check_outputs(out)


def run(config_path, sha, repo, out):
    started = time.monotonic()
    repo, out = regular_path(repo), regular_path(out)
    config = read_config(config_path, sha, repo)
    data, proof = authorities(config, repo)
    require(not out.exists() and not out.is_relative_to(repo), "new external output required")
    group = retained.admission(config)
    helper, native = dependencies(config, repo, data)
    out.mkdir()
    accounting = retained.archived.Accounting(config, out, group)
    accounting.stage_resources = {}; accounting.scorer_invocations = 0
    accounting.admission = dict(cgroup_path=None if group is None else str(group),
        prospective_limits=config["limits"], tasks_max=config["tasks_max"],
        thread_environment={n: os.environ.get(n) for n in retained.THREAD_ENV})
    cleanup = dict(schema=SCHEMA + "-cleanup", process_cleanup=True, native_scratch_empty=True,
                   diagnostics_preserved=True, retained=True)
    sequence = []
    try:
        with accounting.stage("fixed48_fresh_falsifier"):
            accounting.deadline = min(accounting.deadline, started + config["stage_limit_seconds"])
            signal.setitimer(signal.ITIMER_REAL, accounting.remaining())
            publish(out / "config.json", Path(config_path).read_bytes())
            publish(out / "source-qualification.json", proof)
            publish(out / "panel.json", retained.archived.read_ref(repo, REFS["panel.json"]))
            with phase(accounting, sequence, "authenticated_file_store"):
                rehost_receipt = rehost(config, data, native, out)
                publish(out / "sq8-ordinal-check.json", native.ordinal_check(
                    out / "store" / rehost_receipt["sq8_object_key"], out / "source-order.u64",
                    rows=config["rows"], dimensions=config["dimensions"]))
            extraction = dict(data["old"], limits=config["limits"], region=config["region"])
            with helper.accounted_helpers(accounting):
                with phase(accounting, sequence, "fixed_published_rows"):
                    helper.download(extraction, extraction["consumed_queries"], out / "consumed-queries.raw", accounting)
                    queries, sources = helper.acquire_vectors(extraction, data, out, accounting)
                with phase(accounting, sequence, "raw_f64_duplicate_gate"):
                    audit = duplicate_audit(queries, helper, data, out, accounting)
                    audit.update(authenticated_query_sources=sources, config_sha256=sha,
                        inputs={n: descriptor(out / n) for n in ("source.raw", "source-order.u64", "source-root.json",
                            "consumed-queries.raw", "prior-queries.raw", "test-queries.raw")})
                    audit_pin = publish(out / "duplicate-audit.json", audit)
                    retained.archived.write_queries(out, queries)
                    input_pins = dict(audit["inputs"], **{n: descriptor(out / n) for n in ("queries.raw", "requests.jsonl")})
                    publish(out / "input-hashes.json", input_pins)
            with phase(accounting, sequence, "one_truth_free_v3_scorer"):
                require(accounting.scorer_invocations == accounting.oracle_invocations == 0, "one-pass invocation guard")
                (out / "native-scratch").mkdir()
                c = scorer_config(config, out, rehost_receipt)
                c_pin = publish(out / "scorer-config.json", c)
                require(c_pin["bytes"] <= 65536, "scorer config cap")
                # The executable is private; payload hardlinks remain unchanged.
                publish(out / "scorer", read_pointer(proof["binary"], 16 << 20))
                (out / "scorer").chmod(0o500)
                authenticate(out / "scorer", proof["binary"])
                accounting.scorer_invocations += 1; cleanup["process_cleanup"] = False
                signal.setitimer(signal.ITIMER_REAL, 0); accounting.native_active = True
                try:
                    result = native.run_process([str(out / "scorer"), c_pin["path"], c_pin["sha256"],
                        str(out / "records.jsonl")], out / "score.log", accounting.remaining(), out / "score-resources.txt")
                finally:
                    # The authenticated runner reaps its original group in finally.
                    accounting.native_active = False; cleanup["process_cleanup"] = True
                    cleanup["native_scratch_empty"] = not any((out / "native-scratch").iterdir())
                publish(out / "score-resources.json", result)
                signal.setitimer(signal.ITIMER_REAL, accounting.remaining())
            with phase(accounting, sequence, "all64_measurement_and_receipt_sealed"):
                receipt_pin = measurement_receipt(config, proof, out, c, c_pin, result, accounting, cleanup)
                sealed_sequence_pin = publish(out / "measurement-sequence.json", sequence)
            with phase(accounting, sequence, "recheck_all_authority_before_truth"):
                require(read_config(config_path, sha, repo) == config, "driver config drift")
                data_again, proof_again = authorities(config, repo)
                require(proof_again == proof and data_again["retained_marker"] == data["retained_marker"], "launch authority drift")
                check_rehost(config, out, native, rehost_receipt)
                for name, pin in input_pins.items():
                    authenticate(out / name, pin)
                read_pointer(audit_pin); read_pointer(sealed_sequence_pin)
                require(queries.tobytes() == (out / "queries.raw").read_bytes(), "raw query memory/file drift")
                verify_seal(receipt_pin, c, c_pin)
                check_resources(read_json(out / "measurement-resources.json"), config, 0)
                require(read_json(out / "measurement-cleanup.json") == cleanup
                        and accounting.oracle_invocations == 0, "measurement cleanup/chronology drift")
            with phase(accounting, sequence, "one_exhaustive_f64_gt100"):
                helper.shared.oracle_self_check()
                accounting.oracle_invocations += 1
                truth = helper.shared.oracle(out / "source.raw", queries, config["rows"])
                write_truth(helper, out, truth)
                publish(out / "oracle.json", dict(schema=SCHEMA + "-oracle", passed=True,
                    oracle_self_check_passed=True, ground_truth_constructions=1, rows=config["rows"], queries=64, k=100,
                    distance="1-dot of f64-normalized original f32", tie="signed source ordinal ascending",
                    normalization=data["old"]["normalization"], versions=data["old"]["versions"],
                    truth_id_space="source ordinal", widening="LEu32 to LEi64, unchanged values",
                    source_raw=descriptor(out / "source.raw"), queries_raw=descriptor(out / "queries.raw"),
                    measurement_receipt=receipt_pin, measurement_sequence=sealed_sequence_pin,
                    duplicate_audit=audit_pin, truth_u32=descriptor(out / "truth.u32"), truth_i64=descriptor(out / "truth.i64")))
            with phase(accounting, sequence, "six_stage_offline_evaluation"):
                verify_seal(receipt_pin, c, c_pin)
                evaluation = dict(schema=offline.CONFIG_SCHEMA, inputs=dict(
                    scorer_config=c_pin, measurements=descriptor(out / "records.jsonl"), order=c["order"],
                    truth=descriptor(out / "truth.i64"), measurement_receipt=receipt_pin))
                e_pin = publish(out / "offline-config.json", evaluation)
                report = offline.evaluate(e_pin["path"], e_pin["sha256"])
                publish(out / "offline-result.json", offline.canonical(report))
                require(read_config(config_path, sha, repo) == config, "final source/config drift")
                require(native_authority(config, repo) == proof, "final combined qualification/source drift")
                for name, pin in input_pins.items():
                    authenticate(out / name, pin)
                check_rehost(config, out, native, rehost_receipt)
            accounting.checkpoint()
            with phase(accounting, sequence, "retained_outputs_sync"):
                for name, path in retained.file_paths(out).items():
                    if name != "scorer":
                        path.chmod(0o444)
                    with path.open("rb") as stream:
                        os.fsync(stream.fileno())
                files = retained.roster(out)
        require(accounting.oracle_invocations == accounting.scorer_invocations == 1, "one-pass final counts")
        cleanup["native_scratch_empty"] = not any((out / "native-scratch").iterdir())
        require(cleanup["native_scratch_empty"], "native scratch cleanup failed")
        publish(out / "cleanup.json", cleanup)
        final_resources = resources(accounting, config, True, cleanup)
        final_resources["wall_seconds"] = time.monotonic() - started
        check_resources(final_resources, config, 1)
        publish(out / "resources.json", final_resources)
        publish(out / "stage-sequence.json", sequence)
        decision = dict(schema=SCHEMA + "-decision", config_sha256=sha, execution_status="SUCCESS",
            scientific_status=report["scientific_status"], failure_class=report["failure_class"],
            first_crossing_stage=report["first_crossing_stage"], eligible_for_cold_measurement=report["eligible_for_cold_measurement"],
            build_invocations=0, scorer_invocations=1, oracle_invocations=1, execution_source=config["execution_source"],
            resource_gate_passed=True, cleanup_complete=True, native_qualification_passed=True,
            offline_result=descriptor(out / "offline-result.json"), measurement_receipt=receipt_pin,
            physical_s3_measured=False, cold_http_measured=False, launch_authority=False)
        publish(out / "decision.json", decision)
        for name in ("cleanup.json", "resources.json", "stage-sequence.json", "decision.json"):
            files[name] = {k: identity(out / name)[k] for k in ("bytes", "sha256")}
        require(set(files) == output_roster(config, read_json(rehost_receipt["new_root"]["path"])), "exact driver output closure differs")
        require(time.monotonic() - started <= config["stage_limit_seconds"], "whole driver deadline exceeded")
        marker = dict(schema=SCHEMA + "-complete", config_sha256=sha, execution_status="SUCCESS",
            scientific_status=report["scientific_status"], files=files, roster_sha256=retained.archived.prior.value_sha(files))
        publish(out / "COMPLETE.json", marker)
        return decision
    except BaseException as error:
        (out / "COMPLETE.json").unlink(missing_ok=True)
        if not (out / "failure-resources.json").exists():
            publish(out / "failure-resources.json", resources(accounting, config, False, cleanup))
        publish(out / "failure-cleanup.json", cleanup)
        publish(out / "failure.json", dict(schema=SCHEMA + "-failure", execution_status="FAIL", scientific_status="INVALID",
            error_type=type(error).__name__, error=str(error), scorer_invocations=accounting.scorer_invocations,
            oracle_invocations=accounting.oracle_invocations, diagnostics_preserved=True, cleanup=cleanup, stages=sequence))
        raise


def replay(config_path, sha, repo, out):
    repo, out = regular_path(repo), regular_path(out)
    config = read_config(config_path, sha, repo)
    data, proof = authorities(config, repo)
    marker = read_json(out / "COMPLETE.json")
    require(marker["schema"] == SCHEMA + "-complete" and marker["config_sha256"] == sha
            and marker["execution_status"] == "SUCCESS", "complete scientific-driver seal required")
    files = retained.roster(out); files.pop("COMPLETE.json")
    root = read_json(out / "store" / config["generation_prefix"] / "manifest.json")
    require(set(files) == output_roster(config, root) and files == marker["files"]
            and retained.archived.prior.value_sha(files) == marker["roster_sha256"], "driver body/roster drift")
    require((out / "config.json").read_bytes() == Path(config_path).read_bytes()
            and read_json(out / "source-qualification.json") == proof, "saved source/config qualification drift")
    native = importlib.import_module("scripts.run_native_semantic_1m_quality")
    check_code(config, repo)
    check_rehost(config, out, native, read_json(out / "rehost.json"))
    cleanup = read_json(out / "cleanup.json")
    require(cleanup["process_cleanup"] is cleanup["native_scratch_empty"] is True
            and not any((out / "native-scratch").iterdir()), "replay cleanup differs")
    check_resources(read_json(out / "resources.json"), config, 1)
    check_resources(read_json(out / "measurement-resources.json"), config, 0)
    c_pin = descriptor(out / "scorer-config.json"); c = read_json(c_pin["path"])
    require(c == scorer_config(config, out, read_json(out / "rehost.json")), "truth-free scorer configuration drift")
    receipt_pin = descriptor(out / "measurement-receipt.json")
    verify_seal(receipt_pin, c, c_pin)
    oracle = read_json(out / "oracle.json")
    require(oracle["passed"] is oracle["oracle_self_check_passed"] is True
            and oracle["ground_truth_constructions"] == 1 and oracle["measurement_receipt"] == receipt_pin
            and oracle["truth_u32"] == descriptor(out / "truth.u32")
            and oracle["truth_i64"] == descriptor(out / "truth.i64")
            and oracle["source_raw"] == descriptor(out / "source.raw")
            and oracle["queries_raw"] == descriptor(out / "queries.raw")
            and oracle["duplicate_audit"] == descriptor(out / "duplicate-audit.json")
            and oracle["measurement_sequence"] == descriptor(out / "measurement-sequence.json")
            and oracle["versions"] == data["old"]["versions"]
            and oracle["normalization"] == data["old"]["normalization"], "sole oracle provenance differs")
    sequence = read_json(out / "stage-sequence.json")
    expected_phases = ("authenticated_file_store", "fixed_published_rows", "raw_f64_duplicate_gate",
        "one_truth_free_v3_scorer", "all64_measurement_and_receipt_sealed", "recheck_all_authority_before_truth",
        "one_exhaustive_f64_gt100", "six_stage_offline_evaluation", "retained_outputs_sync")
    require(len(sequence) == len(expected_phases), "complete driver chronology required")
    previous = 0
    for ordinal, (stage, name) in enumerate(zip(sequence, expected_phases)):
        offline.fields(stage, "phase ordinal started_ns finished_ns", "driver stage")
        require(stage["phase"] == name and type(stage["ordinal"]) is int and stage["ordinal"] == ordinal,
                "ordered stage identity")
        start = offline.integer(stage["started_ns"], previous, offline.MAX_INT, "stage start")
        previous = offline.integer(stage["finished_ns"], start, offline.MAX_INT, "stage finish")
    sealed_sequence = copy.deepcopy(sequence[:5]); sealed_sequence[-1].pop("finished_ns")
    require(sealed_sequence == read_json(out / "measurement-sequence.json"), "measurement-before-GT chronology drift")
    narrow = (out / "truth.u32").read_bytes(); wide = (out / "truth.i64").read_bytes()
    require(len(narrow) == 25600 and len(wide) == 51200
            and all(a == b for (a,), (b,) in zip(struct.iter_unpack("<I", narrow), struct.iter_unpack("<q", wide))), "LEi64 widening drift")
    e_pin = descriptor(out / "offline-config.json")
    report = offline.run(e_pin["path"], e_pin["sha256"], out / "offline-result.json", replay=True)
    decision = read_json(out / "decision.json")
    require(decision["execution_status"] == "SUCCESS" and decision["scientific_status"] == marker["scientific_status"] == report["scientific_status"]
            and decision["first_crossing_stage"] == report["first_crossing_stage"]
            and decision["failure_class"] == report["failure_class"], "scientific classification replay differs")
    return decision


def self_check():
    """Real driver/order/seal/reducer logic; only external/data seams are mocked."""
    from array import array
    from contextlib import ExitStack
    import subprocess
    from unittest.mock import Mock

    resource.setrlimit(resource.RLIMIT_AS, (256 << 20, 256 << 20))
    signal.alarm(55)
    started = time.monotonic(); checks = 0

    def rejected(call):
        nonlocal checks
        try:
            call()
        except (ValueError, OSError, AssertionError, subprocess.TimeoutExpired):
            checks += 1
            return
        raise AssertionError("negative check admitted")

    assert safe_key("tenant/g1/objects/abc") == "tenant/g1/objects/abc"
    for value in ("../escape", "/absolute", "a//b", "a/./b", "a/%2f/b", "a/é", "a\\b"):
        rejected(lambda: safe_key(value))
    repo = Path(__file__).resolve().parents[1]
    frozen = dict(EXPECTED, refs=REFS, code_sha256={n: identity(repo / n)["sha256"] for n in CODE},
        execution_source=dict(commit="a" * 40, archive_sha256="b" * 64), generation_prefix="test/generation",
        qualification={}, retained={})
    with tempfile.TemporaryDirectory(prefix="fixed48-driver-check-") as temporary:
        base = Path(temporary)
        path = base / "config.json"; path.write_bytes(canonical(frozen))
        config_sha = identity(path)["sha256"]
        require(read_config(path, config_sha, repo) == frozen, "frozen config positive")
        checks += 1
        rejected(lambda: read_config(path, "0" * 64, repo))
        for name in ("authority_pending", "code_sha256", "refs", "execution_source", "generation_prefix", "limits", "extra"):
            changed = copy.deepcopy(frozen)
            if name == "code_sha256": changed[name].pop(OWN)
            elif name == "refs": changed[name]["panel.json"]["sha256"] = "0" * 64
            elif name == "execution_source": changed[name]["commit"] = "pending"
            elif name == "generation_prefix": changed[name] = "../escape"
            elif name == "limits": changed[name]["memory_bytes"] = 8 << 30
            else: changed[name] = True
            path.write_bytes(canonical(changed))
            rejected(lambda: read_config(path, identity(path)["sha256"], repo))
        path.write_bytes(canonical(frozen))
        wrong = copy.deepcopy(frozen); wrong["code_sha256"][OWN] = "0" * 64
        rejected(lambda: check_code(wrong, repo))
        with patch.dict(sys.modules, {"scripts.unpinned": SimpleNamespace(__file__=str(repo / "scripts/unpinned.py"))}):
            rejected(lambda: check_code(frozen, repo))
        with patch.dict(sys.modules, {"scripts.shadow": SimpleNamespace(__file__=str(base / "shadow.py"))}):
            rejected(lambda: check_code(frozen, repo))
        old = offline.decode(retained.archived.read_ref(repo, retained.FIXED["archived_config"]))
        metadata = panel_authority(frozen, repo, old)
        require(len(metadata["panel"]["selected"]) == 64, "one sealed real metadata roster, no vector bodies")
        check_code(frozen, repo); checks += 1
        broken_refs = copy.deepcopy(REFS); broken_refs["panel.json"]["sha256"] = "0" * 64
        with patch.dict(REFS, broken_refs):
            rejected(lambda: panel_authority(frozen, repo, old))

        # Original campaign shapes exercise the actual seven-stage receipt and
        # cgroup validators. Only the synthetic native source identity is mocked.
        qdir = base / "qualification"; qdir.mkdir()
        source_map = {"file" + str(i): "1" * 64 for i in range(397)}
        source_map.update({"crates/borsuk/src/bin/check_semantic_router_scorer.rs": "2" * 64,
                           "crates/borsuk/src/semantic_unit_router.rs": "3" * 64})
        qresult = dict(qualified=True, actual_full_workspace_execution=False, source_identity_sha256=SOURCE_ID,
                       exit_status=0, execution_kind="implementation-gates")
        def put(name, value):
            target = qdir / name; target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(value if type(value) is bytes else canonical(value))
        def pin(name): return {k: identity(qdir / name)[k] for k in ("bytes", "sha256")}
        with qualification.execution_mode(fixed48=True):
            qnames = [*qualification.ARTIFACTS, "aws-reservation.json", "aws-closeout.json", "aws-terminal.json",
                      "collection-replay.json", "root-verification.json"]
            original_code = dict.fromkeys(qualification.CODE, "4" * 64)
            manifest = dict(schema="borsuk-fixed48-native-source-manifest-v1", source_sha256=source_map,
                source_file_count=399, source_identity_sha256=SOURCE_ID, native_source_commit="b" * 40,
                candidate_delta_paths=list(qualification.FIXED48_DELTA))
            put("native-source-manifest.json", manifest)
            manifest_pin = dict(pin("native-source-manifest.json"), path="original/native-source-manifest.json")
            put("config.json", dict(qualification.FIXED, controller_authority_pending=False,
                controller_code_sha256=original_code, controller_source_commit="c" * 40,
                native_source_manifest=manifest_pin))
            original_proof = dict(schema="borsuk-fixed48-implementation-gates-qualification-v1",
                actual_full_workspace_execution=False, config_path=str(qualification.CONFIG),
                campaign_schema=qualification.SCHEMA, config_sha256=pin("config.json")["sha256"],
                source_sha256=source_map, source_file_count=399, source_identity_sha256=SOURCE_ID,
                code_sha256=original_code, code_identity_sha256=offline.sha(qualification.encoded(original_code)),
                artifact_roster_sha256=offline.sha(qualification.encoded(qualification.ARTIFACTS)),
                native_source_manifest=manifest_pin, native_source_manifest_sha256=manifest_pin["sha256"],
                native_source_commit=manifest["native_source_commit"], controller_source_commit="c" * 40,
                candidate_delta_paths=manifest["candidate_delta_paths"], command=qualification.FIXED["command"],
                environment=qualification.FIXED["environment"], execution_kind="implementation-gates",
                awscli_version="synthetic-never-executed", awscli_sha256="5" * 64)
            put("source-qualification.json", original_proof)
            for name in ("source-before.json", "source-after.json"): put(name, source_map)
            lines = []
            for index, (name, command) in enumerate(qualification.FIXED48_STAGES):
                stage = dict(schema=qualification.FIXED48_STAGE_SCHEMA, stage=name, command=command,
                    started_at=f"2026-10-02T00:00:0{index}Z", finished_at=None, exit_status=None,
                    gate_status=None, tests_run=None, required_test_passes=None)
                lines.append(canonical(stage))
                names = qualification.FIXED48_REQUIRED_TESTS.get(name, ())
                lines.extend(("test " + test + " ... ok").encode() for test in names)
                count = (6, 4, 6, 12)[index] if index < 4 else None
                if index < 4:
                    lines.append(f"test result: ok. {count} passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s".encode())
                stage.update(finished_at=stage["started_at"], exit_status=0, gate_status=0,
                    tests_run=count, required_test_passes=dict.fromkeys(names, 1))
                lines.append(canonical(stage))
            put("test.log", b"\n".join(lines) + b"\n")
            counters = dict(observer_pid=1, process_ids=[1], **{"memory.max": str(8 << 30),
                "memory.peak": str(1 << 20), "memory.swap.max": "0", "memory.swap.peak": "0",
                "cpu.max": "200000 100000", "cpu.stat": "usage_usec 1\n", "pids.max": "512",
                "pids.current": "1", "pids_peak": "1", "memory.events": "max 5364\noom 0\noom_kill 0\noom_group_kill 0\n",
                "memory.swap.events": "high 0\nmax 0\nfail 0\n", "pids.events": "max 0\n"})
            put("workspace-cgroup.json", dict(closed=True, before=counters, after=counters))
            for name in ("cpu.txt", "rustc-version.txt", "cargo-version.txt", "test-resources.txt", "run-closed.log"):
                put(name, b"synthetic original campaign; no processes executed\n")
            for name in qualification.RELEASE_ARTIFACTS: put(name, b"\x7fELFsynthetic-never-executed")
            original_receipt = dict(schema=qualification.RECEIPT_SCHEMA, qualified=True, exit_status=0,
                gate_status=0, command_started=True, command_completed=True, source_unchanged=True,
                actual_full_workspace_execution=False, command=original_proof["command"],
                environment=original_proof["environment"], execution_kind="implementation-gates",
                source_sha256=source_map, qualification_sha256=pin("source-qualification.json")["sha256"],
                **{k: original_proof[k] for k in qualification.TERMINAL_IDENTITIES},
                artifacts={n: pin(n) for n in qualification.ARTIFACTS if n not in ("workspace-receipt.json", "run-closed.log")},
                stages=qualification.validate_bounded_publication_stages(qdir / "test.log", fixed48=True))
            put("workspace-receipt.json", original_receipt)
            original_reservation = dict(schema=qualification.SCHEMA, qualification=original_proof,
                source_commit="a" * 40, source_archive_sha256="b" * 64, config_sha256=original_proof["config_sha256"])
            put("aws-reservation.json", original_reservation)
            original_terminal = dict(schema=qualification.SCHEMA, instance_id="synthetic-instance",
                phase="complete", status="complete", exit_code=0, original_exit_code=0,
                source_commit=original_reservation["source_commit"], source_archive_sha256=original_reservation["source_archive_sha256"],
                **{k: original_proof[k] for k in qualification.TERMINAL_IDENTITIES},
                source_qualification_sha256=pin("source-qualification.json")["sha256"],
                artifacts={n: pin(n) for n in qualification.ARTIFACTS})
            put("aws-terminal.json", original_terminal)
            put("aws-closeout.json", dict(state="terminated", nodes={"worker": dict(instance_id="synthetic-instance")}))
            put("collection-replay.json", dict(result=qresult, terminal_sha256=pin("aws-terminal.json")["sha256"]))
            root_receipt = dict(qualified=True, actual_full_workspace_execution=False, source_identity_sha256=SOURCE_ID,
                source_file_count=399, authenticated_terminal_artifacts=16, focused_tests_passed=28,
                instance_state_verified="terminated", new_performance_measurement=False)
            put("root-verification.json", root_receipt)
        qfiles = {n: pin(n) for n in qnames}
        qconfig = dict(frozen, qualification=dict(directory=str(qdir), files=qfiles))
        with patch.object(qualification.worker, "source_hashes", lambda root: source_map), patch.object(
                qualification.worker, "source_identity", lambda source: SOURCE_ID):
            native_authority(qconfig, repo); checks += 1
            broken = copy.deepcopy(qconfig); broken["qualification"]["files"].pop("test.log")
            rejected(lambda: native_authority(broken, repo))
            body = (qdir / "test.log").read_bytes(); (qdir / "test.log").write_bytes(b"tampered")
            rejected(lambda: native_authority(qconfig, repo)); (qdir / "test.log").write_bytes(body)
            put("root-verification.json", dict(root_receipt, qualified=False))
            broken = copy.deepcopy(qconfig); broken["qualification"]["files"]["root-verification.json"] = pin("root-verification.json")
            rejected(lambda: native_authority(broken, repo)); put("root-verification.json", root_receipt)
            put("aws-terminal.json", dict(original_terminal, original_exit_code=17))
            broken = copy.deepcopy(qconfig); broken["qualification"]["files"]["aws-terminal.json"] = pin("aws-terminal.json")
            rejected(lambda: native_authority(broken, repo)); put("aws-terminal.json", original_terminal)
            # Rehash the body/receipt/terminal closure: malformed original stage
            # or resource evidence must still fail the existing validators.
            for bad_name, bad_body in (("test.log", b"\n".join(lines[:-1]) + b"\n"),
                ("workspace-cgroup.json", canonical(dict(closed=False, before=counters, after=counters)))):
                body = (qdir / bad_name).read_bytes(); put(bad_name, bad_body)
                changed_receipt = copy.deepcopy(original_receipt); changed_receipt["artifacts"][bad_name] = pin(bad_name)
                put("workspace-receipt.json", changed_receipt)
                changed_terminal = copy.deepcopy(original_terminal)
                for n in (bad_name, "workspace-receipt.json"): changed_terminal["artifacts"][n] = pin(n)
                put("aws-terminal.json", changed_terminal)
                broken = copy.deepcopy(qconfig)
                for n in (bad_name, "workspace-receipt.json", "aws-terminal.json"): broken["qualification"]["files"][n] = pin(n)
                rejected(lambda: native_authority(broken, repo))
                put(bad_name, body); put("workspace-receipt.json", original_receipt); put("aws-terminal.json", original_terminal)
            with patch.object(qualification.worker, "source_hashes", lambda root: {}):
                rejected(lambda: native_authority(qconfig, repo))
        rdir = base / "retained-admission"; rdir.mkdir()
        (rdir / "config.json").write_bytes(b"{}")
        (rdir / "COMPLETE.json").write_bytes(b"{}")
        rc = dict(frozen, retained=dict(directory=str(rdir), config=descriptor(rdir / "config.json"),
                                       complete=descriptor(rdir / "COMPLETE.json")))
        with patch.object(retained, "replay", side_effect=ValueError("retained body missing")), patch(
                __name__ + ".native_authority", side_effect=AssertionError("qualified before retained closure")):
            rejected(lambda: authorities(rc, repo))

        rdir = base / "retained-payloads"; rdir.mkdir()
        order = array("Q", range(1_000_000))
        if sys.byteorder != "little": order.byteswap()
        for name, body in {"source.raw": b"synthetic-original-f32", "source-order.u64": order.tobytes(),
                           "source-root.json": b"{}", "source-sq8.bin": b"synthetic-sq8"}.items():
            (rdir / name).write_bytes(body)
        del order
        for name in retained.GENERATION_FILES:
            target = rdir / "generation" / name; target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(("payload:" + name).encode())
        sq8_pin = identity(rdir / "source-sq8.bin"); can_pin = identity(rdir / "generation/canonical.bin")
        root = dict(schema="borsuk-two-bit-generation-v8", generation=1, base_epoch=0, low=[0], step=[1],
            canonical=dict(can_pin, object_key="retained/objects/" + can_pin["sha256"]),
            sq8_object_key="retained/objects/" + sq8_pin["sha256"], sq8_object_sha256=sq8_pin["sha256"],
            sq8_etag='"old-host-etag"', discovery=dict(profile="fresh1m"))
        (rdir / "generation/manifest.json").write_bytes(canonical(root))
        original_payloads = retained.roster(rdir)
        binary = base / "fake-binary"; binary.write_bytes(b"ELF-synthetic-never-executed")
        proof = dict(binary=descriptor(binary), scorer_source_sha256="2" * 64, router_source_sha256="3" * 64,
                     native_qualification_passed=True)
        data = dict(metadata, retained_directory=rdir, retained_marker=dict(passed=True),
                    old=dict(old, corpus=dict(old["corpus"], sq8=sq8_pin)))
        native = importlib.import_module("scripts.run_native_semantic_1m_quality")
        host = base / "rehost-positive"; host.mkdir()
        rr = rehost(frozen, data, native, host)
        require(rr["actual_sq8_head"] == native.local_head(host / "store" / root["sq8_object_key"])
                and rr["delta"]["sq8_etag"]["new"] != root["sq8_etag"]
                and retained.roster(rdir) == original_payloads, "actual ETag/exact payload reuse")
        require((host / "source-order.u64").stat().st_ino == (rdir / "source-order.u64").stat().st_ino,
                "large body hardlink")
        checks += 2
        for key in ("sq8_object_key", "canonical"):
            changed = copy.deepcopy(root)
            if key == "canonical": changed[key]["object_key"] = "../escape"
            else: changed[key] = "../escape"
            (rdir / "generation/manifest.json").write_bytes(canonical(changed))
            target = base / ("unsafe-" + key); target.mkdir()
            rejected(lambda: rehost(frozen, data, native, target))
        (rdir / "generation/manifest.json").write_bytes(canonical(root))
        changed = copy.deepcopy(rr); changed["delta"]["sq8_etag"]["new"] = '"invented"'
        rejected(lambda: check_rehost(frozen, host, native, changed))

        events = []

        class Vectors:
            shape = (64, 768)
            def tobytes(self):
                return b"".join(struct.pack("<768f", float(i + 1), *([0.] * 767)) for i in range(64))
            def __iter__(self):
                return iter(SimpleNamespace(tolist=lambda i=i: [float(i + 1)] + [0.] * 767) for i in range(64))

        class Truth:
            shape = (64, 100)
            dtype = "<u4"
            def tobytes(self): return struct.pack("<100I", *range(100)) * 64
            def astype(self, dtype): return SimpleNamespace(tobytes=lambda: struct.pack("<100q", *range(100)) * 64)

        vectors = Vectors()
        query_hashes = ([hashlib.sha256(str(i).encode()).hexdigest() for i in range(64)],
                        [hashlib.sha256(("normalized" + str(i)).encode()).hexdigest() for i in range(64)])
        helper = SimpleNamespace(np=SimpleNamespace(dtype=lambda name: name), accounted_helpers=lambda a: ExitStack())
        helper.shared = SimpleNamespace(vector_hashes=lambda q: query_hashes,
            oracle_self_check=lambda: events.append("oracle_self_check"))
        def oracle(raw, queries, rows):
            require((target / "measurement-receipt.json").is_file()
                    and len((target / "records.jsonl").read_bytes().splitlines()) == 68
                    and not (target / "truth.i64").exists(), "GT after complete seal, exactly once")
            events.append("oracle"); return Truth()
        helper.shared.oracle = oracle
        def check_outputs(directory):
            require((directory / "queries.raw").stat().st_size == 196608
                    and (directory / "truth.u32").stat().st_size == 25600
                    and (directory / "truth.i64").stat().st_size == 51200, "exact synthetic truth widening")
        helper.shared.check_outputs = check_outputs
        def acquire(conf, dataset, directory, accounting):
            events.append("acquire")
            for name in ("prior-queries.raw", "test-queries.raw"):
                (directory / name).write_bytes(b"synthetic-historical")
            return vectors, [dict(authenticated_full_body=True)]
        helper.acquire_vectors = acquire
        helper.download = lambda conf, pin, output, accounting: output.write_bytes(b"synthetic-consumed")
        def audit(q, directory, accounting=None):
            events.append("duplicate_audit")
            return dict(passed=True, raw_sha256=query_hashes[0], unit_sha256=query_hashes[1])
        helper.audit = audit
        for panel_index in range(2):
            for hash_key, hashes in (("raw_sha256", query_hashes[0]), ("normalized_sha256", query_hashes[1])):
                changed = copy.deepcopy(data)
                changed["panel"]["prior_panels_vector_hashes"][panel_index][hash_key][0] = hashes[0]
                rejected(lambda: duplicate_audit(vectors, helper, changed, host, None))

        def process(args, log, seconds, timing):
            events.append("scorer")
            c = read_json(args[1]); c_pin = descriptor(args[1])
            log.write_text("synthetic scorer; no native execution\n"); timing.write_text("synthetic RSS\n")
            require(set(c) == set(scorer_config(frozen, target, read_json(target / "rehost.json")))
                    and not any("truth" in k for k in c) and seconds > 0, "truth-free exact scorer config")
            records = [dict(schema="borsuk-semantic-router-scorer-result-v3", phase="identity",
                config_sha256=c_pin["sha256"], binary_sha256=proof["binary"]["sha256"],
                scorer_source_sha256=proof["scorer_source_sha256"], router_source_sha256=proof["router_source_sha256"],
                physical_s3_measured=False)]
            item = {k: 0 for k in ("metadata_wave metadata_wave_wall_ns bytes reused_root_bytes retained_root_bytes "
                "local_auth_wall_ns local_copy_wall_ns chunks head_wall_ns logical_head_requests logical_get_requests "
                "payload_buffer_bound_bytes get_wall_ns stream_wall_ns write_wall_ns").split()}
            records.append(dict(phase="startup", truth_opened=False, profile="fresh1m", evaluator_payload_charge=32 << 20,
                metadata=dict(metadata=[dict(item, name="manifest.json")], staging_wall_ns=1, decode_wall_ns=1,
                source_head_requests=1, source_head_wall_ns=1, router_head_requests=1, router_head_wall_ns=1)))
            for ordinal in range(64):
                returned = list(range(100))
                if mode == "scientific-fail": returned[9] = 200
                records.append(dict(phase="frozen_query", ordinal=ordinal, truth_opened=False,
                    trace=dict(semantic_leaves=list(range(48)), semantic_units=list(range(3072)),
                        semantic_seed_additions=[], nomination_evaluated_units=list(range(3072)),
                        ranked_candidate_pages=[0], primary_page=0, discoveries=[]),
                    selected_pages=[0], ranges=[dict(start=0, end=256 * 780)], returned_ids=returned,
                    leaf_gets=48, leaf_bytes=4_730_880, source_gets=128, source_bytes=614400,
                    sq8_gets=1, sq8_bytes=256 * 780, query_wall_ns=10, query_process_cpu_ns=5,
                    stages=dict(discovery=dict(start_ns=0, end_ns=1), source=dict(start_ns=1, end_ns=2),
                        planning=dict(start_ns=2, end_ns=3), sq8=dict(start_ns=3, end_ns=4), leaf_peak_inflight=16)))
            summary = dict(status="FROZEN", complete=True, queries=64, first=0, truth_opened=False, physical_s3_measured=False,
                observed_process_peak_bytes=1 << 20, config_sha256=c_pin["sha256"],
                **{k: records[0][k] for k in offline.HASHES},
                **{k: c[k] for k in ("dataset", "profile", "rows", "dimensions", "generation_prefix", "generation_root_sha256")})
            for key in ("requests", "order"):
                for suffix in ("bytes", "sha256"): summary[key + "_" + suffix] = c[key][suffix]
            records.extend([dict(phase="all_queries_frozen", count=64, status="FROZEN", complete=True,
                                 truth_opened=False, summary=summary), dict(phase="terminal", summary=summary)])
            if mode == "partial": records.pop()
            if mode == "bad-identity": records[0]["binary_sha256"] = "0" * 64
            if mode == "reordered": records[2], records[3] = records[3], records[2]
            publish(Path(args[-1]), b"".join(canonical(r) + b"\n" for r in records))
            if mode == "scratch-leak": (target / "native-scratch/leak").write_bytes(b"leaked")
            if mode == "timeout": raise subprocess.TimeoutExpired(args, seconds)
            return dict(exit_status=2 if mode == "scorer-fail" else 0, process_cleanup=mode != "cleanup-fail",
                wall_seconds=.01, process_peak_rss_kib=(512 << 10) + 1 if mode == "rss-fail" else 1024)

        with ExitStack() as stack:
            stack.enter_context(patch.dict(os.environ, {n: "2" for n in retained.THREAD_ENV}))
            stack.enter_context(patch(__name__ + ".authorities", lambda config, root: (data, proof)))
            stack.enter_context(patch(__name__ + ".native_authority", lambda config, root: proof))
            stack.enter_context(patch(__name__ + ".dependencies", lambda *unused: (helper, SimpleNamespace(
                local_head=native.local_head, ordinal_check=lambda *a, **k: dict(synthetic=True), run_process=process))))
            stack.enter_context(patch.object(retained, "admission", lambda config: None))
            for mode in ("go", "scientific-fail", "scorer-fail", "partial", "reordered", "bad-identity", "rss-fail", "cleanup-fail", "scratch-leak", "timeout"):
                target = base / ("run-" + mode); events.clear()
                if mode in ("go", "scientific-fail"):
                    result = run(path, config_sha, repo, target)
                    require(result["execution_status"] == "SUCCESS" and result["scientific_status"] ==
                            ("GO" if mode == "go" else "FAIL"), "execution/scientific statuses separate")
                    require(events == ["acquire", "duplicate_audit", "scorer", "oracle_self_check", "oracle"], "one-pass chronology")
                    if mode == "scientific-fail":
                        require(result["first_crossing_stage"] == "returned_ids", "downstream bottleneck retained")
                    snapshot = events.copy()
                    require(replay(path, config_sha, repo, target) == result and events == snapshot,
                            "replay invokes no data/scorer/oracle")
                    checks += 3
                    rejected(lambda: run(path, config_sha, repo, target))
                    decision_path = target / "decision.json"; decision_path.chmod(0o644)
                    original = decision_path.read_bytes(); decision_path.write_bytes(b"tampered")
                    rejected(lambda: replay(path, config_sha, repo, target)); decision_path.write_bytes(original)
                    marker_path = target / "COMPLETE.json"; marker_path.chmod(0o644)
                    original_marker = marker_path.read_bytes()
                    extra = target / "unlisted-body"; extra.write_bytes(b"unlisted")
                    forged = offline.decode(original_marker)
                    forged["files"][extra.name] = {k: identity(extra)[k] for k in ("bytes", "sha256")}
                    forged["roster_sha256"] = retained.archived.prior.value_sha(forged["files"])
                    marker_path.write_bytes(canonical(forged))
                    rejected(lambda: replay(path, config_sha, repo, target))
                    extra.unlink(); marker_path.write_bytes(original_marker)
                else:
                    rejected(lambda: run(path, config_sha, repo, target))
                    require(events == ["acquire", "duplicate_audit", "scorer"]
                            and not (target / "truth.u32").exists() and not (target / "truth.i64").exists()
                            and not (target / "measurement-receipt.json").exists() and not (target / "COMPLETE.json").exists()
                            and (target / "records.jsonl").is_file() and read_json(target / "failure.json")["scientific_status"] == "INVALID",
                            "failed scorer ends arm without GT; original diagnostics retained")
                    checks += 1
            for panel_index in range(2):
                for hash_key, hashes in (("raw_sha256", query_hashes[0]), ("normalized_sha256", query_hashes[1])):
                    previous = data["panel"]["prior_panels_vector_hashes"][panel_index][hash_key][0]
                    data["panel"]["prior_panels_vector_hashes"][panel_index][hash_key][0] = hashes[0]
                    target = base / ("duplicate-" + str(panel_index) + "-" + hash_key); events.clear()
                    rejected(lambda: run(path, config_sha, repo, target))
                    require(events == ["acquire"] and not (target / "scorer-config.json").exists()
                            and not (target / "truth.i64").exists(), "both-panel duplicate stops before scoring or GT")
                    data["panel"]["prior_panels_vector_hashes"][panel_index][hash_key][0] = previous
                    checks += 1
            target = base / "historical-duplicate"; events.clear()
            with patch.object(helper, "audit", side_effect=ValueError("FIRST1M/historical duplicate")):
                rejected(lambda: run(path, config_sha, repo, target))
                require(events == ["acquire"] and not (target / "truth.i64").exists(), "historical duplicate stops")
                checks += 1
            target = base / "deadline"; events.clear()
            with patch.object(retained.archived.Accounting, "remaining", side_effect=ValueError("deadline exceeded")):
                rejected(lambda: run(path, config_sha, repo, target))
                require(not (target / "truth.i64").exists(), "whole deadline stops before GT")
                checks += 1

        resource_fixture = read_json(base / "run-go/resources.json")
        resource_fixture.update(cgroup={"memory.max": str(12 << 30), "memory.peak": str(1 << 20),
            "memory.swap.max": "0", "memory.swap.peak": "0", "cpu.max": "200000 100000"}, actual_tasks_limit="512")
        check_resources(resource_fixture, frozen, 1); checks += 1
        for field, value in (("passed", False), ("build_invocations", 1), ("scorer_invocations", 0),
            ("oracle_invocations", 2), ("resource_failure", {}), ("transport_accounting_complete", False),
            ("aws_errors", 1), ("tasks_max", 513), ("memory_events", {"oom_kill": "1"}),
            ("peak_scratch_bytes", (16 << 30) + 1), ("process_max_rss_kib", (12 << 20) + 1),
            ("child_max_rss_kib", (12 << 20) + 1), ("wall_seconds", 3601), ("actual_tasks_limit", "max")):
            changed = dict(resource_fixture, **{field: value})
            rejected(lambda: check_resources(changed, frozen, 1))
        for field, value in (("memory.max", str(8 << 30)), ("memory.peak", str((12 << 30) + 1)),
                             ("memory.swap.peak", "1"), ("cpu.max", "300000 100000")):
            changed = dict(resource_fixture, cgroup=dict(resource_fixture["cgroup"], **{field: value}))
            rejected(lambda: check_resources(changed, frozen, 1))
        for field in ("process_cleanup", "native_scratch_empty"):
            changed = dict(resource_fixture, failure_cleanup=dict(resource_fixture["failure_cleanup"], **{field: False}))
            rejected(lambda: check_resources(changed, frozen, 1))
        changed = dict(resource_fixture, thread_environment=dict(resource_fixture["thread_environment"], OPENBLAS_NUM_THREADS="1"))
        rejected(lambda: check_resources(changed, frozen, 1))

        # Exercise the actual shared runner's timeout cleanup without a process.
        child = SimpleNamespace(pid=424242, wait=Mock(side_effect=[subprocess.TimeoutExpired("mock", 1), 0, 0]))
        with patch.object(native.subprocess, "Popen", return_value=child), patch.object(native.os, "killpg") as kill:
            rejected(lambda: native.run_process(["never-executed"], base / "reaper.log", 1))
            require(child.wait.call_count == 3 and [c.args for c in kill.call_args_list] ==
                    [(424242, signal.SIGTERM), (424242, signal.SIGKILL)], "original process group reaped on timeout")
            checks += 1
    signal.alarm(0)
    require("numpy" not in sys.modules and "pyarrow" not in sys.modules, "self-check must remain stdlib only")
    return dict(schema=SCHEMA + "-self-check", passed=True, checks=checks,
        elapsed_seconds=time.monotonic() - started, peak_rss_kib=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,
        synthetic=True, native_executed=False, real_vectors_or_truth_opened=False, launch_authority=False)


def main(argv=None):
    args = list(sys.argv[1:] if argv is None else argv)
    try:
        if args == ["--self-check"]:
            result = self_check()
        else:
            replay_only = args[:1] == ["--replay"]
            if replay_only: args.pop(0)
            require(len(args) == 4, "usage: [--replay] CONFIG SHA REPO OUTPUT | --self-check")
            result = (replay if replay_only else run)(Path(args[0]), args[1], Path(args[2]), Path(args[3]))
        print(json.dumps(result, sort_keys=True, allow_nan=False))
        return 0
    except (Exception, KeyboardInterrupt) as error:
        print(type(error).__name__ + ": " + str(error), file=sys.stderr)
        return 2


if __name__ == "__main__":
    def interrupted(signum, frame):
        raise InterruptedError("driver interrupted: " + str(signum))
    signal.signal(signal.SIGTERM, interrupted)
    sys.exit(main())
