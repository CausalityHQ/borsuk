"""Offline construction checks for the closed, failed CoHere preparation a0001.

CLI: REPO NEW_OUTPUT_JSON | --self-check
The report binds existing query/GT bytes; the root owns subsequent authority.
No campaign, resource, serving, historical-coverage or ANN qualification passes.
"""

import argparse
import hashlib
import importlib
import json
import math
import resource
import signal
import struct
import sys
import time
from collections import Counter
from pathlib import Path


BASE = "docs/research/performance-architecture-20260930/semantic-1m/"
ORIGINAL = BASE + "cohere-preparation/a0001/"
READBACK = BASE + "cohere-preparation/a0001-scoped-readback/readback.json"
SEALED = (
    "queries.raw", "requests.jsonl", "truth.u32", "truth.i64", "panel.json",
    "duplicate-audit.json", "oracle.json", "resources.json", "decision.json",
)
TERMINAL_BODIES = (
    "config.json", "helper-config.json", "source-qualification.json", "cpu.txt",
    "tool-versions.json", "run-closed.log", "profile.log", "profile-resources.txt",
    "profile-cgroup.json", "preparation-closure.json", "failure.json",
    "screen/final-resources.json", *("screen/" + name for name in SEALED),
)
# Independently frozen controls from evidence base 90c98a9f65de6a3e79323df3ce9c8c3587c98d28.
PINS = {
    ORIGINAL + "aws-terminal.json": (3421, "ab8b1fab71fcda376e3a71143b196ecf55abfce52ad1994cc297ce5f98fa8706"),
    ORIGINAL + "aws-reservation.json": (11660, "7f3bca19e8350b1afbb50f0d47578114c202bd7d13b0696dd7b3bed6bc8cf0f3"),
    ORIGINAL + "aws-launch.json": (361, "808859a6c65deebee157ccb5f707fc325201000d4b0c4540ad7e50fb5afdb4da"),
    ORIGINAL + "aws-closeout.json": (135, "26103e9b223f5ad71d7b52428ee1ad3ca5e0b1eb8503deea63f4f607db9b07aa"),
    READBACK: (5497, "ceb282074e66500f1213ea230d416c2a49c23cdb32c070d2cdc0a6f00b415aea"),
}


def identity(body):
    return dict(bytes=len(body), sha256=hashlib.sha256(body).hexdigest())


def read(repo, name):
    path = repo / name
    assert path.is_file() and not path.is_symlink(), "regular evidence body: " + name
    assert path.resolve().is_relative_to(repo), "evidence escapes repository: " + name
    with path.open("rb") as stream:
        body = stream.read((4 << 20) + 1)
    assert len(body) <= 4 << 20, "small evidence cap: " + name
    return body


def load_evidence(repo):
    return {name: read(repo, name) for name in (*PINS, *(ORIGINAL + n for n in TERMINAL_BODIES))}


def rejects(action):
    try:
        action()
    except AssertionError:
        return
    raise AssertionError("invalid evidence accepted")


def validate(repo, bodies=None):
    """Reuse offline predicates; never invoke or alter the campaign success gate."""
    assert __debug__, "authority checks require assertions enabled"
    repo = Path(repo).resolve()
    assert Path(__file__).resolve() == repo / "scripts/check_cohere_closed_panel_construction.py", "validator origin"
    if bodies is None:
        bodies = load_evidence(repo)
    assert set(bodies) == set(PINS) | {ORIGINAL + n for n in TERMINAL_BODIES}, "exact evidence roster"
    for name, (size, digest) in PINS.items():
        assert identity(bodies[name]) == dict(bytes=size, sha256=digest), "frozen control: " + name
    terminal, reservation, launch, closeout = (
        json.loads(bodies[ORIGINAL + name]) for name in
        ("aws-terminal.json", "aws-reservation.json", "aws-launch.json", "aws-closeout.json")
    )
    assert set(terminal["artifacts"]) == set(TERMINAL_BODIES) and len(TERMINAL_BODIES) == 21
    for name, expected in terminal["artifacts"].items():
        assert identity(bodies[ORIGINAL + name]) == expected, "original terminal body: " + name
    proof = reservation["qualification"]
    # Authenticate the entire original code closure before importing its stdlib predicates.
    code_identities = {}
    assert len(proof["code_sha256"]) == 44
    for name, digest in proof["code_sha256"].items():
        code_identities[name] = identity(read(repo, name))
        assert code_identities[name]["sha256"] == digest, "original code drift: " + name
    for path in (repo, repo / "scripts"):
        sys.path.insert(0, str(path))
    campaign = importlib.import_module("scripts.launch_cohere_semantic_1m_preparation_spot")
    helper = campaign.helper
    assert Path(campaign.__file__).resolve() == repo / "scripts/launch_cohere_semantic_1m_preparation_spot.py"
    assert Path(helper.__file__).resolve() == repo / "scripts/prepare_cohere_semantic_1m_panel.py"
    assert campaign.qualify(repo) == proof, "original frozen qualification"
    assert len(proof["refs"]) == 18 and set(proof["code_sha256"]) == set(campaign.CODE)
    assert terminal["schema"] == reservation["schema"] == campaign.SCHEMA
    assert terminal["status"] == "failed" and terminal["phase"] == "preparation"
    assert type(terminal["exit_code"]) is type(terminal["original_exit_code"]) is int
    assert terminal["exit_code"] == terminal["original_exit_code"] == 1
    assert closeout["state"] == "terminated" and closeout["nodes"] == launch["nodes"]
    assert terminal["instance_id"] == launch["instance_id"] in {n["instance_id"] for n in closeout["nodes"].values()}
    source = {k: terminal[k] for k in ("source_commit", "source_archive_sha256")}
    for key, value in source.items():
        assert launch[key] == reservation[key] == value, "original source binding: " + key
    for key in campaign.TERMINAL_IDENTITIES:
        assert terminal[key] == proof[key], "original qualification binding: " + key
    assert reservation["config_sha256"] == proof["config_sha256"]
    assert json.loads(bodies[ORIGINAL + "source-qualification.json"]) == dict(proof, **source)
    assert identity(bodies[ORIGINAL + "config.json"])["sha256"] == proof["config_sha256"]
    assert bodies[ORIGINAL + "config.json"] == read(repo, proof["config_path"])
    assert bodies[ORIGINAL + "helper-config.json"] == campaign.read(repo, campaign.HELPER_CONFIG)
    config = json.loads(bodies[ORIGINAL + "helper-config.json"])
    prefix = launch["prefix"]
    assert prefix == campaign.PREFIX + "a0001"

    closure = json.loads(bodies[ORIGINAL + "preparation-closure.json"])
    failure = json.loads(bodies[ORIGINAL + "failure.json"])
    assert closure["schema"] == "borsuk-cohere-preparation-closure-v1" and closure["closed"] is False
    assert type(closure["helper_exit_code"]) is int and closure["helper_exit_code"] == 0
    assert closure["helper_invocations"] == 1 and closure["prefix"] == prefix
    assert all(closure[k] == 0 for k in ("native_build_invocations", "native_search_invocations", "native_publication_invocations"))
    assert closure["gt_reuse"] is closure["ann_quality_measured"] is False
    assert 0 <= closure["wall_seconds"] <= campaign.WORKER_SECONDS
    assert all(closure[k] == v for k, v in source.items())
    assert closure["config_sha256"] == proof["config_sha256"]
    assert closure["helper_config_sha256"] == campaign.HELPER_CONFIG["sha256"]
    versions = json.loads(bodies[ORIGINAL + "tool-versions.json"])
    assert versions["python"].startswith("3.12") and versions["versions"] == helper.VERSIONS
    assert versions["architecture"] == "x86_64" and versions["os_release"]["ID"] == "ubuntu"
    assert versions["os_release"]["VERSION_ID"] == "24.04" and versions["threads"] == 2 and versions["aws_max_attempts"] == 1
    command = closure["command"]
    assert command[0] == versions["executable"]
    assert command[1:] == [str(Path(command[4]) / helper.CODE[0]), str(Path(command[4]) / campaign.HELPER_CONFIG["path"]),
                           campaign.HELPER_CONFIG["sha256"], command[4], str(Path(command[4]).parent / "screen"), prefix]
    assert failure["schema"] == "borsuk-cohere-preparation-failure-v1" and failure["status"] == "failed"
    assert type(failure["helper_exit_code"]) is int and failure["helper_exit_code"] == 0
    assert failure["bootstrap_exit_code"] == 1 and failure["bootstrap_phase"] == "preparation"
    assert failure["error_type"] == "AssertionError" and failure["replacement_allowed"] is False
    assert failure["ann_quality_measured"] is False
    assert failure["cleanup"]["owned_heavy_inputs_remaining"] is failure["cleanup"]["replacement_allowed"] is False
    assert "seal-readback.json" in failure["cleanup"]["removed"]
    cgroup = json.loads(bodies[ORIGINAL + "profile-cgroup.json"])
    assert int(cgroup["after"]["memory.max"]) == campaign.MEMORY == 2147483648
    assert int(cgroup["after"]["memory.peak"]) == 2155061248 > campaign.MEMORY
    rejects(lambda: campaign.validate_cgroup(cgroup))
    assert b"validate_cgroup(counters)" in bodies[ORIGINAL + "profile.log"]
    assert b"Exit status: 1" in bodies[ORIGINAL + "profile-resources.txt"]

    readback = json.loads(bodies[READBACK])
    assert readback["schema"] == "borsuk-cohere-closed-panel-scoped-readback-v1"
    assert readback["original_terminal_sha256"] == identity(bodies[ORIGINAL + "aws-terminal.json"])["sha256"]
    assert all(readback[k] == v for k, v in source.items())
    assert readback["helper_exit_code"] == 0 and readback["original_campaign_status"] == "FAIL"
    assert readback["prepared_campaign_passed"] is readback["ann_quality_measured"] is readback["input_reconstruction_or_gt_reexecution"] is False
    assert readback["logical_head_calls"] == readback["logical_get_calls"] == 9
    assert readback["sdk_max_attempts"] == 1 and readback["confirmed_wire_requests"] == "UNMEASURED"
    sealed = readback["artifact_bodies_authenticated"]
    assert set(sealed) == set(SEALED)
    decision = json.loads(bodies[ORIGINAL + "screen/decision.json"])
    assert decision["schema"] == "borsuk-cohere-semantic-1m-construction-v1"
    assert decision["config_sha256"] == campaign.HELPER_CONFIG["sha256"] and decision["prefix"] == prefix
    assert decision["decision"] == "PASS fixed fresh64 construction only"
    for key in ("refs", "code_sha256", "corpus", "consumed_queries", "registered_test"):
        assert decision[key] == config[key], "construction binding: " + key
    assert decision["qualification"] is decision["ann_quality_measured"] is decision["complete_historical_coverage"] is False
    assert decision["truth_id_space"] == "source ordinal" and decision["queries"] == 64 and decision["gt_k"] == 100
    assert set(decision["artifacts"]) == set(SEALED) - {"decision.json"}
    artifacts = {}
    for name in SEALED:
        ident = identity(bodies[ORIGINAL + "screen/" + name])
        artifacts[name] = dict(path=name, **ident)
        if name != "decision.json":
            assert decision["artifacts"][name] == artifacts[name], "decision body: " + name
        observed = sealed[name]
        assert observed["key"] == prefix + "/sealed/" + name
        assert observed["authenticated_readback"] is observed["conditional_get_if_match"] is True
        assert {k: observed[k] for k in ("bytes", "sha256")} == ident
        assert observed["metadata"] == {"sha256": ident["sha256"]}
        assert isinstance(observed["etag"], str) and len(observed["etag"]) > 2
        assert observed["etag"].startswith('"') and observed["etag"].endswith('"')
        assert observed["version_id"] is None  # This pinned receipt records an unversioned read.
    assert sum(v["bytes"] for v in artifacts.values()) == 1343471
    assert artifacts["panel.json"] == dict(path="panel.json", **{k: config["refs"]["panel"][k] for k in ("bytes", "sha256")})
    assert bodies[ORIGINAL + "screen/panel.json"] == campaign.read(repo, config["refs"]["panel"])
    panel = json.loads(bodies[ORIGINAL + "screen/panel.json"])
    assert decision["selected_locators_sha256"] == panel["selected_sha256"] == helper.value_sha(panel["selected"])
    authority = json.loads(campaign.read(repo, config["refs"]["metadata_authority"]))
    assert set(config["refs"]) == set(helper.FIXED) | set(authority["proofs"])
    assert all(config["refs"][name] == pin for name, pin in authority["proofs"].items())
    refs = {name: json.loads(campaign.read(repo, pin)) for name, pin in config["refs"].items()}
    helper.selector.authenticate_bindings(authority, refs)
    assert helper.selector.receipt_roster(refs["source_receipt"]) == authority["ordered_train_shards"]
    assert config["corpus"] == authority["corpus"]
    assert config["consumed_queries"] == authority["old_consumed_panel"]["artifacts"]["queries.raw"]
    assert config["prior_unit_sha256"] == helper.historical_pins(refs)
    test_objects = [o for o in refs["source_receipt"]["objects"] if o["role"] == "query"]
    assert len(test_objects) == 1
    test = test_objects[0]
    assert test["format"] == "parquet" and test["rows"] == 1000
    assert test["uri"].startswith("s3://" + config["bucket"] + "/")
    assert config["registered_test"] == dict(key=test["uri"].split("/", 3)[3], bytes=test["bytes"], sha256=test["sha256"], rows=1000)
    assert config["registered_test"]["sha256"] == refs["registered_test"]["test_sha256"]

    for name in ("resources.json", "final-resources.json"):
        report = json.loads(bodies[ORIGINAL + "screen/" + name])
        assert report["schema"] == "borsuk-cohere-preparation-resources-v1" and report["passed"] is True
        assert report["prospective_preparation_limits"] == helper.LIMITS and report["serving_or_build_measurement"] is False
        assert report["ann_quality_measured"] is False and report["aws_max_attempts"] == 1
        assert 0 <= report["wall_seconds"] <= campaign.WORKER_SECONDS
        assert 0 < report["process_max_rss_kib"] * 1024 <= campaign.MEMORY and report["child_max_rss_kib"] * 1024 <= campaign.MEMORY
        assert 0 <= report["actual_scratch_bytes"] <= report["peak_scratch_bytes"] <= campaign.SCRATCH
        assert report["transport_accounting_complete"] is True and report["aws_errors"] == 0
        attempts = report["http_request_dispatch_attempts"]
        assert set(attempts) == {"GET", "HEAD", "PUT"} and all(type(v) is int and v >= 0 for v in attempts.values())
        assert sum(attempts.values()) <= helper.LIMITS["max_requests"]
        assert report["confirmed_wire_requests"] == report["billed_requests"] == "UNMEASURED"
        assert int(report["cgroup"]["memory.peak"]) == int(cgroup["after"]["memory.peak"])

    audit = json.loads(bodies[ORIGINAL + "screen/duplicate-audit.json"])
    assert audit["schema"] == "borsuk-cohere-semantic-1m-vector-audit-v1"
    assert audit["passed"] is True and audit["replacement_allowed"] is audit["complete_historical_coverage"] is False
    assert (audit["new_rows_audited"], audit["indexed_rows_audited"], audit["consumed_rows_audited"]) == (64, 1000000, 4000)
    assert audit["normalization"] == helper.NORMALIZATION
    assert audit["raw_hash_dtype"] == "little-endian f32 bits"
    assert audit["unit_hash_dtype"] == "little-endian f64; normalized signed zero canonicalized"
    assert len(audit["raw_sha256"]) == len(set(audit["raw_sha256"])) == 64
    assert len(audit["unit_sha256"]) == len(set(audit["unit_sha256"])) == 64
    assert audit["config_sha256"] == campaign.HELPER_CONFIG["sha256"] and audit["selected_locators_sha256"] == decision["selected_locators_sha256"]
    assert audit["consumed_scopes"] == dict(old_sealed1000=1000, source_holdout_prior=2000, registered_test=1000)
    assert audit["within_index_prior_covered_by_first1m"] is True
    assert audit["consumed_query_ledger"] == authority["consumed_query_ledger"]
    assert audit["inputs"] == decision["inputs"]
    counts = Counter(row["shard_ordinal"] for row in panel["selected"])
    roster = authority["ordered_train_shards"]
    for low, high in ((1000000, 1002000), (1005000, 1006000)):
        for shard in roster:
            count = min(high, shard["source_end_exclusive"]) - max(low, shard["source_start"])
            if count > 0:
                counts[shard["ordinal"]] += count
    assert len(counts) == 61
    expected_sources = [dict(roster[i], extracted_rows=counts[i], authenticated_full_body=True) for i in sorted(counts)]
    expected_sources.append(dict(config["registered_test"], authenticated_full_body=True))
    assert audit["authenticated_query_sources"] == expected_sources
    assert set(decision["inputs"]) == set(helper.INPUT_FILES) - {"all-consumed.raw"}
    for name, pin in (("source.raw", config["corpus"]["raw"]), ("source-order.u64", config["corpus"]["order"]),
                      ("source-root.json", config["corpus"]["root_manifest"]), ("consumed-queries.raw", config["consumed_queries"])):
        assert decision["inputs"][name] == dict(path=name, **{k: pin[k] for k in ("bytes", "sha256")})
    oracle = json.loads(bodies[ORIGINAL + "screen/oracle.json"])
    assert oracle["schema"] == "borsuk-cohere-semantic-1m-oracle-v1" and oracle["passed"] is oracle["oracle_self_check"] is True
    assert (oracle["rows"], oracle["queries"], oracle["k"]) == (1000000, 64, 100)
    assert oracle["exhaustive_block_sort_top100_merge"] is True and oracle["truth_id_space"] == "source ordinal"
    assert oracle["source_raw"] == config["corpus"]["raw"]
    assert oracle["distance"] == "1-dot of f64-normalized original f32" and oracle["tie"] == "signed source ordinal ascending"
    assert oracle["widening"] == "LEu32 to LEi64, unchanged values"
    for name, key in (("truth.u32", "truth_u32"), ("truth.i64", "truth_i64")):
        assert oracle[key] == artifacts[name]
    assert artifacts["queries.raw"]["bytes"] == 64 * 768 * 4
    assert artifacts["truth.u32"]["bytes"] == 64 * 100 * 4
    assert artifacts["truth.i64"]["bytes"] == 64 * 100 * 8
    queries = bodies[ORIGINAL + "screen/queries.raw"]
    assert audit["raw_sha256"] == [identity(queries[i*3072:(i+1)*3072])["sha256"] for i in range(64)]
    requests = bodies[ORIGINAL + "screen/requests.jsonl"].splitlines()
    assert len(requests) == 64
    narrow, wide = (bodies[ORIGINAL + "screen/" + name] for name in ("truth.u32", "truth.i64"))
    for i, line in enumerate(requests):
        request = json.loads(line)
        assert set(request) == {"ordinal", "query"} and type(request["ordinal"]) is int and request["ordinal"] == i
        vector = request["query"]
        assert len(vector) == 768 and all(type(v) in (float, int) and math.isfinite(v) for v in vector)
        assert struct.pack("<768f", *vector) == queries[i*3072:(i+1)*3072], "request f32 parity"
        raw = struct.unpack_from("<768f", queries, i*3072)
        assert all(math.isfinite(v) for v in raw) and any(raw), "finite nonzero f32 query"
        ids = struct.unpack_from("<100I", narrow, i*400)
        assert len(set(ids)) == 100 and max(ids) < 1000000
        assert ids == struct.unpack_from("<100q", wide, i*800), "signed source ordinal widening"

    return dict(
        schema="borsuk-cohere-closed-panel-construction-check-v1",
        scoped_construction_passed=True, original_campaign_status="FAIL",
        prepared_campaign_passed=False, resource_qualification="failed",
        resource_qualification_passed=False, queries=64, dimensions=768, gt_k=100,
        truth_id_space="source ordinal",
        complete_historical_coverage=False, ann_quality_measured=False,
        serving_or_build_qualified=False, root_authority_issued=False,
        input_reconstruction_or_gt_reexecution=False,
        historical_seal_readback_available=False, historical_etags_reconstructed=False,
        original_cgroup_validator_rejected=True,
        original_memory_cap_bytes=campaign.MEMORY,
        original_memory_peak_bytes=int(cgroup["after"]["memory.peak"]),
        original_preparation_limits=helper.LIMITS, source=source, prefix=prefix,
        source_corpus=config["corpus"], construction_inputs=decision["inputs"],
        selected_locators_sha256=decision["selected_locators_sha256"],
        sealed_artifacts=artifacts, fresh_sealed_observations=sealed,
        evidence_bodies={name: identity(body) for name, body in sorted(bodies.items())},
        original_code_bodies=code_identities, original_reference_bodies=proof["refs"],
        validator_body=identity(read(repo, "scripts/check_cohere_closed_panel_construction.py")),
    )


def self_check():
    repo = Path(__file__).resolve().parents[1]
    bodies = load_evidence(repo)
    report = validate(repo, bodies)
    assert report["scoped_construction_passed"] is True, "committed construction rejected"
    assert report["original_campaign_status"] == "FAIL"
    assert report["prepared_campaign_passed"] is False
    assert report["resource_qualification"] == "failed"
    assert report["complete_historical_coverage"] is False
    assert report["ann_quality_measured"] is False
    changed = dict(bodies)
    name = ORIGINAL + "screen/queries.raw"
    changed[name] = bytes([bodies[name][0] ^ 1]) + bodies[name][1:]
    rejects(lambda: validate(repo, changed))
    campaign = sys.modules["scripts.launch_cohere_semantic_1m_preparation_spot"]
    rejects(lambda: campaign.validate_cgroup(json.loads(bodies[ORIGINAL + "profile-cgroup.json"])))
    assert campaign.replay(repo / ORIGINAL)["prepared"] is False
    return dict(self_check_passed=True, committed_evidence_passed=True,
                in_memory_byte_flip_rejected=True, original_cgroup_validator_rejected=True,
                original_campaign_status="FAIL", prepared_campaign_passed=False)


def main():
    assert __debug__, "authority checks require assertions enabled"
    resource.setrlimit(resource.RLIMIT_AS, (200 << 20, 200 << 20))
    signal.alarm(55)
    started = time.monotonic()
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("repo", nargs="?", type=Path)
    parser.add_argument("output", nargs="?", type=Path)
    parser.add_argument("--self-check", action="store_true")
    args = parser.parse_args()
    if args.self_check:
        if args.repo is not None or args.output is not None:
            parser.error("--self-check takes no positional arguments")
        report = self_check()
    else:
        if args.repo is None or args.output is None:
            parser.error("REPO and NEW_OUTPUT_JSON are required")
        assert not args.output.exists() and not args.output.is_symlink(), "fresh output required"
        report = validate(args.repo)
    report["check_wall_seconds"] = time.monotonic() - started
    report["check_max_rss_kib"] = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    assert report["check_wall_seconds"] < 55 and report["check_max_rss_kib"] * 1024 <= 200 << 20
    encoded = json.dumps(report, sort_keys=True, indent=2, allow_nan=False) + "\n"
    if not args.self_check:
        with args.output.open("x") as stream:
            stream.write(encoded)
    print(encoded if args.self_check else json.dumps(dict(
        output=str(args.output), scoped_construction_passed=True,
        original_campaign_status="FAIL", prepared_campaign_passed=False)))
    signal.alarm(0)


if __name__ == "__main__":
    main()
