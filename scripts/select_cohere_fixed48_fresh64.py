#!/usr/bin/env python3
"""CLI: REPO PROTOCOLPATH PROTOCOLSHA NEWOUTPUT, or --self-check.

PROTOCOLPATH is repository-relative research JSON, frozen by the root before
selection. Required fields are FIXED_PROTOCOL and input_pins (the authenticated
older selector's pins plus TOP32_PINS); each pin is {path, bytes, sha256}.
The API/template is /tmp/borsuk-fixed48-fresh-panel-selector-contract.json.
Publish four new fixed48 metadata schemas; verification.json is the completion
receipt. No vector/truth/quality bodies are read and root qualification is pending.
"""

import argparse
import copy
import hashlib
import json
import os
from pathlib import Path
import platform
import sys
import tempfile

import select_cohere_fresh64_coverage as previous


original = previous.original
require, canonical, value_sha = original.require, original.canonical, original.value_sha
BASE = previous.BASE + "cohere-top32-coverage/"
EVIDENCE_BASE = "53e0d09f31aa7ee6a60c3b2017404309795f3cd1"
SEED = "borsuk-cohere-first1m-fixed48-routing-fresh64-v1"
POPULATION = 8_996_872
FIXED_PROTOCOL = dict(
    schema="borsuk-cohere-fixed48-fresh64-selector-protocol-v1", seed_text=SEED,
    count=64, selected_leaves=48, original_population_size=8_997_000,
    excluded_previous_count=128, population_size=POPULATION,
    python=dict(implementation=platform.python_implementation(), version=platform.python_version()),
    rule=previous.RULE, replacement_allowed=False, metadata_only=True,
    old_fail_preserved=True, raw_normalized_duplicate_gate_pending=True)
TOP32_PINS = {
    "top32_authority": dict(path=BASE + "panel-tools/authority.json", bytes=202936,
                            sha256="d07bef1f8552a0884cceb04c7404ad596516666aa1433ba06be7c26182b3027b"),
    "top32_panel": dict(path=BASE + "panel-tools/panel.json", bytes=58878,
                        sha256="572e11afce1f778ababcbd17038ba8e5ac3ab2c0744e1075b56d6af2d9d07f49"),
    "top32_root_freeze": dict(path=BASE + "panel-tools/root-freeze.json", bytes=897,
                              sha256="a79ebfdf80458c51136ab1b25a4330c7f31be289fbfc9621e74a204be1f30779"),
    "top32_verification": dict(path=BASE + "panel-tools/verification.json", bytes=7152,
                               sha256="c8b770f6753df4f39eabe87cac750e49d7491766810ef46277377eab95d7ecdb"),
    "top32_terminal": dict(path=BASE + "a0002/aws-terminal.json", bytes=5530,
                           sha256="f7aa03265a032ea075bdd3506360186a156e841a4eb931b4fc7b4de3af6a0611"),
    "top32_execution_panel": dict(path=BASE + "a0002/screen/panel.json", bytes=58878,
                                  sha256="572e11afce1f778ababcbd17038ba8e5ac3ab2c0744e1075b56d6af2d9d07f49"),
    "top32_duplicate_audit": dict(path=BASE + "a0002/screen/duplicate-audit.json", bytes=42110,
                                  sha256="ec642ebd86ddcbc033815b47eaa797825ed80369f48ee3c7bf6bbecbd138d124"),
    "top32_root_independent_audit": dict(path=BASE + "a0002/root-independent-audit.json", bytes=3423,
                                         sha256="0c15b6d8f79377ff0bf3e536fad72d70fd9d04a96986a9373b71cfd1f85675a4"),
}
MISSING = ["source-order.u64", "source-root.json", "seal-readback.json", "decision.json"]
SCOPE = dict(previous.SCOPE, original_top32_execution_status="FAIL",
             original_top32_exit_status=1, original_top32_execution_qualified=False,
             original_top32_report_status="FAIL", original_top32_fail_unchanged=True,
             original_top32_closure_restored=False, new_vector_duplicates_checked=False)


def load_protocol(repo, path, sha):
    path = Path(path)
    require(not path.is_absolute() and ".." not in path.parts
            and path.parts[:2] == ("docs", "research") and path.suffix == ".json",
            "protocol must be repository-relative research JSON")
    pin = dict(path=path.as_posix(), bytes=os.lstat(Path(repo) / path).st_size, sha256=sha)
    protocol = previous.read_checked(repo, pin)
    require(canonical({k: protocol[k] for k in FIXED_PROTOCOL}) == canonical(FIXED_PROTOCOL)
            and set(protocol) == set(FIXED_PROTOCOL) | {"input_pins"},
            "fixed48 seed/count/Python/population/protocol scope differs")
    return protocol, pin


def historical_ledger(a, prior, pins):
    return a["consumed_query_ledger"] + [dict(
        split="train", source_ordinals=prior["source_ordinals"], count=64,
        panel_pin=pins["previous_consumed_panel"], duplicate_audit_pin=pins["previous_duplicate_audit"])]


def authenticate_top32(repo, a, prior, pins):
    bodies = {name: previous.read_checked(repo, pin) for name, pin in TOP32_PINS.items()}
    authority, panel = bodies["top32_authority"], bodies["top32_panel"]
    freeze, verification = bodies["top32_root_freeze"], bodies["top32_verification"]
    for body, suffix in ((authority, "metadata-authority"), (panel, "locators"),
                         (freeze, "root-freeze"), (verification, "metadata-verification")):
        require(body["schema"] == "borsuk-cohere-top32-fresh64-" + suffix + "-v1"
                and canonical({k: body[k] for k in previous.SCOPE}) == canonical(previous.SCOPE),
                "previous top32 metadata schema/scope differs")
    for name in ("bucket", "dataset_id", "dataset_content_sha256", "train_rows", "embedding_column",
                 "source_receipt", "ordered_train_shards", "proofs", "corpus", "old_consumed_panel"):
        require(canonical(authority[name]) == canonical(a[name]), "top32 original authority differs: " + name)
    spec = previous.selection_spec(a, prior)
    ledger = historical_ledger(a, prior, pins)
    require(authority["original_selection"] == a["selection"] and authority["selection"] == spec
            and authority["consumed_query_ledger"] == ledger
            and authority["original_consumed_query_ledger_sha256"] == value_sha(a["consumed_query_ledger"])
            and authority["prior64_vector_hashes"] == prior and authority["input_pins"] == pins,
            "top32 original population/ledger/previous64 binding differs")
    previous.validate_panel(panel["selected"], a, prior, spec)
    expected_panel = dict(previous.SCOPE, schema="borsuk-cohere-top32-fresh64-locators-v1",
                          input_pins=pins, source_receipt_sha256=a["source_receipt"]["sha256"],
                          corpus=a["corpus"], selection=spec, consumed_query_ledger_sha256=value_sha(ledger),
                          selected_sha256=value_sha(panel["selected"]), selected=panel["selected"],
                          prior64_vector_hashes=prior, authority_sha256=TOP32_PINS["top32_authority"]["sha256"])
    require(canonical(panel) == canonical(expected_panel)
            and canonical(bodies["top32_execution_panel"]) == canonical(panel),
            "top32 consumed panel/order/authority differs")
    artifacts = {name + ".json": {k: TOP32_PINS["top32_" + key][k] for k in ("bytes", "sha256")}
                 for name, key in (("authority", "authority"), ("panel", "panel"), ("root-freeze", "root_freeze"))}
    require(verification["artifacts"] == artifacts and verification["input_pins"] == pins
            and verification["selected_sha256"] == panel["selected_sha256"]
            and freeze["authority_sha256"] == panel["authority_sha256"]
            and freeze["panel_sha256"] == TOP32_PINS["top32_panel"]["sha256"]
            and freeze["selected_locators_sha256"] == panel["selected_sha256"]
            and freeze["selection_count"] == verification["selected_count"] == 64,
            "top32 metadata freeze/receipt binding differs")
    terminal, audit = bodies["top32_terminal"], bodies["top32_duplicate_audit"]
    for name, key in (("screen/panel.json", "top32_execution_panel"),
                      ("screen/duplicate-audit.json", "top32_duplicate_audit")):
        require(terminal["artifacts"][name] == {k: TOP32_PINS[key][k] for k in ("bytes", "sha256")},
                "top32 original terminal must bind consumed panel and duplicate-audit bytes/SHA256")
    require(audit["schema"] == "borsuk-cohere-top32-duplicate-audit-v1"
            and audit["passed"] is True and audit["replacement_allowed"] is False
            and audit["complete_historical_coverage"] is False
            and audit["new_rows_audited"] == audit["previous64_rows_audited"] == 64
            and audit["indexed_rows_audited"] == original.ROWS
            and audit["selected_locators_sha256"] == panel["selected_sha256"]
            and audit["previous64_authority"] == prior
            and audit["config_sha256"] == terminal["helper_config_sha256"],
            "top32 consumed duplicate audit binding differs")
    for input_name, source_name in (("source.raw", "raw"), ("source-order.u64", "order"),
                                    ("source-root.json", "root_manifest"), ("source-sq8.bin", "sq8")):
        require(all(audit["inputs"][input_name][k] == a["corpus"][source_name][k]
                    for k in ("bytes", "sha256")), "top32 audit FIRST1M binding differs")
    require(all(audit["inputs"]["consumed-queries.raw"][k] == a["old_consumed_panel"]["artifacts"]["queries.raw"][k]
                for k in ("bytes", "sha256")), "top32 audit historical consumed1000 binding differs")
    for audit_key, prior_key in (("normalization", "normalization"), ("raw_hash_dtype", "raw_hash_dtype"),
                                 ("unit_hash_dtype", "normalized_hash_dtype")):
        require(audit[audit_key] == prior[prior_key], "prior-panel vector hash interpretation differs")
    closed = bodies["top32_root_independent_audit"]
    require(terminal["schema"] == "borsuk-cohere-top32-coverage-spot-v1"
            and terminal["status"] == "failed" and terminal["exit_code"] == terminal["original_exit_code"] == 1
            and terminal["old_fail_preserved"] is True
            and closed["schema"] == "borsuk-cohere-top32-closed-offline-audit-v1"
            and closed["original_execution_status"] == closed["scientific_report_status"] == "FAIL"
            and closed["original_exit_status"] == 1 and closed["original_execution_qualified"] is False
            and closed["scientific_report_qualified"] is False and closed["old_fail_preserved"] is True
            and closed["original_source_root_missing"] is True
            and closed["final_seal_and_decision_authority_missing"] is True
            and closed["physical_page_manifest_body_verified"] is False
            and closed["missing_closure_artifacts"] == MISSING
            and closed["terminal_bodies_authenticated"] == len(terminal["artifacts"]) == 34
            and closed["source_archive_sha256"] == terminal["source_archive_sha256"]
            and closed["source_commit"] == terminal["source_commit"],
            "original top32 FAIL/missing38-roster authority must remain unchanged")
    second = dict(query_ordinals=list(range(64)), source_ordinals=[r["source_ordinal"] for r in panel["selected"]],
                  raw_sha256=audit["raw_sha256"], normalized_sha256=audit["unit_sha256"],
                  normalization=audit["normalization"], raw_hash_dtype=audit["raw_hash_dtype"],
                  normalized_hash_dtype=audit["unit_hash_dtype"], audit_pin=TOP32_PINS["top32_duplicate_audit"],
                  terminal_pin=TOP32_PINS["top32_terminal"], panel_pin=TOP32_PINS["top32_execution_panel"],
                  must_check_before_ground_truth=True)
    return second


def selection_spec(a, priors):
    require(len(priors) == 2, "both consumed fresh64 panels required")
    for prior in priors:
        require(canonical(prior["query_ordinals"]) == canonical(list(range(64))) and len(prior["source_ordinals"]) == 64,
                "each prior panel must carry all64 ordered locators")
        for field in ("raw_sha256", "normalized_sha256"):
            hashes = prior[field]
            require(isinstance(hashes, list) and len(hashes) == len(set(hashes)) == 64
                    and all(previous.valid_sha(h) for h in hashes), "prior64 hashes missing/duplicate/invalid")
    for field in ("raw_sha256", "normalized_sha256"):
        require(len(set(priors[0][field] + priors[1][field])) == 128, "prior vector hashsets overlap")
    removed = priors[0]["source_ordinals"] + priors[1]["source_ordinals"]
    require(len(removed) == len(set(removed)) == 128, "both prior64 locators must be distinct/disjoint")
    intervals = previous.subtract_ordinals(a["selection"]["eligible_intervals"], removed)
    population = sum(high - low for low, high in intervals)
    require(a["selection"]["population_size"] == original.POPULATION and population == POPULATION,
            "fresh population must be 8997000 - 128 = 8996872")
    seed = hashlib.sha256(SEED.encode()).digest()
    geometry = dict(candidate_interval=a["selection"]["candidate_interval"],
                    excluded_consumed_intervals=a["selection"]["excluded_consumed_intervals"],
                    excluded_previous_source_ordinals=sorted(removed), eligible_intervals=intervals,
                    population_size=population, roster_sha256=value_sha(a["ordered_train_shards"]))
    return dict(**geometry, population_sha256=value_sha(geometry), count=64, seed_text=SEED,
                seed_sha256=seed.hex(), seed_integer=int.from_bytes(seed, "big"),
                seed_derivation="SHA256 UTF-8 seed text; entire digest as unsigned big-endian integer",
                python=FIXED_PROTOCOL["python"], rule=previous.RULE, failure_policy=original.FAILURE_POLICY)


def prepare(repo, protocol_path, protocol_sha):
    protocol, protocol_pin = load_protocol(repo, protocol_path, protocol_sha)
    a, first, old_pins = previous.authenticate(repo)
    pins = dict(old_pins, **TOP32_PINS)
    require(canonical(protocol["input_pins"]) == canonical(pins), "frozen historical metadata pins differ")
    second = authenticate_top32(repo, a, first, old_pins)
    priors = [first, second]
    spec = selection_spec(a, priors)
    selected = previous.select_panel(a, spec)
    require(len(selected) == len({r["source_ordinal"] for r in selected}) == 64
            and not set(spec["excluded_previous_source_ordinals"]) & {r["source_ordinal"] for r in selected},
            "STOP whole panel: new64 locator count/duplicate/exclusion failure")
    test_hashes = {entry["shard_sha256"] for entry in a["consumed_query_ledger"] if entry["split"] == "test"}
    require(all(original.ROWS <= row["source_ordinal"] < 10_000_000
                and row["shard_sha256"] not in test_hashes
                and not any(entry["split"] == "train" and entry["interval"][0] <= row["source_ordinal"] < entry["interval"][1]
                            for entry in a["consumed_query_ledger"]) for row in selected),
            "STOP whole panel: FIRST1M/historical consumed/test scope")
    ledger = historical_ledger(a, first, old_pins) + [dict(
        split="train", source_ordinals=second["source_ordinals"], count=64,
        panel_pin=second["panel_pin"], duplicate_audit_pin=second["audit_pin"], terminal_pin=second["terminal_pin"])]
    pins["fixed48_protocol"] = protocol_pin
    authority = dict(a)
    authority.update(SCOPE)
    authority.update(schema="borsuk-cohere-fixed48-fresh64-metadata-authority-v1", input_pins=pins,
                     evidence_base_commit=EVIDENCE_BASE, original_evidence_base_commit=a["evidence_base_commit"],
                     original_selection=a["selection"], selection=spec,
                     original_consumed_query_ledger_sha256=value_sha(a["consumed_query_ledger"]),
                     consumed_query_ledger=ledger, prior_panels_vector_hashes=priors,
                     original_top32_missing_closure_artifacts=MISSING, original_top32_terminal_roster_count=34,
                     original_top32_required_roster_count=38,
                     duplicate_policy="Before GT: compare new raw/normalized vectors with FIRST1M, all historical consumed inputs, BOTH prior64 hashsets and each other; STOP whole panel, never replace/resample")
    panel = dict(SCOPE, schema="borsuk-cohere-fixed48-fresh64-locators-v1", input_pins=pins,
                 source_receipt_sha256=a["source_receipt"]["sha256"], corpus=a["corpus"], selection=spec,
                 consumed_query_ledger_sha256=value_sha(ledger), selected_sha256=value_sha(selected),
                 selected=selected, prior_panels_vector_hashes=priors)
    return authority, panel


def publish(repo, protocol_path, protocol_sha, output):
    output = Path(output).absolute()
    require(output.name not in ("", ".", ".."), "new output directory required")
    parent = previous.open_directory(output.parent)
    try:
        require(not os.path.lexists(output), "output already exists; never overwrite")
        authority, panel = prepare(repo, protocol_path, protocol_sha)
        os.mkdir(output.name, mode=0o700, dir_fd=parent)
        directory = os.open(output.name, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=parent)
        try:
            location = Path(f"/proc/self/fd/{directory}")
            original.write_new(location / "authority.json", authority)
            authority_pin = previous.file_pin(location / "authority.json")
            panel["authority_sha256"] = authority_pin["sha256"]
            original.write_new(location / "panel.json", panel)
            panel_pin = previous.file_pin(location / "panel.json")
            freeze = dict(SCOPE, schema="borsuk-cohere-fixed48-fresh64-root-freeze-v1",
                          authority_sha256=authority_pin["sha256"], panel_sha256=panel_pin["sha256"],
                          selected_locators_sha256=panel["selected_sha256"], selection_count=64,
                          protocol_pin=authority["input_pins"]["fixed48_protocol"],
                          root_action="Independently authenticate and freeze locators before vector/GT construction; no resampling")
            original.write_new(location / "root-freeze.json", freeze)
            verification = dict(SCOPE, schema="borsuk-cohere-fixed48-fresh64-metadata-verification-v1",
                                artifacts={"authority.json": authority_pin, "panel.json": panel_pin,
                                           "root-freeze.json": previous.file_pin(location / "root-freeze.json")},
                                input_pins=authority["input_pins"], original_population_size=original.POPULATION,
                                excluded_previous_count=128, population_size=POPULATION,
                                interval_subtraction_bijection=True, both_previous64_source_disjoint=True,
                                corpus_known_consumed_test_scopes_disjoint=True,
                                authenticated_train_shards=len(authority["ordered_train_shards"]),
                                selected_count=64, selected_sha256=panel["selected_sha256"], network_requests=0)
            original.write_new(location / "verification.json", verification)
            os.fsync(directory)
        finally:
            os.close(directory)
        os.fsync(parent)
    finally:
        os.close(parent)
    return verification


def self_check():
    require(not sys.flags.optimize, "self-check requires assertions enabled; -O forbidden")
    import time
    from unittest.mock import patch

    start = time.monotonic()

    def rejects(call):
        try:
            call()
        except (ValueError, KeyError, TypeError, OSError):
            return
        raise AssertionError("invalid metadata/output was accepted")

    # Invented shards, proofs and prior-panel locators: never select the real panel.
    sha = lambda text: hashlib.sha256(text.encode()).hexdigest()
    shard = dict(ordinal=0, key="synthetic/train.parquet", bytes=1, sha256=sha("shard"),
                 rows=10_000_000, source_start=0, source_end_exclusive=10_000_000)
    a = dict(schema="borsuk-cohere-first1m-fresh64-metadata-authority-v1", bucket="synthetic",
             dataset_id="synthetic", dataset_content_sha256=sha("dataset"), train_rows=10_000_000,
             embedding_column="emb", source_receipt=dict(bytes=1, sha256=sha("receipt")),
             ordered_train_shards=[shard], proofs={}, evidence_base_commit="synthetic",
             corpus={k: dict(bytes=1, sha256=sha(k)) for k in ("raw", "order", "root_manifest", "sq8")},
             old_consumed_panel=dict(artifacts={"queries.raw": dict(bytes=1, sha256=sha("consumed"))}),
             consumed_query_ledger=[dict(split="train", interval=[1_000_000, 1_002_000]),
                                    dict(split="train", interval=[1_005_000, 1_006_000])])
    geometry = dict(candidate_interval=[original.ROWS, 10_000_000],
                    excluded_consumed_intervals=[[1_000_000, 1_002_000], [1_005_000, 1_006_000]],
                    eligible_intervals=original.INTERVALS, population_size=original.POPULATION,
                    roster_sha256=value_sha(a["ordered_train_shards"]))
    seed = hashlib.sha256(original.SEED).digest()
    a["selection"] = dict(**geometry, population_sha256=value_sha(geometry), count=64,
                          seed_text=original.SEED.decode(), seed_sha256=seed.hex(),
                          seed_integer=int.from_bytes(seed, "big"), python=FIXED_PROTOCOL["python"],
                          rule=original.RULE, failure_policy=original.FAILURE_POLICY)
    original.validate_population(a)
    first = dict(query_ordinals=list(range(64)), source_ordinals=list(range(1_002_000, 1_002_064)),
                 raw_sha256=[sha(f"first-raw-{i}") for i in range(64)],
                 normalized_sha256=[sha(f"first-unit-{i}") for i in range(64)],
                 normalization="synthetic", raw_hash_dtype="synthetic-f32", normalized_hash_dtype="synthetic-f64",
                 must_check_before_ground_truth=True)
    with tempfile.TemporaryDirectory(prefix="cohere-fixed48-synthetic-") as temporary:
        root = Path(temporary)
        repo = root / "repo"
        pins = {}

        def store(name, path, value):
            target = repo / path
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(canonical(value))
            return dict(path=path, **previous.file_pin(target))

        for name in ("source_population_authority", "previous_consumed_panel", "previous_duplicate_audit"):
            pins[name] = store(name, f"docs/research/synthetic/{name}.json", {"synthetic": name})
        first["audit_pin"] = pins["previous_duplicate_audit"]
        spec = previous.selection_spec(a, first)
        selected = previous.select_panel(a, spec)
        ledger = historical_ledger(a, first, pins)
        authority = dict(a, **previous.SCOPE)
        authority.update(schema="borsuk-cohere-top32-fresh64-metadata-authority-v1", selection=spec,
                         original_selection=a["selection"], consumed_query_ledger=ledger,
                         original_consumed_query_ledger_sha256=value_sha(a["consumed_query_ledger"]),
                         prior64_vector_hashes=first, input_pins=pins)
        bodies = {"top32_authority": authority}
        synthetic_pins = {"top32_authority": store("top32_authority", TOP32_PINS["top32_authority"]["path"], authority)}
        panel = dict(previous.SCOPE, schema="borsuk-cohere-top32-fresh64-locators-v1", input_pins=pins,
                     source_receipt_sha256=a["source_receipt"]["sha256"], corpus=a["corpus"], selection=spec,
                     consumed_query_ledger_sha256=value_sha(ledger), selected_sha256=value_sha(selected),
                     selected=selected, prior64_vector_hashes=first,
                     authority_sha256=synthetic_pins["top32_authority"]["sha256"])
        for name in ("top32_panel", "top32_execution_panel"):
            bodies[name] = panel
            synthetic_pins[name] = store(name, TOP32_PINS[name]["path"], panel)
        freeze = dict(previous.SCOPE, schema="borsuk-cohere-top32-fresh64-root-freeze-v1",
                      authority_sha256=panel["authority_sha256"], panel_sha256=synthetic_pins["top32_panel"]["sha256"],
                      selected_locators_sha256=panel["selected_sha256"], selection_count=64)
        bodies["top32_root_freeze"] = freeze
        synthetic_pins["top32_root_freeze"] = store("top32_root_freeze", TOP32_PINS["top32_root_freeze"]["path"], freeze)
        artifacts = {name + ".json": {k: synthetic_pins["top32_" + key][k] for k in ("bytes", "sha256")}
                     for name, key in (("authority", "authority"), ("panel", "panel"), ("root-freeze", "root_freeze"))}
        bodies["top32_verification"] = dict(previous.SCOPE, schema="borsuk-cohere-top32-fresh64-metadata-verification-v1",
                                             artifacts=artifacts, input_pins=pins, selected_count=64,
                                             selected_sha256=panel["selected_sha256"])
        audit = dict(schema="borsuk-cohere-top32-duplicate-audit-v1", passed=True, replacement_allowed=False,
                     complete_historical_coverage=False, new_rows_audited=64, previous64_rows_audited=64,
                     indexed_rows_audited=original.ROWS, selected_locators_sha256=panel["selected_sha256"],
                     previous64_authority=first, config_sha256=sha("config"), normalization=first["normalization"],
                     raw_hash_dtype=first["raw_hash_dtype"], unit_hash_dtype=first["normalized_hash_dtype"],
                     raw_sha256=[sha(f"second-raw-{i}") for i in range(64)],
                     unit_sha256=[sha(f"second-unit-{i}") for i in range(64)],
                     inputs={n: a["corpus"][k] for n, k in (("source.raw", "raw"), ("source-order.u64", "order"),
                              ("source-root.json", "root_manifest"), ("source-sq8.bin", "sq8"))})
        audit["inputs"]["consumed-queries.raw"] = a["old_consumed_panel"]["artifacts"]["queries.raw"]
        bodies["top32_duplicate_audit"] = audit
        for name in ("top32_verification", "top32_duplicate_audit"):
            synthetic_pins[name] = store(name, TOP32_PINS[name]["path"], bodies[name])
        terminal_artifacts = {f"unused-{i}.json": dict(bytes=1, sha256=sha(str(i))) for i in range(32)}
        terminal_artifacts.update({n: {k: synthetic_pins[key][k] for k in ("bytes", "sha256")}
                                   for n, key in (("screen/panel.json", "top32_execution_panel"),
                                                  ("screen/duplicate-audit.json", "top32_duplicate_audit"))})
        bodies["top32_terminal"] = dict(schema="borsuk-cohere-top32-coverage-spot-v1",
                                         status="failed", exit_code=1, original_exit_code=1, old_fail_preserved=True,
                                         helper_config_sha256=sha("config"), artifacts=terminal_artifacts,
                                         source_archive_sha256=sha("archive"), source_commit="synthetic")
        bodies["top32_root_independent_audit"] = dict(schema="borsuk-cohere-top32-closed-offline-audit-v1",
            original_execution_status="FAIL", scientific_report_status="FAIL",
            original_exit_status=1, original_execution_qualified=False, scientific_report_qualified=False,
            original_source_root_missing=True, final_seal_and_decision_authority_missing=True,
            physical_page_manifest_body_verified=False,
            old_fail_preserved=True, missing_closure_artifacts=MISSING, terminal_bodies_authenticated=34,
            source_archive_sha256=sha("archive"), source_commit="synthetic")
        for name in ("top32_terminal", "top32_root_independent_audit"):
            synthetic_pins[name] = store(name, TOP32_PINS[name]["path"], bodies[name])
        protocol_path = "docs/research/synthetic/protocol.json"
        protocol = dict(FIXED_PROTOCOL, input_pins=dict(pins, **synthetic_pins))
        protocol_pin = store("protocol", protocol_path, protocol)
        with patch.object(previous, "authenticate", return_value=(a, first, pins)), patch.dict(TOP32_PINS, synthetic_pins, clear=True):
            second = authenticate_top32(repo, a, first, pins)
            priors = [first, second]
            spec = selection_spec(a, priors)
            assert len(spec["excluded_previous_source_ordinals"]) == 128
            assert spec["population_size"] == original.POPULATION - 128 == POPULATION
            assert spec["seed_integer"] == int.from_bytes(hashlib.sha256(SEED.encode()).digest(), "big")
            # Exhaustive small-space bijection plus boundary checks over all real-size intervals.
            removed = list(range(1, 129))
            intervals = previous.subtract_ordinals([[0, 260]], removed)
            assert [original.rank_to_ordinal(i, intervals) for i in range(132)] == [0] + list(range(129, 260))
            cursor = 0
            for low, high in spec["eligible_intervals"]:
                assert original.rank_to_ordinal(cursor, spec["eligible_intervals"]) == low
                assert original.rank_to_ordinal(cursor + high - low - 1, spec["eligible_intervals"]) == high - 1
                cursor += high - low
            rejects(lambda: original.rank_to_ordinal(cursor, spec["eligible_intervals"]))
            for field, value in (("source_ordinals", first["source_ordinals"]),
                                 ("source_ordinals", second["source_ordinals"][:-1]),
                                 ("source_ordinals", [999_999] + second["source_ordinals"][1:]),
                                 ("source_ordinals", [True] + second["source_ordinals"][1:]),
                                 ("source_ordinals", [second["source_ordinals"][1]] + second["source_ordinals"][1:]),
                                 ("raw_sha256", first["raw_sha256"]),
                                 ("raw_sha256", [second["raw_sha256"][1]] + second["raw_sha256"][1:]),
                                 ("normalized_sha256", ["invalid"] + second["normalized_sha256"][1:])):
                rejects(lambda: selection_spec(a, [first, dict(second, **{field: value})]))
            for name in ("one", "two"):
                publish(repo, protocol_path, protocol_pin["sha256"], root / name)
            outputs = lambda directory: {p.name: p.read_bytes() for p in directory.iterdir()}
            before = outputs(root / "one")
            assert before == outputs(root / "two")
            assert set(before) == {"authority.json", "panel.json", "root-freeze.json", "verification.json"}
            results = {name: json.loads(body) for name, body in before.items()}
            assert all(all(result[k] == v for k, v in SCOPE.items()) for result in results.values())
            assert results["panel.json"]["prior_panels_vector_hashes"] == priors
            for name, pin in results["verification.json"]["artifacts"].items():
                assert previous.file_pin(root / "one" / name) == pin
            new = results["panel.json"]["selected"]
            assert len(new) == len({row["source_ordinal"] for row in new}) == 64
            assert not {row["source_ordinal"] for row in new} & set(spec["excluded_previous_source_ordinals"])
            assert all(row["shard_key"] == shard["key"] and row["local_row"] == row["source_ordinal"] for row in new)
            rejects(lambda: publish(repo, protocol_path, protocol_pin["sha256"], root / "one"))
            assert outputs(root / "one") == before
            for kind in ("file", "directory", "fifo", "symlink"):
                output = root / kind
                if kind == "file": output.write_text("preserve")
                elif kind == "directory": output.mkdir()
                elif kind == "fifo": os.mkfifo(output)
                else: output.symlink_to(root / "absent")
                rejects(lambda: publish(repo, protocol_path, protocol_pin["sha256"], output))
            for name, pin in dict(synthetic_pins, protocol=protocol_pin).items():
                path = repo / pin["path"]
                body = path.read_bytes()
                path.write_bytes(body[:-1] + b" ")
                rejects(lambda: publish(repo, protocol_path, protocol_pin["sha256"], root / "rejected"))
                assert not (root / "rejected").exists()
                path.write_bytes(body)
            # Even repinning a changed terminal cannot turn an unbound audit or old FAIL into PASS.
            terminal_path = TOP32_PINS["top32_terminal"]["path"]
            for edit in (lambda d: d["artifacts"]["screen/duplicate-audit.json"].update(sha256="0" * 64),
                         lambda d: d["artifacts"]["screen/duplicate-audit.json"].update(bytes=1),
                         lambda d: d.update(status="complete", exit_code=0)):
                changed = copy.deepcopy(bodies["top32_terminal"])
                edit(changed)
                TOP32_PINS["top32_terminal"] = store("terminal", terminal_path, changed)
                rejects(lambda: authenticate_top32(repo, a, first, pins))
            TOP32_PINS["top32_terminal"] = store("terminal", terminal_path, bodies["top32_terminal"])
            path = repo / protocol_path
            body = path.read_bytes()
            path.unlink()
            for kind in ("fifo", "directory", "symlink"):
                if kind == "fifo": os.mkfifo(path)
                elif kind == "directory": path.mkdir()
                else: path.symlink_to(root / "one" / "panel.json")
                rejects(lambda: prepare(repo, protocol_path, protocol_pin["sha256"]))
                rejects(lambda: previous.read_checked(repo, protocol_pin))
                if kind == "directory": path.rmdir()
                else: path.unlink()
            path.write_bytes(body)
            for pin in (dict(protocol_pin, bytes=previous.LIMIT + 1), dict(protocol_pin, sha256="invalid"),
                        dict(protocol_pin, path="docs/research/source.raw")):
                rejects(lambda: previous.read_checked(repo, pin))
            for field, value in (("seed_text", "wrong"), ("count", 65), ("python", {}), ("old_fail_preserved", False)):
                changed = dict(protocol, **{field: value})
                changed_pin = store("protocol", protocol_path, changed)
                rejects(lambda: prepare(repo, protocol_path, changed_pin["sha256"]))
    require(time.monotonic() - start <= 55, "self-check exceeded 55 seconds")
    print(json.dumps(dict(self_check="passed", synthetic_only=True, excluded=128,
                          population_size=POPULATION, selected=64, metadata_only=True, network_requests=0,
                          elapsed_seconds=round(time.monotonic() - start, 3))))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--self-check", action="store_true")
    parser.add_argument("repo", nargs="?", type=Path)
    parser.add_argument("protocol_path", nargs="?", type=Path)
    parser.add_argument("protocol_sha", nargs="?")
    parser.add_argument("new_output", nargs="?", type=Path)
    args = parser.parse_args()
    positional = (args.repo, args.protocol_path, args.protocol_sha, args.new_output)
    if args.self_check:
        if any(value is not None for value in positional):
            parser.error("--self-check takes no positional arguments")
    elif any(value is None for value in positional):
        parser.error("required: REPO PROTOCOLPATH PROTOCOLSHA NEWOUTPUT")
    try:
        if args.self_check:
            self_check()
        else:
            receipt = publish(*positional)
            print(json.dumps(dict(selected=64, population_size=POPULATION,
                                  selected_sha256=receipt["selected_sha256"], **SCOPE)))
    except (ValueError, KeyError, TypeError, OSError) as error:
        parser.exit(1, f"STOP: {error}\n")
