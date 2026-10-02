#!/usr/bin/env python3
"""CLI: REPO NEW_OUTPUT_DIR, or --self-check (stdlib, bounded metadata only).

Publish authority.json, panel.json, root-freeze.json and verification.json with
borsuk-cohere-top32-fresh64-{metadata-authority,locators,root-freeze,
metadata-verification}-v1 schemas. The last file is the completion receipt.
Root qualification and raw/normalized duplicate checks remain pending.
"""

import argparse
import copy
import hashlib
import json
import os
from pathlib import Path
import random
import stat
import sys
import tempfile

import select_cohere_1m_fresh64 as original


require = original.require
canonical = original.canonical
value_sha = original.value_sha
BASE = "docs/research/performance-architecture-20260930/semantic-1m/"
SEED = "borsuk-cohere-first1m-top32-coverage-fresh64-v1"
POPULATION = 8_996_936
LIMIT = 4 << 20
RULE = original.RULE
PINS = {
    "preregister": dict(path=BASE + "cohere-top32-coverage/preregister.md", bytes=1833,
                        sha256="8c134c4ecfc0662d555c6c197419ceb3da4623c23285973d9582005886858e13"),
    "prospective_protocol": dict(path=BASE + "cohere-top32-coverage/prospective-protocol.json", bytes=2378,
                                sha256="b4547ffd131a72e1f5cd2e244a26d47743892c39da1c2cf3a7c48500c9c9048f"),
    "previous_root_freeze": dict(path=BASE + "cohere-panel-tools/root-freeze.json", bytes=868,
                                sha256="c0af609473b8eee64ab0264c9458edffb7891557c830012aec7a7b37f39fd535"),
}
SCOPE = dict(authority_pending=False, root_pending=True, qualification=False,
             metadata_only=True, locator_metadata_only=True,
             vector_or_truth_bodies_opened=False, raw_normalized_duplicate_gate_pending=True,
             ground_truth_constructed=False, ann_measured=False,
             complete_historical_coverage=False, original_quality_status="FAIL",
             original_quality_fail_unchanged=True)
PROOFS = {"candidate", "stress", "registered_test", "v271", "v277", "v278", "source_build",
          "source_terminal", "source_verification", "old_seal", "old_identity", "old_verification",
          "dev64_config", "confirm936_config"}


def valid_sha(value):
    return isinstance(value, str) and len(value) == 64 and all(c in "0123456789abcdef" for c in value)


def open_directory(path):
    """Walk descriptors so neither the directory nor any ancestor can be a symlink."""
    path = Path(path).absolute()
    require(".." not in path.parts, "directory traversal forbidden")
    descriptor = os.open(path.anchor, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        for part in path.parts[1:]:
            child = os.open(part, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=descriptor)
            os.close(descriptor)
            descriptor = child
        return descriptor
    except BaseException:
        os.close(descriptor)
        raise


def read_checked(repo, pin, raw=False):
    """Bounded, pinned regular metadata; O_NONBLOCK also makes FIFO rejection prompt."""
    relative = Path(pin["path"])
    require(not relative.is_absolute() and ".." not in relative.parts
            and relative.parts[:2] == ("docs", "research")
            and (relative.suffix == ".json" or relative.as_posix() == PINS["preregister"]["path"]),
            "only repository research metadata inputs allowed")
    require(valid_sha(pin["sha256"]) and type(pin["bytes"]) is int
            and 0 < pin["bytes"] <= LIMIT, "invalid/bounded metadata pin required")
    directory = open_directory(repo)
    descriptor = None
    try:
        for part in relative.parts[:-1]:
            child = os.open(part, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=directory)
            os.close(directory)
            directory = child
        descriptor = os.open(relative.name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=directory)
        info = os.fstat(descriptor)
        require(stat.S_ISREG(info.st_mode) and info.st_size == pin["bytes"] and info.st_size <= LIMIT,
                "input must be a bounded regular file with pinned bytes")
        # The existing reader opens this already-authenticated descriptor, never a replaced pathname.
        if not raw:
            return original.read_pinned(Path(f"/proc/self/fd/{descriptor}"), pin["sha256"], pin["bytes"])
        with os.fdopen(os.dup(descriptor), "rb") as stream:
            body = stream.read(LIMIT + 1)
        require(len(body) == pin["bytes"] and hashlib.sha256(body).hexdigest() == pin["sha256"],
                "metadata byte/SHA256 identity differs")
        return body
    finally:
        if descriptor is not None:
            os.close(descriptor)
        os.close(directory)


def authenticate(repo, *, historical_metadata_replay=False):
    pins = copy.deepcopy(PINS)
    read_checked(repo, pins["preregister"], raw=True)
    protocol = read_checked(repo, pins["prospective_protocol"])
    require(protocol["schema"] == "borsuk-cohere-top32-fresh-coverage-prospective-v1"
            and protocol["seed_text"] == SEED and type(protocol["count"]) is int and protocol["count"] == 64
            and protocol["replacement_allowed"] is False and protocol["old_fail_preserved"] is True
            and protocol["raw_normalized_duplicate_gate_pending"] is True
            and protocol["selected_queries_must_exclude_previous_64"] is True
            and protocol["complete_historical_coverage"] is False, "prospective protocol differs")
    for name in ("source_population_authority", "previous_consumed_panel", "previous_duplicate_audit",
                 "previous_quality_recount", "previous_quality_terminal"):
        pins[name] = protocol[name]
    a = read_checked(repo, pins["source_population_authority"])
    require(a["schema"] == "borsuk-cohere-first1m-fresh64-metadata-authority-v1"
            and a["authority_pending"] is False and a["root_pending"] is False
            and a["metadata_only"] is True and a["vector_or_truth_bodies_opened"] is False
            and a["complete_historical_coverage"] is False and a["qualification"] is False,
            "original metadata authority/scope differs")
    require(a["bucket"] == original.BUCKET and a["dataset_id"] == "cohere-large-10m-768"
            and a["dataset_content_sha256"] == original.CONTENT_SHA
            and a["train_rows"] == 10_000_000 and a["embedding_column"] == "emb",
            "original CoHere source identity differs")
    receipt = a["source_receipt"]
    require(receipt["sha256"] == original.RECEIPT_SHA and receipt["bytes"] == 135_298
            and receipt["key"] == original.PUBLICATION + "/STAGING_COMPLETE.json", "source receipt differs")
    pins["source_receipt"] = receipt
    roster = original.receipt_roster(read_checked(repo, receipt))
    require(canonical(a["ordered_train_shards"]) == canonical(roster), "source shard roster differs")
    require(set(a["proofs"]) == PROOFS, "missing original source/consumption proofs")
    proofs = {}
    for name, pin in a["proofs"].items():
        pins["source_proof_" + name] = pin
        proofs[name] = read_checked(repo, pin)
    original.authenticate_bindings(a, proofs)
    original.validate_population(a, historical_metadata_replay=historical_metadata_replay)

    previous = read_checked(repo, pins["previous_consumed_panel"])
    original.validate_panel(previous["selected"], a, historical_metadata_replay=historical_metadata_replay)
    expected_previous = dict(schema="borsuk-cohere-first1m-fresh64-locators-v1", metadata_only=True,
                             root_pending=False, qualification=False, complete_historical_coverage=False,
                             vector_or_truth_bodies_opened=False,
                             authority_sha256=pins["source_population_authority"]["sha256"],
                             source_receipt_sha256=receipt["sha256"], corpus=a["corpus"], selection=a["selection"],
                             consumed_query_ledger_sha256=value_sha(a["consumed_query_ledger"]),
                             selected_sha256=value_sha(previous["selected"]), selected=previous["selected"])
    require(canonical(previous) == canonical(expected_previous), "old panel identity/order differs")
    freeze = read_checked(repo, pins["previous_root_freeze"])
    require(freeze["schema"] == "borsuk-cohere-fresh64-root-freeze-v1"
            and freeze["authority_sha256"] == previous["authority_sha256"]
            and freeze["panel_sha256"] == pins["previous_consumed_panel"]["sha256"]
            and freeze["selected_locators_sha256"] == previous["selected_sha256"]
            and freeze["selection_count"] == 64 and freeze["root_pending"] is False
            and freeze["independent_rank_mapping_equal"] is True
            and freeze["complete_historical_coverage"] is False, "old root freeze differs")
    audit = read_checked(repo, pins["previous_duplicate_audit"])
    require(audit["schema"] == "borsuk-cohere-semantic-1m-vector-audit-v1"
            and audit["passed"] is True and audit["replacement_allowed"] is False
            and audit["complete_historical_coverage"] is False and audit["new_rows_audited"] == 64
            and audit["selected_locators_sha256"] == previous["selected_sha256"]
            and canonical(audit["consumed_query_ledger"]) == canonical(a["consumed_query_ledger"]),
            "old duplicate audit/panel binding differs")
    for input_name, source_name in (("source.raw", "raw"), ("source-order.u64", "order"),
                                    ("source-root.json", "root_manifest")):
        require(all(audit["inputs"][input_name][k] == a["corpus"][source_name][k]
                    for k in ("bytes", "sha256")), "duplicate audit source binding differs")
    require(all(audit["inputs"]["consumed-queries.raw"][k] == a["old_consumed_panel"]["artifacts"]["queries.raw"][k]
                for k in ("bytes", "sha256")), "duplicate audit consumed queries differ")
    for field in ("raw_sha256", "unit_sha256"):
        hashes = audit[field]
        require(isinstance(hashes, list) and len(hashes) == len(set(hashes)) == 64
                and all(valid_sha(h) for h in hashes), "old64 vector hashes missing/duplicate/invalid")
    # This authenticates old evidence only; no fresh vector duplicate check has occurred.
    prior = dict(query_ordinals=list(range(64)), source_ordinals=[r["source_ordinal"] for r in previous["selected"]],
                 raw_sha256=audit["raw_sha256"], normalized_sha256=audit["unit_sha256"],
                 normalization=audit["normalization"], raw_hash_dtype=audit["raw_hash_dtype"],
                 normalized_hash_dtype=audit["unit_hash_dtype"], audit_pin=pins["previous_duplicate_audit"],
                 must_check_before_ground_truth=True)
    recount = read_checked(repo, pins["previous_quality_recount"])
    terminal = read_checked(repo, pins["previous_quality_terminal"])
    require(recount["scientific_status"] == "FAIL" and recount["frozen_arm_closed"] is True
            and recount["execution_status"] == 0 and recount["terminal_sha256"] == pins["previous_quality_terminal"]["sha256"]
            and recount["queries"] == 64 and recount["hits"]["10"] == 584
            and terminal["status"] == "complete" and terminal["exit_code"] == 0,
            "original quality FAIL must remain closed and unchanged")
    return a, prior, pins


def subtract_ordinals(intervals, ordinals):
    require(isinstance(intervals, list) and intervals, "eligible intervals required")
    end = -1
    for low, high in intervals:
        require(type(low) is int and type(high) is int and 0 <= low < high and low >= end,
                "intervals must be ascending, half-open, nonoverlapping")
        end = high
    require(all(type(n) is int for n in ordinals) and len(set(ordinals)) == len(ordinals),
            "excluded ordinals must be unique integers")
    require(all(any(low <= n < high for low, high in intervals) for n in ordinals),
            "excluded ordinal outside eligible population")
    removed = sorted(ordinals)
    result, cursor = [], 0
    for low, high in intervals:
        start = low
        while cursor < len(removed) and removed[cursor] < high:
            n = removed[cursor]
            if start < n:
                result.append([start, n])
            start = n + 1
            cursor += 1
        if start < high:
            result.append([start, high])
    require(sum(high - low for low, high in result) == sum(high - low for low, high in intervals) - len(removed),
            "interval subtraction population differs")
    # Reassembling every resulting interval and removed singleton proves no gaps or overlaps.
    pieces = sorted(result + [[n, n + 1] for n in removed])
    cursor = 0
    for low, high in intervals:
        start = low
        while cursor < len(pieces) and pieces[cursor][0] < high:
            piece = pieces[cursor]
            require(piece[0] == start and piece[1] <= high, "subtraction gap/overlap")
            start = piece[1]
            cursor += 1
        require(start == high, "subtraction gap")
    require(cursor == len(pieces), "subtraction out-of-bounds interval")
    return result


def selection_spec(a, prior):
    removed = prior["source_ordinals"]
    require(len(removed) == len(set(removed)) == 64, "exactly64 unique old locators required")
    intervals = subtract_ordinals(a["selection"]["eligible_intervals"], removed)
    population = sum(high - low for low, high in intervals)
    require(a["selection"]["population_size"] == 8_997_000
            and population == 8_997_000 - 64 == POPULATION, "fresh population must be 8996936")
    seed = hashlib.sha256(SEED.encode("utf-8")).digest()
    geometry = dict(candidate_interval=a["selection"]["candidate_interval"],
                    excluded_consumed_intervals=a["selection"]["excluded_consumed_intervals"],
                    excluded_previous_source_ordinals=sorted(removed), eligible_intervals=intervals,
                    population_size=population, roster_sha256=value_sha(a["ordered_train_shards"]))
    return dict(**geometry, population_sha256=value_sha(geometry), count=64, seed_text=SEED,
                seed_sha256=seed.hex(), seed_integer=int.from_bytes(seed, "big"),
                seed_derivation="SHA256 UTF-8 seed text; entire digest as unsigned big-endian integer",
                python=dict(a["selection"]["python"]),
                rule=RULE, failure_policy=original.FAILURE_POLICY)


def select_panel(a, spec):
    ranks = random.Random(spec["seed_integer"]).sample(range(spec["population_size"]), 64)
    panel = []
    for query, rank in enumerate(ranks):
        source = original.rank_to_ordinal(rank, spec["eligible_intervals"])
        index, local = original.locate(source, a["ordered_train_shards"])
        shard = a["ordered_train_shards"][index]
        panel.append(dict(query_ordinal=query, source_ordinal=source, eligible_rank=rank,
                          shard_ordinal=index, shard_key=shard["key"], shard_sha256=shard["sha256"],
                          shard_bytes=shard["bytes"], local_row=local, embedding_column="emb"))
    return panel


def validate_panel(panel, a, prior, spec):
    require(canonical(spec) == canonical(selection_spec(a, prior)), "frozen seed/population/rule differs")
    require(len(panel) == len({r["source_ordinal"] for r in panel}) == 64,
            "STOP whole panel: duplicate/count failure")
    require(canonical(panel) == canonical(select_panel(a, spec)), "STOP whole panel: locator/order differs")
    test_hashes = {entry["shard_sha256"] for entry in a["consumed_query_ledger"] if entry["split"] == "test"}
    for row in panel:
        n = row["source_ordinal"]
        require(type(n) is int and original.ROWS <= n < 10_000_000
                and n not in prior["source_ordinals"]
                and not any(entry["split"] == "train" and entry["interval"][0] <= n < entry["interval"][1]
                            for entry in a["consumed_query_ledger"])
                and row["shard_sha256"] not in test_hashes, "STOP whole panel: corpus/consumed/test scope")


def prepare(repo):
    a, prior, pins = authenticate(repo)
    spec = selection_spec(a, prior)
    selected = select_panel(a, spec)
    validate_panel(selected, a, prior, spec)
    ledger = copy.deepcopy(a["consumed_query_ledger"])
    ledger.append(dict(split="train", source_ordinals=prior["source_ordinals"], count=64,
                       panel_pin=pins["previous_consumed_panel"], duplicate_audit_pin=pins["previous_duplicate_audit"]))
    authority = dict(a)
    authority.update(SCOPE)
    authority.update(schema="borsuk-cohere-top32-fresh64-metadata-authority-v1", input_pins=pins,
                     evidence_base_commit="5ed9f640a1ace544e9aea89f2c02cde2dab17030",
                     original_evidence_base_commit=a["evidence_base_commit"],
                     original_selection=a["selection"], selection=spec,
                     original_consumed_query_ledger_sha256=value_sha(a["consumed_query_ledger"]),
                     consumed_query_ledger=ledger, previous_source_ordinals=prior["source_ordinals"],
                     prior64_vector_hashes=prior,
                     duplicate_policy="Before GT: compare fresh raw/normalized vectors with FIRST1M, all original consumed inputs, previous64 hash lists and each other; STOP whole panel, never replace/resample")
    panel = dict(SCOPE, schema="borsuk-cohere-top32-fresh64-locators-v1", input_pins=pins,
                 source_receipt_sha256=a["source_receipt"]["sha256"], corpus=a["corpus"], selection=spec,
                 consumed_query_ledger_sha256=value_sha(ledger), selected_sha256=value_sha(selected),
                 selected=selected, prior64_vector_hashes=prior)
    return authority, panel


def publish(repo, output):
    output = Path(output).absolute()
    require(output.name not in ("", ".", ".."), "new output directory required")
    parent = open_directory(output.parent)
    try:
        # Existing directories, files, FIFOs and even dangling symlinks all fail before any input read.
        require(not os.path.lexists(output), "output already exists; never overwrite")
        authority, panel = prepare(repo)
        os.mkdir(output.name, mode=0o700, dir_fd=parent)
        directory = os.open(output.name, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=parent)
        try:
            location = Path(f"/proc/self/fd/{directory}")
            original.write_new(location / "authority.json", authority)
            authority_pin = file_pin(location / "authority.json")
            panel["authority_sha256"] = authority_pin["sha256"]
            original.write_new(location / "panel.json", panel)
            panel_pin = file_pin(location / "panel.json")
            freeze = dict(SCOPE, schema="borsuk-cohere-top32-fresh64-root-freeze-v1",
                          authority_sha256=authority_pin["sha256"], panel_sha256=panel_pin["sha256"],
                          selected_locators_sha256=panel["selected_sha256"], selection_count=64,
                          selected_shards=len({r["shard_ordinal"] for r in panel["selected"]}),
                          root_action="Authenticate metadata, then freeze before vector/GT construction; no resampling")
            original.write_new(location / "root-freeze.json", freeze)
            verification = dict(SCOPE, schema="borsuk-cohere-top32-fresh64-metadata-verification-v1",
                                artifacts={"authority.json": authority_pin, "panel.json": panel_pin,
                                           "root-freeze.json": file_pin(location / "root-freeze.json")},
                                input_pins=authority["input_pins"], original_population_size=8_997_000,
                                excluded_previous_count=64, population_size=POPULATION,
                                interval_subtraction_bijection=True, previous64_source_disjoint=True,
                                corpus_known_consumed_test_scopes_disjoint=True,
                                authenticated_train_shards=len(authority["ordered_train_shards"]),
                                selected_count=64, selected_sha256=panel["selected_sha256"],
                                network_requests=0, new_vector_duplicates_checked=False)
            original.write_new(location / "verification.json", verification)
            os.fsync(directory)
        finally:
            os.close(directory)
        os.fsync(parent)
    finally:
        os.close(parent)
    return verification


def file_pin(path):
    body = path.read_bytes()
    return dict(bytes=len(body), sha256=hashlib.sha256(body).hexdigest())


def self_check():
    require(not sys.flags.optimize, "self-check requires assertions enabled; -O forbidden")
    import subprocess
    import time

    start = time.monotonic()

    def rejects(call):
        try:
            call()
        except (ValueError, KeyError, TypeError, OSError):
            return
        raise AssertionError("invalid metadata/output was accepted")

    assert subtract_ordinals([[0, 4], [6, 9]], [0, 1, 3, 6, 8]) == [[2, 3], [7, 8]]
    assert subtract_ordinals([[0, 2], [2, 4]], [1, 2]) == [[0, 1], [3, 4]]
    assert subtract_ordinals([[10, 74]], list(range(10, 74))) == []
    for intervals, removed in [([[0, 4]], [-1]), ([[0, 4]], [4]), ([[0, 4]], [1, 1]),
                               ([[0, 3], [2, 5]], []), ([[2, 2]], []), ([[3, 5], [0, 2]], []),
                               ([[0, 3]], [True])]:
        rejects(lambda: subtract_ordinals(intervals, removed))
    intervals = [[2, 5], [7, 12]]
    eligible = [2, 3, 4, 7, 8, 9, 10, 11]
    for mask in range(1 << len(eligible)):
        removed = [n for bit, n in enumerate(eligible) if mask & (1 << bit)]
        remaining = subtract_ordinals(intervals, removed)
        want = [n for n in eligible if n not in removed]
        assert [original.rank_to_ordinal(rank, remaining) for rank in range(len(want))] == want
        rejects(lambda: original.rank_to_ordinal(len(want), remaining))

    repo = Path(__file__).absolute().parent.parent
    # Record every readable open, including descriptor-backed helper reads. Only pinned
    # metadata, our temporary JSON outputs, and Python code may be read by this check.
    opened = []

    def audit_open(event, args):
        if event != "open" or isinstance(args[0], int) or args[2] & (os.O_DIRECTORY | os.O_WRONLY | os.O_CREAT):
            return
        path = os.fsdecode(args[0])
        if path.startswith("/proc/self/fd/"):
            path = str(Path(path).resolve())
        opened.append(path)

    sys.addaudithook(audit_open)
    authority, panel = prepare(repo)
    a, prior, pins = authenticate(repo)
    spec = panel["selection"]
    assert len(panel["selected"]) == 64 and spec["population_size"] == POPULATION
    rejects(lambda: selection_spec(a, dict(prior, source_ordinals=prior["source_ordinals"][:-1])))
    rejects(lambda: selection_spec(a, dict(prior, source_ordinals=prior["source_ordinals"][:63] + [prior["source_ordinals"][0]])))
    # Independently derived by shifting original eligible ranks over removed singleton ranks.
    source_ordinals = [r["source_ordinal"] for r in panel["selected"]]
    assert source_ordinals[:3] == [8_344_841, 6_354_772, 3_457_253]
    assert hashlib.sha256(json.dumps(source_ordinals, separators=(",", ":")).encode()).hexdigest() == (
        "7fca9221410d2d912828abfa151217384d5dcd4482032e752fab3b42d9c8d72c")
    assert not set(prior["source_ordinals"]) & {r["source_ordinal"] for r in panel["selected"]}
    assert canonical((authority, panel)) == canonical(prepare(repo))
    for row in panel["selected"]:
        shard = a["ordered_train_shards"][row["shard_ordinal"]]
        assert row["source_ordinal"] == shard["source_start"] + row["local_row"]
        assert 0 <= row["local_row"] < shard["rows"]
    for n in [prior["source_ordinals"][0], 999_999, 1_000_000, 1_001_999, 1_005_000, 1_005_999]:
        bad = copy.deepcopy(panel["selected"])
        bad[0]["source_ordinal"] = n
        rejects(lambda: validate_panel(bad, a, prior, spec))
    bad = copy.deepcopy(panel["selected"])
    bad[1] = dict(bad[0], query_ordinal=1)
    rejects(lambda: validate_panel(bad, a, prior, spec))
    for field, value in [("seed_text", "wrong"), ("seed_integer", 1), ("population_size", POPULATION - 1),
                         ("rule", "replacement sampling")]:
        altered = dict(spec, **{field: value})
        rejects(lambda: validate_panel(panel["selected"], a, prior, altered))

    with tempfile.TemporaryDirectory(prefix="cohere-top32-metadata-check-") as work:
        work = Path(work)
        fixture = work / "repo"
        fixture.mkdir()
        # Only authenticated JSON/preregister metadata is copied; never the repository's data bodies.
        bodies = {}
        for pin in pins.values():
            path = fixture / pin["path"]
            body = read_checked(repo, pin, raw=True)
            bodies[pin["path"]] = body
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(body)
        first, second = work / "first", work / "second"
        publish(fixture, first)
        publish(fixture, second)
        assert sorted(p.name for p in first.iterdir()) == ["authority.json", "panel.json", "root-freeze.json", "verification.json"]
        assert {p.name: p.read_bytes() for p in first.iterdir()} == {p.name: p.read_bytes() for p in second.iterdir()}
        results = {p.name: json.loads(p.read_bytes()) for p in first.iterdir()}
        assert all(all(result[k] == v for k, v in SCOPE.items()) for result in results.values())
        assert results["verification.json"]["new_vector_duplicates_checked"] is False
        assert results["panel.json"]["selected"] == panel["selected"]
        assert results["panel.json"]["prior64_vector_hashes"] == prior
        for name, pin in results["verification.json"]["artifacts"].items():
            assert file_pin(first / name) == pin
        assert results["panel.json"]["authority_sha256"] == file_pin(first / "authority.json")["sha256"]
        before = {p.name: p.read_bytes() for p in first.iterdir()}
        rejects(lambda: publish(fixture, first))
        assert before == {p.name: p.read_bytes() for p in first.iterdir()}
        link = work / "output-link"
        link.symlink_to(first, target_is_directory=True)
        rejects(lambda: publish(fixture, link))
        dangling = work / "dangling"
        dangling.symlink_to(work / "absent")
        rejects(lambda: publish(fixture, dangling))
        empty = work / "empty"
        empty.mkdir()
        rejects(lambda: publish(fixture, empty))
        occupied = work / "occupied"
        occupied.write_text("preserve me")
        rejects(lambda: publish(fixture, occupied))
        assert occupied.read_text() == "preserve me"
        fifo = work / "output-fifo"
        os.mkfifo(fifo)
        rejects(lambda: publish(fixture, fifo))
        linked_repo = work / "linked-repo"
        linked_repo.symlink_to(fixture, target_is_directory=True)
        rejects(lambda: prepare(linked_repo))
        rejects(lambda: publish(fixture, linked_repo / "output"))

        for name in ["prospective_protocol", "preregister", "source_receipt", "source_population_authority",
                     "previous_consumed_panel", "previous_root_freeze", "previous_duplicate_audit",
                     "source_proof_source_build", "source_proof_old_identity", "previous_quality_recount"]:
            pin = pins[name]
            path = fixture / pin["path"]
            body = bodies[pin["path"]]
            path.write_bytes(body + b" ")
            rejects(lambda: publish(fixture, work / "rejected"))
            assert not (work / "rejected").exists()
            path.write_bytes(body)
        # Equal-size source/roster/old-panel/protocol/hash-list mutations must fail on SHA, too.
        for name, edit in [
            ("source_population_authority", lambda d: d["ordered_train_shards"][0].update(sha256="0" * 64)),
            ("source_population_authority", lambda d: d["selection"].update(population_size=8_997_001)),
            ("previous_consumed_panel", lambda d: d["selected"].reverse()),
            ("prospective_protocol", lambda d: d.update(seed_text="wrong")),
            ("previous_duplicate_audit", lambda d: d["raw_sha256"].__setitem__(1, d["raw_sha256"][0])),
        ]:
            pin = pins[name]
            path = fixture / pin["path"]
            changed = json.loads(bodies[pin["path"]])
            edit(changed)
            changed_body = json.dumps(changed, separators=(",", ":")).encode()
            changed_body = changed_body.ljust(len(bodies[pin["path"]]), b" ")
            assert len(changed_body) == pin["bytes"]
            path.write_bytes(changed_body)
            rejects(lambda: prepare(fixture))
            # Exercise semantic validation independently of immutable input hashes.
            if name == "source_population_authority":
                if changed["selection"]["population_size"] != 8_997_000:
                    rejects(lambda: original.validate_population(changed))
                else:
                    roster = original.receipt_roster(read_checked(fixture, pins["source_receipt"]))
                    assert canonical(changed["ordered_train_shards"]) != canonical(roster)
            elif name == "previous_consumed_panel":
                rejects(lambda: original.validate_panel(changed["selected"], a))
            path.write_bytes(bodies[pin["path"]])
        pin = pins["prospective_protocol"]
        path = fixture / pin["path"]
        body = bodies[pin["path"]]
        path.unlink()
        other = work / "other.json"
        other.write_bytes(body)
        path.symlink_to(other)
        rejects(lambda: prepare(fixture))
        path.unlink()
        os.mkfifo(path)
        rejects(lambda: prepare(fixture))
        path.unlink()
        path.mkdir()
        rejects(lambda: prepare(fixture))
        path.rmdir()
        path.write_bytes(body)
        directory = fixture / "docs/research"
        directory.rename(fixture / "docs/real-research")
        directory.symlink_to("real-research", target_is_directory=True)
        rejects(lambda: prepare(fixture))
        directory.unlink()
        (fixture / "docs/real-research").rename(directory)
        bad_pin = dict(pin, bytes=LIMIT + 1)
        rejects(lambda: read_checked(fixture, bad_pin))
        rejects(lambda: read_checked(fixture, dict(pin, path="docs/research/source.raw")))
        result = subprocess.run([sys.executable, "-O", str(Path(__file__).absolute()), "--self-check"],
                                capture_output=True, text=True, timeout=5)
        assert result.returncode == 1 and "-O forbidden" in result.stderr
        for flag in ("--seed", "--count"):
            result = subprocess.run([sys.executable, str(Path(__file__).absolute()), flag, "1"],
                                    capture_output=True, text=True, timeout=5)
            assert result.returncode == 2
        metadata_paths = {str(root / pin["path"]) for root in (repo, fixture) for pin in pins.values()}
        output_paths = {str(root / name) for root in (first, second) for name in
                        ("authority.json", "panel.json", "root-freeze.json", "verification.json")}
        output_paths.add(str(occupied))
        # os.open(dir_fd=...) audit events expose only the basename; the subsequent
        # descriptor-backed reads above record the full authenticated pathname.
        metadata_names = {Path(path).name for path in metadata_paths}
        unexpected = [path for path in opened if not (
            path in metadata_paths | output_paths or Path(path).suffix in (".py", ".pyc")
            or (not Path(path).is_absolute() and path in metadata_names))]
        assert not unexpected, unexpected
    require(time.monotonic() - start <= 55, "self-check exceeded 55 seconds")
    print(json.dumps(dict(self_check="passed", population_size=POPULATION, selected=64,
                          selected_sha256=panel["selected_sha256"], metadata_only=True,
                          network_requests=0, elapsed_seconds=round(time.monotonic() - start, 3))))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--self-check", action="store_true")
    parser.add_argument("repo", nargs="?", type=Path)
    parser.add_argument("new_output_dir", nargs="?", type=Path)
    args = parser.parse_args()
    if args.self_check:
        if args.repo is not None or args.new_output_dir is not None:
            parser.error("--self-check takes no positional arguments")
    elif args.repo is None or args.new_output_dir is None:
        parser.error("required: REPO NEW_OUTPUT_DIR")
    try:
        if args.self_check:
            self_check()
        else:
            verification = publish(args.repo, args.new_output_dir)
            print(json.dumps(dict(selected=64, population_size=POPULATION,
                                  selected_sha256=verification["selected_sha256"], **SCOPE)))
    except (ValueError, KeyError, TypeError, OSError) as error:
        parser.exit(1, f"STOP: {error}\n")
