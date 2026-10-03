#!/usr/bin/env python3
"""Offline preregistered audit of the closed historical paired100k diagnostic.

CLI: REPO NEW_OUTPUT_JSON [--truth-dir DIR] | --self-check [REPO]
Optional DIR contains relaion-truth.u32 and cohere-truth.u32, whose exact
25600-byte identities must match the original authenticated diagnostic configs.
Without those bodies, intersections are explicitly native-accounting-derived.
No network, search, reranking, parameter selection, or native execution occurs.
Run in a <=256MiB, no-swap, one-CPU cgroup with a <=120-second deadline.
"""

import argparse
import copy
import gzip
import hashlib
import json
import math
import os
from pathlib import Path
import resource
import signal
import struct
import tempfile
import time


ROOT = Path("docs/research/performance-architecture-20260930/semantic-1m/hierarchical-cells/paired100k")
# Immutable controls from 76fff47e; f85c8f1d supplies two missing gzip bodies.
PINS = {
    "coverage-ceiling-preregistration.json": (2017, "c86aa3c45ecd560ee93e2fdf5ed0a647f46f8ed57c6406536a1997358a51dbcf"),
    "a0001/aws-terminal.json": (4916, "a0ca521980142f869e7be74b10802926459674cc06ec2e7bf6e11214c8065bf2"),
    "a0001/aws-reservation.json": (25126, "a9e0c653cf7ec442e93cf99b0d2db519981258489b347738d2d29cea4d6c0572"),
    "a0001/aws-launch.json": (348, "5e201469b045a061d480e8225a1540e659c7aeeda16ad1b812245df5070adbc6"),
    "a0001/aws-closeout.json": (135, "b72a14426e80eecc659c776191623630596c673815f2ebfe2f3ebca68a26fe0c"),
    "a0001/root-decision.json": (1115, "34d75611d232e45bce4b159837f59dc67bb487004ba86bf1be488119cab27e80"),
}
OPTIONS = dict(blocks_per_cell=4, boundary_beam=24, fetch_policy="whole_cell",
               max_cell_bytes=16777216, max_cell_gets=32, max_cells=32,
               max_query_payload_bytes=134217728, max_refinement_bytes=0,
               max_refinement_gets=1, max_source_bytes=0, max_source_gets=1, primary_beam=8)
OUTPUTS = ("config.json", "source-qualification.json", "native-proof.json", "tool-versions.json",
           "staging.json", "local-config.json", "admission.json", "summary.json", "resources.json",
           "worker-cgroup.json", "cleanup.json", "relaion-panel-binding.json", "cohere-panel-binding.json",
           "measurement/frozen-config.json", "measurement/receipt.json",
           *(f"measurement/{d}-{n}" for d in ("relaion", "cohere") for n in
             ("writer.json", "writer.log", "build.json", "build.log", "diagnose.json", "diagnose.log", "diagnostic.jsonl")))
ARTIFACTS = ("test-resources.txt", "run-closed.log", *("screen/" + n for n in OUTPUTS))
STAGES = ("routing", "local_nomination", "final_ranking")
COUNTERS = ("submitted_gets", "requested_bytes", "verified_bytes", "failed_gets")
BODY_CAP = 16 << 20


def require(condition, message):
    if not condition:
        raise ValueError(message)


def encoded(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()


def identity_of(body):
    return dict(bytes=len(body), sha256=hashlib.sha256(body).hexdigest())


def pin_of(value):
    return {k: value[k] for k in ("bytes", "sha256")}


def read_body(root, name, expected):
    require(not Path(name).is_absolute() and ".." not in Path(name).parts, "relative evidence path")
    require(type(expected["bytes"]) is int and 0 <= expected["bytes"] <= BODY_CAP, "bounded body geometry")
    root = Path(root).resolve()
    body = None
    # Authenticate each retained representation; gzip hashes refer to decoded
    # terminal body bytes, never compressed bytes substituted for native SHA.
    for suffix in ("", ".gz"):
        path = root / (str(name) + suffix)
        if not os.path.lexists(path):
            continue
        require(path.is_file() and not path.is_symlink() and path.resolve().is_relative_to(root),
                "regular evidence body: " + str(path))
        opener = gzip.open if suffix else open
        with opener(path, "rb") as stream:
            current = stream.read(expected["bytes"] + 1)
        require(identity_of(current) == pin_of(expected), "body bytes/SHA: " + str(path))
        body = current
    require(body is not None, "missing authenticated body: " + str(root / name))
    return body


def integer(value, low=0, high=100000):
    require(type(value) is int and low <= value <= high, "integer domain")
    return value


def roster(values):
    require(type(values) is list and len(values) <= 100000, "bounded ID roster")
    for value in values:
        integer(value, high=99999)
    result = set(values)
    require(len(result) == len(values), "unique ID roster")
    return result


def aggregate(values):
    require(bool(values), "nonempty aggregate")
    for value in values:
        integer(value, high=100)
    mean = sum(values) / (len(values) * 100)
    p05 = sorted(values)[math.ceil(.05 * len(values)) - 1]
    return dict(total_hits=sum(values), mean_hits_at_100=mean, p05_hits=p05,
                quality_gate_possible=mean >= .98 and p05 >= 95)


def counter_summary(values):
    return dict(total=sum(values), mean=sum(values)/len(values), minimum=min(values), maximum=max(values))


def load_truth(path, pin):
    require(pin["bytes"] == 25600, "truth geometry: 64 unique truth100 rows")
    body = read_body(Path(path).parent, Path(path).name, pin)
    ids = struct.unpack("<6400I", body)
    rows = [list(ids[start:start+100]) for start in range(0, 6400, 100)]
    for row in rows:
        require(len(roster(row)) == 100, "unique truth100")
    return rows


def reduce_trace(body, config, config_pin, proof, truths=None):
    """Validate original order/freeze; independently intersect only supplied GT."""
    for name, want in dict(first=0, count=64, top_k=100, truth_width=100, options=OPTIONS).items():
        require(config[name] == want, "frozen diagnostic config: " + name)
    require(config["truth"]["bytes"] == 25600, "truth panel geometry")
    if truths is not None:
        require(len(truths) == 64 and all(len(roster(row)) == 100 for row in truths), "unique direct truth100")
    prefix, prefix_bytes, frozen, terminal, losses = hashlib.sha256(), 0, False, False, 0
    rows, startup = [], {}
    for index, line in enumerate(body.splitlines(keepends=True)):
        require(not terminal and line.endswith(b"\n") and len(line) <= 8 << 20, "event cap/newline/terminal order")
        event = json.loads(line)
        phase = event.get("phase")
        if index == 0:
            expected = dict(phase="identity", schema="borsuk-hierarchical-cells-diagnostic-v3",
                            config_sha256=config_pin["sha256"], candidate_root_sha256=config["candidate_root"]["sha256"],
                            requests_sha256=config["requests"]["sha256"], first=0, count=64, top_k=100,
                            options=OPTIONS, physical_s3_measured=False,
                            **{n+"_source_sha256": proof["sources"][n]["sha256"] for n in ("module", "binary")})
            require(all(event.get(n) == v for n, v in expected.items()), "trace/source/config identity")
            startup = {n: event[n] for n in ("startup", "startup_directory")}
        elif phase == "query_frozen":
            require(not frozen and len(rows) < 64 and event["ordinal"] == len(rows)
                    and type(event["ordinal"]) is int and event["truth_opened"] is False, "query ordinal/freeze/truth order")
            trace = event["trace"]
            primary, covered, nominated = (roster(trace[n+"_ids"]) for n in ("primary", "covered", "nominated"))
            require(type(trace["returned"]) is list and len(trace["returned"]) <= 100, "bounded top100")
            returned = roster([entry["id"] for entry in trace["returned"]])
            require(primary <= covered and returned <= nominated <= covered, "returned subset nominated subset covered")
            require(event["underfilled"] is (len(returned) < 100), "underfill flag")
            for entry in trace["returned"]:
                integer(entry["ordinal"], high=99999)
                require(type(entry["score"]) in (int, float) and math.isfinite(entry["score"]), "finite returned score")
            a = trace["accounting"]
            for tier in ("directory", "source", "refinement"):
                require(all(type(a[tier][n]) is int and a[tier][n] == 0 for n in COUNTERS), "inactive local read tier")
            reads = {n: integer(a["whole_cell"][n], high=16 << 20) for n in COUNTERS}
            require(0 < reads["submitted_gets"] <= 32 and reads["requested_bytes"] > 0
                    and reads["verified_bytes"] == reads["requested_bytes"] and reads["failed_gets"] == 0,
                    "original local read charges")
            require(len(a["waves"]) == 1 and all(a["waves"][0].get(n) == v for n, v in
                    dict(stage="whole_cell", dependency=1, max_parallel_gets=1,
                         **{n: reads[n] for n in COUNTERS if n != "failed_gets"}).items()), "serial whole-cell local read wave")
            timing = {stage: {n: integer(trace[stage][n], high=10**15) for n in ("wall_ns", "process_cpu_ns")}
                      for stage in STAGES}
            row = dict(ordinal=len(rows), roster_sizes=dict(primary=len(primary), covered=len(covered),
                       nominated=len(nominated), returned=len(returned)), local_reads=reads, stage_timing_ns=timing)
            if truths is not None:
                gold = set(truths[len(rows)])
                row["independent_hits"] = {n: len(gold & ids) for n, ids in
                                           dict(primary=primary, coverage=covered, nomination=nominated, returned=returned).items()}
            rows.append(row)
        elif phase == "all_queries_frozen":
            require(not frozen and len(rows) == 64 and event == dict(phase=phase, first=0, count=64,
                    truth_opened=False, trace_prefix_bytes=prefix_bytes, trace_prefix_sha256=prefix.hexdigest()),
                    "authenticated exact64 freeze bytes/SHA/order")
            frozen = True
        elif phase == "loss_attribution":
            require(frozen and losses < 64 and type(event["ordinal"]) is int and event["ordinal"] == losses
                    and event["truth_sha256"] == config["truth"]["sha256"], "loss ordinal/freeze/truth SHA")
            loss, row = event["loss"], rows[losses]
            for n in ("truth_count", "primary_hits", "boundary_hits", "nomination_hits", "returned_hits",
                      "router_misses", "boundary_recovered", "boundary_misses", "local_nomination_misses", "final_ranking_misses"):
                integer(loss[n], high=100)
            p, c, n, r = (loss[key] for key in ("primary_hits", "boundary_hits", "nomination_hits", "returned_hits"))
            require(loss["truth_count"] == 100 and p <= c and 0 <= r <= n <= c <= 100, "nested truth100 counts")
            expected_losses = dict(router_misses=100-p, boundary_recovered=c-p, boundary_misses=100-c,
                                   local_nomination_misses=c-n, final_ranking_misses=n-r)
            require(all(loss[key] == value for key, value in expected_losses.items()), "native loss accounting formulas")
            require(all(hits <= row["roster_sizes"][name] for name, hits in
                        dict(primary=p, covered=c, nominated=n, returned=r).items()), "hits cannot exceed roster size")
            if truths is not None:
                require(row.pop("independent_hits") == dict(primary=p, coverage=c, nomination=n, returned=r),
                        "independent truth intersections disagree with native accounting")
            row.update(coverage_ceiling_hits=c, nomination_ceiling_hits=n, returned_hits=r, native_loss=loss)
            losses += 1
        elif phase == "terminal":
            require(frozen and losses == 64 and all(event.get(n) == value for n, value in
                    dict(status="DIAGNOSTIC", complete=True, queries=64, truth_opened=True,
                         scientific_qualification=False, quality_or_performance_claim=False).items()), "complete diagnostic terminal")
            terminal = True
        else:
            raise ValueError("unexpected diagnostic phase")
        if not frozen:
            prefix.update(line)
            prefix_bytes += len(line)
    require(terminal, "partial diagnostic is not authority")
    result = dict(queries=64, ceiling_basis="independent_truth_intersections" if truths is not None else
                  "authenticated_native_loss_accounting", truth_sha256=config["truth"]["sha256"],
                  truth_uniqueness_verification="independently_validated" if truths is not None else
                  "attested_by_authenticated_native_decompose_loss_not_independently_rechecked",
                  freeze=dict(bytes=prefix_bytes, sha256=prefix.hexdigest()), per_query=rows, startup_local_reads=startup,
                  local_reads={n: counter_summary([row["local_reads"][n] for row in rows]) for n in COUNTERS},
                  stage_timing_ns={stage: {n: counter_summary([row["stage_timing_ns"][stage][n] for row in rows])
                                  for n in ("wall_ns", "process_cpu_ns")} for stage in STAGES})
    for name, field in (("coverage_ceiling", "coverage_ceiling_hits"), ("nomination_ceiling", "nomination_ceiling_hits"),
                        ("returned", "returned_hits")):
        result[name] = aggregate([row[field] for row in rows])
    result["interpretation"] = dict(
        perfect_ranking_of_fetched_cells_can_meet_original_gate=result["coverage_ceiling"]["quality_gate_possible"],
        full_fetched_sq8_alone_sufficient=False if not result["coverage_ceiling"]["quality_gate_possible"] else None,
        full_fetched_sq8_measured=False, quantization_loss_established=False,
        final_ranking_misses=sum(row["native_loss"]["final_ranking_misses"] for row in rows),
        local_nomination_misses=sum(row["native_loss"]["local_nomination_misses"] for row in rows),
        explanation="Coverage below either gate makes full-fetched SQ8 alone insufficient even with perfect ranking. "
                    "Zero final-ranking misses on the nominated set does not establish quantization loss or exact-score equivalence.")
    return result


def audit(repo, truth_dir=None):
    repo = Path(repo).resolve()
    controls = {name: json.loads(read_body(repo / ROOT, name, dict(bytes=size, sha256=sha)))
                for name, (size, sha) in PINS.items()}
    prereg = controls["coverage-ceiling-preregistration.json"]
    terminal, reservation, launch, close = (controls["a0001/"+name+".json"] for name in
                                            ("aws-terminal", "aws-reservation", "aws-launch", "aws-closeout"))
    require(terminal["phase"] == terminal["status"] == "complete" and
            terminal["exit_code"] == terminal["original_exit_code"] == 0, "original execution complete")
    require(close["state"] == "terminated" and close["nodes"] == launch["nodes"] and
            terminal["instance_id"] == launch["instance_id"] in {n["instance_id"] for n in close["nodes"].values()},
            "same terminated original execution0")
    for name in ("source_commit", "source_archive_sha256"):
        require(terminal[name] == reservation[name] == launch[name], "closed controller source binding")
    qualified = reservation["qualification"]
    require(all(terminal[name] == value for name, value in qualified.items() if name not in
                ("config_path", "source_archive_paths")), "reserved terminal identities")
    require(set(terminal["artifacts"]) == set(ARTIFACTS) and
            identity_of(encoded(ARTIFACTS))["sha256"] == terminal["artifact_roster_sha256"], "complete original artifact roster")
    folder = repo / ROOT / "a0001"
    bodies = {name: read_body(folder, name, pin) for name, pin in terminal["artifacts"].items()}
    def get(name):
        return json.loads(bodies["screen/"+name])
    config, receipt, proof, frozen, summary = (get(n) for n in
        ("config.json", "measurement/receipt.json", "native-proof.json", "measurement/frozen-config.json", "summary.json"))
    require(get("source-qualification.json") == qualified, "deployed source qualification")
    require(identity_of(bodies["screen/config.json"])["sha256"] == terminal["config_sha256"], "original config identity")
    for key, value in dict(rows=100000, dimensions=768, metric="cosine", k=100, count=64, first=0,
                           consumed=True, held_out=False, retune_allowed=False).items():
        require(config[key] == value, "historical fixed geometry: " + key)
    require(config["policy"] == {n: v for n, v in OPTIONS.items() if n != "max_query_payload_bytes"}, "unchanged selection")
    require(prereg["unchanged_selection"] == dict(primary_cells=8, boundary_cells=24, max_cells=32, blocks_per_cell=4),
            "preregistered selection")
    require(identity_of(encoded(config["code_sha256"]))["sha256"] == terminal["code_identity_sha256"] and
            identity_of(encoded(config["refs"]))["sha256"] == terminal["refs_identity_sha256"], "controller/reference identities")
    require(qualified["source_archive_paths"] == sorted(qualified["source_archive_paths"]) and
            terminal["source_file_count"] == 401 and
            identity_of(encoded(qualified["source_archive_paths"]))["sha256"] == terminal["source_archive_paths_sha256"],
            "closed source archive roster")
    # Small source/metadata bodies only: no native archives or binary execution.
    source_ids = {}
    for name, sha in config["code_sha256"].items():
        path = repo / name
        require(path.is_file(), "original controller source exists")
        source_ids[name] = identity_of(read_body(repo, name, dict(bytes=path.stat().st_size, sha256=sha)))
    refs = {name: json.loads(read_body(repo, pin["path"], pin)) for name, pin in config["refs"].items()}
    manifest = refs["manifest"]
    require(len(manifest["source_sha256"]) == 401 and
            identity_of(encoded(manifest["source_sha256"]))["sha256"] == terminal["native_identity_sha256"],
            "qualified native source inventory identity")
    verification = refs["verification"]
    require(all(verification[n] is True for n in ("qualified", "instance_terminated", "all_terminal_bodies_authenticated",
            "all_current_native_files_match")) and verification["swap_peak_bytes"] == verification["oom_events"] == 0,
            "retained native qualification admission")
    require(receipt["proof"] == proof and receipt["status"] == "DIAGNOSTIC" and receipt["complete"] is True
            and receipt["cleanup_complete"] is True, "complete measurement proof")
    for role, pin in config["native"]["sources"].items():
        require(pin_of(pin) == pin_of(proof["sources"][role]), "native source binding")
        source_ids[pin["path"]] = identity_of(read_body(repo, pin["path"], pin))
    for role in ("source_archive", "gate_log"):
        require(pin_of(proof[role]) == pin_of(config["native"][role]), "native archive/log identity")
    require(proof["source_commit"] == config["native"]["source_commit"], "native source commit")
    require(all(pin_of(proof["binaries"][n]) == pin_of(pin) for n, pin in config["native"]["binaries"].items()),
            "native binary identities")
    frozen_pin = terminal["artifacts"]["screen/measurement/frozen-config.json"]
    require(pin_of(receipt["frozen_config"]) == frozen_pin == pin_of(receipt["config"]) and
            bodies["screen/local-config.json"] == bodies["screen/measurement/frozen-config.json"], "frozen local config")
    require(len(receipt["stages"]) == 6 and all(s["exit_status"] == 0 and s["resource_gate_passed"] is True and
            s["cleanup_complete"] is True for s in receipt["stages"]), "closed native stages")
    admission = get("admission.json")
    require(all(admission[n] == value for n, value in dict(status="ADMITTED", excluded_from_measurement=True,
            quality_promotion=False, disposable_outputs_removed=True).items()), "original real-input admission")
    require(all(get("cleanup.json")[n] is True for n in ("scratch_removed", "monitor_stopped", "sdk_client_closed")), "cleanup")
    counters = get("worker-cgroup.json")
    require(counters["closed"] is True and counters["before"]["path"] == counters["after"]["path"], "resource closure")
    for snapshot in (counters["before"], counters["after"]):
        require(0 < int(snapshot["memory.max"]) <= 2 << 30 and int(snapshot["memory.peak"]) <= 2 << 30
                and snapshot["memory.swap.max"] == "0" and int(snapshot["memory.swap.peak"]) == 0, "original memory/no swap")
        q, p = map(int, snapshot["cpu_max"].split())
        require(q == 2*p and int(snapshot["tasks_max"]) == config["tasks_max"], "original CPU/tasks limit")
    before, after = (dict(line.split() for line in counters[n]["memory.events"].splitlines()) for n in ("before", "after"))
    require(all(before[n] == after[n] == "0" for n in ("oom", "oom_kill", "oom_group_kill")), "original no OOM")
    resources = get("resources.json")
    require(0 <= resources["wall_seconds"] <= 1800 and resources["scratch_bytes"] <= 16 << 30 and
            len(resources["sdk_calls"]) == 18 and not resources["monitor_errors"], "original resource admission")
    items = {item["dataset"]: item for item in frozen["items"]}
    require(set(items) == set(receipt["results"]) == {"relaion", "cohere"}, "paired dataset roster")
    datasets = {}
    for dataset in ("relaion", "cohere"):
        name = "screen/measurement/"+dataset+"-diagnose.json"
        diagnostic = json.loads(bodies[name])
        binding = get(dataset+"-panel-binding.json")
        for key, value in dict(dataset=dataset, first=0, count=64, k=100, metric="cosine", consumed=True).items():
            require(binding[key] == value, "consumed panel binding")
        require(pin_of(receipt["diagnostic_configs"][dataset]) == terminal["artifacts"][name] and
                diagnostic["candidate_root"] == receipt["candidates"][dataset], "diagnostic candidate/config binding")
        for role in ("requests", "truth"):
            require(diagnostic[role] == items[dataset]["inputs"][role] and
                    diagnostic[role]["sha256"] == binding[role+"_sha256"], "authenticated panel identity")
        trace_name = "screen/measurement/"+dataset+"-diagnostic.jsonl"
        require(pin_of(receipt["results"][dataset]) == terminal["artifacts"][trace_name], "terminal/receipt trace binding")
        truths = load_truth(Path(truth_dir)/(dataset+"-truth.u32"), diagnostic["truth"]) if truth_dir is not None else None
        result = reduce_trace(bodies[trace_name], diagnostic, terminal["artifacts"][name], proof, truths)
        original = summary["items"][dataset]
        require(result["returned"]["mean_hits_at_100"] == original["mean_recall_at_100"] and
                result["returned"]["p05_hits"] == original["p05_hits"] and
                result["returned"]["total_hits"] == original["returned_hits"] and
                [row["local_reads"] for row in result["per_query"]] == original["per_query_local_reads"], "independent reduction parity")
        datasets[dataset] = result
    return dict(schema="borsuk-hierarchical-100k-coverage-ceilings-v1", audit_status="VALID",
                preregistration=pin_of(dict(zip(("bytes", "sha256"), PINS["coverage-ceiling-preregistration.json"]))),
                original_terminal=pin_of(dict(zip(("bytes", "sha256"), PINS["a0001/aws-terminal.json"]))),
                original_execution="CLOSED_VALID", instance_id=terminal["instance_id"], instance_state="terminated",
                controls={n: dict(bytes=s, sha256=h) for n, (s, h) in PINS.items()},
                authenticated_terminal_bodies=terminal["artifacts"], authenticated_source_bodies=source_ids,
                selection=prereg["unchanged_selection"], split=prereg["split"], rows=100000, dimensions=768,
                metric="cosine", k=100, datasets=datasets,
                scope=dict(new_ann_or_ranking_execution=False, new_quality_or_performance_measurement=False,
                    physical_s3_ann_measured=False, vendor_claim=False, fresh_holdout=False, scientific_qualification=False,
                    counters="Original whole-cell serial local FileRangeReader calls and requested/verified range bytes; "
                             "startup root/directory reads separate. Auditor evidence file reads excluded.",
                    timing="Original process stage wall/CPU counters only: routing, local_nomination (includes local cell fetching), "
                           "final_ranking. No S3, end-to-end latency, vendor baseline, or full-fetched SQ8 measurement.",
                    truth="Optional exact authenticated truth64 bodies enable independent intersections; otherwise counts "
                          "derive from authenticated native loss_attribution with source-validated unique truth100 and loss formulas.",
                    loss_formulas="boundary_hits=|truth & covered|; nomination_hits=|truth & nominated|; returned_hits=|truth & returned|; "
                                  "boundary_misses=100-boundary_hits; local_nomination_misses=boundary_hits-nomination_hits; "
                                  "final_ranking_misses=nomination_hits-returned_hits"))


def self_check(repo):
    """Real admission plus small synthetic fixtures, never synthetic authority."""
    checks = []

    def rejects(name, action):
        try:
            action()
        except (ValueError, OSError, EOFError, json.JSONDecodeError):
            checks.append(name)
            return
        raise ValueError("self-check accepted invalid input: " + name)

    # Catches interpolated/off-by-one p05 and threshold-boundary mistakes.
    for values, p05 in [([95], 95), (list(range(20)), 0),
                        (list(range(21)), 1), (list(range(64)), 3)]:
        require(aggregate(values)["p05_hits"] == p05, "nearest-rank boundary")
    require(aggregate([98] * 64)["quality_gate_possible"], "inclusive mean gate")
    require(aggregate([95] * 4 + [100] * 60)["quality_gate_possible"], "inclusive p05 gate")
    require(not aggregate([97] * 64)["quality_gate_possible"], "mean gate failure")
    require(not aggregate([94] * 4 + [100] * 60)["quality_gate_possible"], "p05 gate failure")
    rejects("empty aggregate", lambda: aggregate([]))
    rejects("noninteger hits", lambda: aggregate([True]))
    rejects("hit overflow", lambda: aggregate([101]))
    checks.append("nearest-rank and inclusive gate boundaries")

    def fixture():
        zero = dict(submitted_gets=0, requested_bytes=0, verified_bytes=0, failed_gets=0)
        reads = dict(submitted_gets=24, requested_bytes=1024, verified_bytes=1024, failed_gets=0)
        cfg = dict(first=0, count=64, top_k=100, truth_width=100,
                   candidate_root=dict(sha256="1" * 64), requests=dict(sha256="2" * 64),
                   truth=dict(bytes=25600, sha256="3" * 64), options=OPTIONS)
        proof = dict(sources={n: dict(sha256="4" * 64) for n in ("module", "binary")})
        pin = dict(sha256="5" * 64)
        identity = dict(phase="identity", schema="borsuk-hierarchical-cells-diagnostic-v3",
                        config_sha256=pin["sha256"], candidate_root_sha256="1" * 64,
                        requests_sha256="2" * 64, module_source_sha256="4" * 64,
                        binary_source_sha256="4" * 64, first=0, count=64, top_k=100,
                        options=OPTIONS, physical_s3_measured=False, startup=reads,
                        startup_directory=reads)
        events, losses = [identity], []
        for ordinal in range(64):
            c, n, r = (94, 93, 90) if ordinal < 4 else (100, 100, 100)
            trace = dict(primary_ids=list(range(40)),
                         covered_ids=list(range(c)) + list(range(100, 120)),
                         nominated_ids=list(range(n)) + list(range(100, 120)),
                         returned=[dict(id=i, ordinal=i, score=0.5) for i in
                                   list(range(r)) + list(range(100, 200-r))],
                         accounting=dict(directory=zero, source=zero, refinement=zero,
                                         whole_cell=reads, waves=[dict(stage="whole_cell", dependency=1,
                                         max_parallel_gets=1, **reads)], modeled_query_payload_bytes=4096),
                         **{stage: dict(wall_ns=10, process_cpu_ns=11) for stage in STAGES})
            events.append(dict(phase="query_frozen", ordinal=ordinal, truth_opened=False,
                               underfilled=False, trace=trace))
            losses.append(dict(phase="loss_attribution", ordinal=ordinal, truth_sha256="3" * 64,
                               loss=dict(truth_count=100, primary_hits=40, boundary_hits=c,
                                         nomination_hits=n, returned_hits=r, router_misses=60,
                                         boundary_recovered=c-40, boundary_misses=100-c,
                                         local_nomination_misses=c-n, final_ranking_misses=n-r)))
        prefix = b"".join(encoded(e) + b"\n" for e in events)
        events.append(dict(phase="all_queries_frozen", first=0, count=64, truth_opened=False,
                           trace_prefix_bytes=len(prefix), trace_prefix_sha256=identity_of(prefix)["sha256"]))
        events.extend(losses)
        events.append(dict(phase="terminal", status="DIAGNOSTIC", complete=True, queries=64,
                           truth_opened=True, scientific_qualification=False, quality_or_performance_claim=False))
        return events, cfg, pin, proof

    def run(events, cfg, pin, proof, direct=False):
        body = b"".join(encoded(e) + b"\n" for e in events)
        truths = [list(range(100)) for _ in range(64)] if direct else None
        return reduce_trace(body, cfg, pin, proof, truths)

    events, cfg, pin, proof = fixture()
    for direct in (False, True):
        report = run(events, cfg, pin, proof, direct)
        require(report["coverage_ceiling"]["total_hits"] == 6376, "synthetic coverage intersection")
        require(report["nomination_ceiling"]["total_hits"] == 6372, "synthetic nomination intersection")
        require(report["returned"]["total_hits"] == 6360, "synthetic returned intersection")
        require(report["coverage_ceiling"]["p05_hits"] == 94, "synthetic coverage p05")
        require(report["ceiling_basis"] == ("independent_truth_intersections" if direct else
                                           "authenticated_native_loss_accounting"), "basis distinction")
    checks.append("direct intersections and accounting fallback")

    # Resealing a synthetic freeze must not bypass nesting/count/order checks.
    def reseal(items):
        index = next(i for i, e in enumerate(items) if e["phase"] == "all_queries_frozen")
        body = b"".join(encoded(e) + b"\n" for e in items[:index])
        items[index].update(trace_prefix_bytes=len(body), trace_prefix_sha256=identity_of(body)["sha256"])

    mutations = [
        ("freeze SHA tamper", lambda e: e[65].update(trace_prefix_sha256="0" * 64), False),
        ("freeze byte tamper", lambda e: e[65].update(trace_prefix_bytes=0), False),
        ("query order", lambda e: e.__setitem__(slice(1, 3), [e[2], e[1]]), True),
        ("loss order", lambda e: e.__setitem__(slice(66, 68), [e[67], e[66]]), False),
        ("missing ordinal", lambda e: e.pop(64), True),
        ("duplicate ordinal", lambda e: e[2].update(ordinal=0), True),
        ("truth before freeze", lambda e: e.insert(1, e.pop(66)), True),
        ("early truth flag", lambda e: e[1].update(truth_opened=True), True),
        ("returned outside nominees", lambda e: e[1]["trace"]["returned"][0].update(id=99999), True),
        ("nominee outside coverage", lambda e: e[1]["trace"]["nominated_ids"].append(99999), True),
        ("duplicate returned", lambda e: e[1]["trace"]["returned"][0].update(id=1), True),
        ("ID bool", lambda e: e[1]["trace"]["covered_ids"].append(True), True),
        ("ID out of domain", lambda e: e[1]["trace"]["covered_ids"].append(100000), True),
        ("truth denominator", lambda e: e[66]["loss"].update(truth_count=99), False),
        ("loss formula", lambda e: e[66]["loss"].update(boundary_misses=0), False),
        ("truth SHA", lambda e: e[66].update(truth_sha256="0" * 64), False),
        ("accounting false intersection", lambda e: e[66]["loss"].update(primary_hits=39,
            router_misses=61, boundary_recovered=55), False),
        ("incomplete terminal", lambda e: e[-1].update(complete=False), False),
        ("extra terminal", lambda e: e.append(e[-1]), False),
        ("missing terminal", lambda e: e.pop(), False),
        ("policy drift", lambda e: e[0].update(options=dict(OPTIONS, primary_beam=9)), True),
        ("source identity drift", lambda e: e[0].update(module_source_sha256="0" * 64), True),
        ("failed local read", lambda e: e[1]["trace"]["accounting"]["whole_cell"].update(failed_gets=1), True),
    ]
    for name, mutate, seal in mutations:
        items = copy.deepcopy(events)
        mutate(items)
        if seal:
            reseal(items)
        rejects(name, lambda: run(items, cfg, pin, proof, True))
    rejects("unterminated event", lambda: reduce_trace(b"{}", cfg, pin, proof))

    with tempfile.TemporaryDirectory(prefix="hierarchical-ceiling-selfcheck-") as temp:
        root = Path(temp)
        raw = struct.pack("<6400I", *list(range(100)) * 64)
        path = root / "truth.u32"
        path.write_bytes(raw)
        truth_pin = identity_of(raw)
        require(load_truth(path, truth_pin)[0] == list(range(100)), "truth body decoding")
        rejects("truth body SHA", lambda: load_truth(path, dict(truth_pin, sha256="0" * 64)))
        bad = struct.pack("<6400I", *([0] * 6400))
        path.write_bytes(bad)
        rejects("duplicate truth", lambda: load_truth(path, identity_of(bad)))
        bad = struct.pack("<6400I", *([100000] + list(range(99)) + list(range(100)) * 63))
        path.write_bytes(bad)
        rejects("truth domain", lambda: load_truth(path, identity_of(bad)))
        path.write_bytes(raw + b"x")
        rejects("truth geometry", lambda: load_truth(path, identity_of(raw + b"x")))
        body = b"authenticated terminal body\n"
        path = root / "body"
        path.write_bytes(body)
        expected = identity_of(body)
        require(read_body(root, "body", expected) == body, "raw body")
        path.unlink()
        zipped = root / "body.gz"
        zipped.write_bytes(gzip.compress(body))
        require(read_body(root, "body", expected) == body, "gzip fallback")
        zipped.write_bytes(gzip.compress(body + b"x"))
        rejects("gzip body tamper", lambda: read_body(root, "body", expected))
        zipped.write_bytes(b"corrupt gzip")
        rejects("corrupt gzip", lambda: read_body(root, "body", expected))
        zipped.unlink()
        rejects("missing terminal body", lambda: read_body(root, "body", expected))
        path.symlink_to("/etc/passwd")
        rejects("symlink evidence", lambda: read_body(root, "body", expected))
        rejects("path traversal", lambda: read_body(root, "../body", expected))
        for name in ("coverage-ceiling-preregistration.json", "a0001/aws-terminal.json"):
            size, sha = PINS[name]
            original = read_body(Path(repo)/ROOT, name, dict(bytes=size, sha256=sha))
            tampered = root / "control"
            tampered.write_bytes(original.replace(b"0", b"1", 1))
            rejects("immutable control SHA: " + name,
                    lambda: read_body(root, "control", dict(bytes=size, sha256=sha)))
    checks.append("raw/gzip authentication and truth validation")

    # The immutable real record is the integration test; no native rerun.
    actual = audit(repo)
    for dataset, covered, returned in [("relaion", 5513, 4156), ("cohere", 4788, 4408)]:
        item = actual["datasets"][dataset]
        require(item["coverage_ceiling"]["total_hits"] == covered, "real coverage accounting")
        require(item["returned"]["total_hits"] == returned, "real returned accounting")
        require(not item["coverage_ceiling"]["quality_gate_possible"], "real causal ceiling")
        require(item["ceiling_basis"] == "authenticated_native_loss_accounting", "real fallback scope")
    checks.append("all real closed bodies, source binding, freezes and 128 ordinals")
    return dict(status="PASS", checks=checks,
                real_closed_summary={d: {n: actual["datasets"][d][n] for n in
                    ("ceiling_basis", "coverage_ceiling", "nomination_ceiling", "returned", "interpretation")}
                    for d in ("relaion", "cohere")},
                real_closed_inputs_authenticated=True, scientific_qualification=False)


def audit_resources():
    group = Path("/sys/fs/cgroup") / Path(Path("/proc/self/cgroup").read_text().strip().split("::", 1)[1]).relative_to("/")
    snapshot = {name: (group/name).read_text().strip() for name in
                ("memory.max", "memory.peak", "memory.swap.max", "memory.swap.peak", "memory.events", "cpu.max")}
    q, p = map(int, snapshot["cpu.max"].split())
    require(0 < int(snapshot["memory.max"]) <= 256 << 20 and int(snapshot["memory.peak"]) <= 256 << 20,
            "audit requires <=256MiB cgroup")
    require(snapshot["memory.swap.max"] == snapshot["memory.swap.peak"] == "0", "audit requires no-swap cgroup")
    require(0 < q <= p and len(os.sched_getaffinity(0)) == 1, "audit requires one CPU and <=100% quota")
    snapshot.update(path=str(group), cpu_affinity=sorted(os.sched_getaffinity(0)))
    return snapshot


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("repo", nargs="?", type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument("output", nargs="?", type=Path)
    parser.add_argument("--self-check", action="store_true")
    parser.add_argument("--truth-dir", type=Path)
    args = parser.parse_args()
    require((args.self_check and args.output is None and args.truth_dir is None) or
            (not args.self_check and args.output is not None), "use REPO NEW_OUTPUT_JSON or --self-check [REPO]")
    before = audit_resources()
    resource.setrlimit(resource.RLIMIT_AS, (256 << 20, 256 << 20))
    resource.setrlimit(resource.RLIMIT_CPU, (120, 120))
    def deadline_expired(*_):
        raise ValueError("audit 120-second deadline")
    signal.signal(signal.SIGALRM, deadline_expired)
    signal.alarm(120)
    started = time.monotonic()
    result = self_check(args.repo) if args.self_check else audit(args.repo, args.truth_dir)
    after = audit_resources()
    events_before, events_after = (dict(line.split() for line in s["memory.events"].splitlines()) for s in (before, after))
    require(all(events_before[n] == events_after[n] for n in ("oom", "oom_kill", "oom_group_kill")), "audit no OOM")
    result["audit_resources"] = dict(before=before, after=after, wall_seconds=time.monotonic()-started,
                                    deadline_seconds=120, auditor_peak_rss_bytes=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss*1024)
    signal.alarm(0)
    if args.output is not None:
        body = json.dumps(result, indent=2, sort_keys=True, allow_nan=False).encode() + b"\n"
        with args.output.open("xb") as stream:
            stream.write(body)
        print(json.dumps(dict(audit_status="VALID", output=str(args.output), **identity_of(body))))
    else:
        print(json.dumps(result, sort_keys=True))


if __name__ == "__main__":
    main()
