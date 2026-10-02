#!/usr/bin/env python3
"""Reduce sealed v3 diagnostics; never execute search or open query vectors.

CLI: CONFIG CONFIG_SHA NEW_OUTPUT | --replay CONFIG CONFIG_SHA OUTPUT | --self-check
Config: {schema: borsuk-semantic-frozen-offline-config-v1, inputs: {...}}.
Exactly five inputs (scorer_config, measurements, order, truth, measurement_receipt)
are absolute regular-file descriptors {path, bytes, sha256}. The closed receipt
schema and required fields are defined by validate_receipt below. Root must seal
measurements AND that receipt before constructing GT; this reducer authenticates
root's attestation, but cannot prove chronology or native/resource qualification.
Exit 0 means valid evidence, including scientific FAIL; invalid evidence exits 2.
No physical S3 or cold HTTP result is established. Replay only recomputes this
offline reduction and compares the existing canonical JSON; it never overwrites.
"""

from array import array
import copy
import json
import os
from pathlib import Path
import resource
import signal
import struct
import sys
import tempfile

from check_semantic_router_coverage import (
    authenticated, canonical, decode, digest, fields, integer, require, sha,
)
from prepare_semantic_positive_inputs import output_file

CONFIG_SCHEMA = "borsuk-semantic-frozen-offline-config-v1"
RECEIPT_SCHEMA = "borsuk-semantic-closed-measurement-v1"
RESULT_SCHEMA = "borsuk-semantic-frozen-offline-result-v1"
CAPS = dict(scorer_config=65536, measurements=16 << 20, order=8_000_000,
            truth=51200, measurement_receipt=65536)
STAGES = ("nominated_units", "page_closure", "source_scored_units",
          "source_ranked_pages", "sq8_admitted_ranges", "returned_ids")
HASHES = ("binary_sha256", "scorer_source_sha256", "router_source_sha256")
IO = ("leaf_gets", "leaf_bytes", "source_gets", "source_bytes", "sq8_gets", "sq8_bytes")
TIMES = ("query_wall_ns", "query_process_cpu_ns")
MAX_INT = (1 << 128) - 1
CHRONOLOGY = ("driver-owned measure-before-GT requirement; authenticated root receipt "
              "attestation only, chronology not independently proven by reducer")


def exact(value, expected, name):
    require(type(value) is type(expected) and value == expected, name)


def pointer(value, cap):
    fields(value, "path bytes sha256", "artifact")
    require(type(value["path"]) is str and Path(value["path"]).is_absolute(), "absolute artifact")
    integer(value["bytes"], 1, cap, "artifact bytes")
    digest(value["sha256"])
    return value


def read(value, cap):
    pointer(value, cap)
    return authenticated(value["path"], value["sha256"], cap, value["bytes"])


def validate_scorer(c, inputs):
    fields(c, "schema dataset profile rows dimensions first count store_root generation_prefix "
           "generation_root_sha256 scratch_parent requests order max_memory_bytes", "scorer config")
    exact(c["schema"], "borsuk-semantic-router-scorer-config-v3", "v3 only")
    require(c["dataset"] in ("ReLAION", "CoHere"), "dataset")
    rows = integer(c["rows"], 100, 1_000_000, "rows sufficient for GT100")
    integer(c["dimensions"], 1, 768, "dimensions")
    require((c["profile"] == "native100k" and rows <= 100_000) or
            (c["profile"] == "fresh1m" and (rows, c["dimensions"]) == (1_000_000, 768)),
            "scorer profile geometry")
    for key, value in dict(first=0, count=64, max_memory_bytes=512 << 20).items():
        exact(c[key], value, key)
    for key in ("store_root", "scratch_parent"):
        require(type(c[key]) is str and Path(c[key]).is_absolute(), key)
    prefix = c["generation_prefix"]
    require(type(prefix) is str and all(part and part not in (".", "..") and
            all(ch.isascii() and (ch.isalnum() or ch in "_-.") for ch in part)
            for part in prefix.split("/")), "generation prefix")
    digest(c["generation_root_sha256"])
    pointer(c["requests"], 2 << 20)  # Metadata only: NEVER open the query vectors.
    pointer(c["order"], rows * 8)
    exact(c["order"]["bytes"], rows * 8, "order geometry")
    require(c["order"] == inputs["order"] and
            c["requests"]["path"] != c["order"]["path"], "order/config binding")
    require(os.path.abspath(c["requests"]["path"]) not in
            {os.path.abspath(p["path"]) for p in inputs.values()}, "query vectors are not evaluator inputs")
    exact(inputs["truth"]["bytes"], 51200, "truth geometry")


def validate_receipt(r, c, inputs):
    fields(r, "schema scorer_config measurements order requests binary_sha256 scorer_source_sha256 "
           "router_source_sha256 generation_root_sha256 generation_prefix exit_status "
           "resource_gate_passed cleanup_complete native_qualification_passed "
           "measurement_sealed_before_truth", "root receipt")
    exact(r["schema"], RECEIPT_SCHEMA, "closed receipt schema")
    exact(r["exit_status"], 0, "original normal exit 0")
    for key in ("resource_gate_passed", "cleanup_complete", "native_qualification_passed",
                "measurement_sealed_before_truth"):
        exact(r[key], True, "root attestation: " + key)
    for key in ("scorer_config", "measurements", "order"):
        pointer(r[key], CAPS[key])
        require(r[key] == inputs[key], "receipt binding: " + key)
    pointer(r["requests"], 2 << 20)
    require(r["requests"] == c["requests"], "receipt requests binding")
    for key in HASHES:
        digest(r[key])
    for key in ("generation_root_sha256", "generation_prefix"):
        exact(r[key], c[key], "receipt generation binding: " + key)


def ids(value, bound, cap, name, low=0):
    require(type(value) is list and low <= len(value) <= min(bound, cap), name + " count")
    seen = set()
    for item in value:
        integer(item, 0, bound - 1, name)
        require(item not in seen, name + " duplicate")
        seen.add(item)
    return seen


def cover(selected, rows, width):
    """Geometry-only reproduction of cover_pages (including cheapest gap ties)."""
    runs = []
    for page in selected:
        if runs and runs[-1][1] == page:
            runs[-1][1] += 1
        else:
            runs.append([page, page + 1])
    gaps = sorted((runs[i + 1][0] - runs[i][1], i) for i in range(len(runs) - 1))
    joined = {i for _, i in gaps[:max(0, len(runs) - 32)]}
    merged = [runs[0]]
    for i, run in enumerate(runs[1:]):
        if i in joined:
            merged[-1][1] = run[1]
        else:
            merged.append(run)
    return [dict(start=a * 256 * width, end=min(b * 256, rows) * width) for a, b in merged]


def validate_startup(s, c):
    fields(s, "phase truth_opened profile metadata evaluator_payload_charge", "startup")
    exact(s["phase"], "startup", "startup phase")
    exact(s["truth_opened"], False, "startup truth unopened")
    exact(s["profile"], c["profile"], "startup profile")
    exact(s["evaluator_payload_charge"], 32 << 20, "evaluator charge")
    m = s["metadata"]
    fields(m, "metadata staging_wall_ns decode_wall_ns source_head_requests source_head_wall_ns "
           "router_head_requests router_head_wall_ns", "startup metadata")
    for key in set(m) - {"metadata"}:
        integer(m[key], 0, MAX_INT, key)
    require(type(m["metadata"]) is list and 1 <= len(m["metadata"]) <= 32, "metadata roster")
    names = set()
    for item in m["metadata"]:
        fields(item, "name metadata_wave metadata_wave_wall_ns bytes reused_root_bytes retained_root_bytes "
               "local_auth_wall_ns local_copy_wall_ns chunks head_wall_ns logical_head_requests "
               "logical_get_requests payload_buffer_bound_bytes get_wall_ns stream_wall_ns write_wall_ns",
               "metadata item")
        require(type(item["name"]) is str and item["name"] and item["name"] not in names,
                "unique metadata name")
        names.add(item["name"])
        for key in set(item) - {"name"}:
            integer(item[key], 0, MAX_INT, "metadata " + key)


def validate_query(q, ordinal, c):
    fields(q, "phase ordinal truth_opened trace selected_pages ranges returned_ids " +
           " ".join(IO + TIMES) + " stages", "query")
    exact(q["phase"], "frozen_query", "query phase")
    exact(q["ordinal"], ordinal, "ordered query ordinal")
    exact(q["truth_opened"], False, "query truth unopened")
    rows, width = c["rows"], c["dimensions"] + 12
    units, pages = (rows + 31) // 32, (rows + 255) // 256
    fresh = c["profile"] == "fresh1m"
    leaf_cap, page_cap, source_cap = (48, 512, 4096) if fresh else (16, 1024, 2544)
    t = q["trace"]
    fields(t, "ranked_candidate_pages nomination_evaluated_units primary_page discoveries "
           "semantic_leaves semantic_units semantic_seed_additions", "trace")
    leaves = ids(t["semantic_leaves"], 2 * ((units + 63) // 64) - 1, leaf_cap, "leaves", 1)
    if fresh:
        exact(len(leaves), 48, "fixed48 leaf count")
    nominated = ids(t["semantic_units"], units, leaf_cap * 64, "semantic units", 1)
    scored = ids(t["nomination_evaluated_units"], units, source_cap, "SOURCE units", 1)
    ranked = ids(t["ranked_candidate_pages"], pages, page_cap, "SOURCE pages", 1)
    additions = ids(t["semantic_seed_additions"], units, 7, "seed additions")
    closure = {u // 8 for u in nominated}
    require(len(closure) <= page_cap, "closure page cap")
    require(not (additions & nominated) and all(u // 8 == min(closure) for u in additions),
            "seed addition geometry")
    require(all(u // 8 in closure for u in scored) and ranked <= closure, "SOURCE closure geometry")
    integer(t["primary_page"], 0, pages - 1, "primary page")
    exact(t["primary_page"], t["ranked_candidate_pages"][0], "primary ranked page")
    exact(t["discoveries"], [], "semantic scorer has no graph discoveries")
    selected = ids(q["selected_pages"], pages, page_cap, "selected pages", 1)
    require(q["selected_pages"] == sorted(selected) and selected <= ranked, "selected page order/binding")
    require(t["primary_page"] in selected, "primary page admitted")
    require(type(q["ranges"]) is list and 1 <= len(q["ranges"]) <= 32, "range count")
    for interval in q["ranges"]:
        fields(interval, "start end", "range")
        integer(interval["start"], 0, rows * width - 1, "range start")
        integer(interval["end"], interval["start"] + 1, rows * width, "range end")
    require(q["ranges"] == cover(q["selected_pages"], rows, width), "exact physical range geometry")
    ids(q["returned_ids"], rows, 100, "returned IDs", 10)
    for key in IO + TIMES:
        integer(q[key], 0, MAX_INT, key)
    require(q["leaf_gets"] == len(leaves) and
            q["leaf_bytes"] == len(nominated) * (4 + 2 * c["dimensions"]), "logical leaf charges")
    require(1 <= q["source_gets"] <= 128 and 0 < q["source_bytes"] <= 64 << 20, "SOURCE charges")
    require(q["sq8_gets"] == len(q["ranges"]) and q["sq8_bytes"] ==
            sum(r["end"] - r["start"] for r in q["ranges"]) <= 16773120, "SQ8 charges")
    stages = q["stages"]
    fields(stages, "discovery source planning sq8 leaf_peak_inflight", "stages")
    integer(stages["leaf_peak_inflight"], 1, min(16, len(leaves)), "leaf inflight")
    previous = 0
    for key in ("discovery", "source", "planning", "sq8"):
        fields(stages[key], "start_ns end_ns", key + " interval")
        start = integer(stages[key]["start_ns"], previous, q["query_wall_ns"], key + " start")
        previous = integer(stages[key]["end_ns"], start, q["query_wall_ns"], key + " end")


def validate_measurements(body, c, inputs, receipt):
    require(body.endswith(b"\n"), "sealed JSONL final newline")
    lines = body.splitlines()
    require(len(lines) == 68 and all(0 < len(line) <= 256 << 10 for line in lines), "sealed 68-row roster")
    identity, startup = decode(lines[0]), decode(lines[1])
    fields(identity, "schema phase config_sha256 binary_sha256 scorer_source_sha256 router_source_sha256 "
           "physical_s3_measured", "identity")
    exact(identity["schema"], "borsuk-semantic-router-scorer-result-v3", "v3 identity")
    exact(identity["phase"], "identity", "first identity")
    exact(identity["physical_s3_measured"], False, "no physical S3 measurement")
    exact(identity["config_sha256"], inputs["scorer_config"]["sha256"], "identity config binding")
    for key in HASHES:
        exact(identity[key], receipt[key], "identity/receipt " + key)
    validate_startup(startup, c)
    queries = []
    for ordinal, line in enumerate(lines[2:66]):
        q = decode(line)
        validate_query(q, ordinal, c)
        queries.append(q)
    marker, terminal = decode(lines[66]), decode(lines[67])
    fields(marker, "phase count status complete truth_opened summary", "frozen marker")
    for key, value in dict(phase="all_queries_frozen", count=64, status="FROZEN",
                           complete=True, truth_opened=False).items():
        exact(marker[key], value, "marker " + key)
    fields(terminal, "phase summary", "terminal")
    exact(terminal["phase"], "terminal", "last terminal")
    summary = marker["summary"]
    expected = dict(status="FROZEN", complete=True, queries=64, first=0, truth_opened=False,
                    physical_s3_measured=False, config_sha256=inputs["scorer_config"]["sha256"],
                    **{key: receipt[key] for key in HASHES},
                    **{key: c[key] for key in ("dataset", "profile", "rows", "dimensions",
                                               "generation_prefix", "generation_root_sha256")})
    for key in ("requests", "order"):
        for suffix in ("bytes", "sha256"):
            expected[key + "_" + suffix] = c[key][suffix]
    fields(summary, " ".join(expected) + " observed_process_peak_bytes", "summary")
    for key, value in expected.items():
        exact(summary[key], value, "summary binding: " + key)
    integer(summary["observed_process_peak_bytes"], 1, c["max_memory_bytes"], "observed peak")
    # Canonical equality rejects bool/int substitutions in the terminal copy too.
    require(canonical(summary) == canonical(terminal["summary"]), "identical FROZEN summaries")
    return queries, startup, summary


def inverse_order(body, rows):
    require(len(body) == rows * 8, "order length")
    inverse = array("I", [rows]) * rows
    for physical, (logical,) in enumerate(struct.iter_unpack("<Q", body)):
        require(logical < rows and inverse[logical] == rows, "order bijection")
        inverse[logical] = physical
    return inverse


def hits(q, truth, inverse, width):
    t = q["trace"]
    nominated, scored = set(t["semantic_units"]), set(t["nomination_evaluated_units"])
    closure, ranked = {u // 8 for u in nominated}, set(t["ranked_candidate_pages"])
    physical = [inverse[logical] for logical in truth]
    membership = [
        [p // 32 in nominated for p in physical],
        [p // 256 in closure for p in physical],
        [p // 32 in scored for p in physical],
        [p // 256 in ranked for p in physical],
        [any(r["start"] <= p * width < r["end"] for r in q["ranges"]) for p in physical],
    ]
    result = {stage: dict(hits10=sum(m[:10]), hits100=sum(m))
              for stage, m in zip(STAGES, membership)}
    result["returned_ids"] = dict(hits10=len(set(q["returned_ids"][:10]) & set(truth[:10])),
                                   hits100=len(set(q["returned_ids"]) & set(truth)))
    return result


def evaluate(config_path, config_sha256):
    config = decode(authenticated(config_path, config_sha256, 65536))
    fields(config, "schema inputs", "evaluator config")
    exact(config["schema"], CONFIG_SCHEMA, "evaluator schema")
    inputs = config["inputs"]
    fields(inputs, " ".join(CAPS), "input roster")
    for key, cap in CAPS.items():
        pointer(inputs[key], cap)
    require(len({os.path.abspath(p["path"]) for p in inputs.values()}) == len(inputs), "distinct input paths")
    c = decode(read(inputs["scorer_config"], CAPS["scorer_config"]))
    validate_scorer(c, inputs)
    receipt = decode(read(inputs["measurement_receipt"], CAPS["measurement_receipt"]))
    validate_receipt(receipt, c, inputs)
    queries, startup, summary = validate_measurements(
        read(inputs["measurements"], CAPS["measurements"]), c, inputs, receipt)
    inverse = inverse_order(read(inputs["order"], CAPS["order"]), c["rows"])
    for q in queries:
        require(all(any(r["start"] <= inverse[i] * (c["dimensions"] + 12) < r["end"]
                        for r in q["ranges"]) for i in q["returned_ids"]), "returned ID/range binding")
    # First and only truth open: all measurement, receipt, order and ID checks passed.
    truth_body = read(inputs["truth"], 51200)
    per_query = []
    for ordinal, q in enumerate(queries):
        truth = struct.unpack_from("<100q", truth_body, ordinal * 800)
        require(len(set(truth)) == 100 and all(0 <= i < c["rows"] for i in truth), "truth unique logical IDs")
        per_query.append(dict(ordinal=ordinal, stages=hits(q, truth, inverse, c["dimensions"] + 12)))
    aggregate = {}
    for stage in STAGES:
        counts = {key: sum(q["stages"][stage][key] for q in per_query) for key in ("hits10", "hits100")}
        aggregate[stage] = dict(counts, recall10=counts["hits10"] / 640, recall100=counts["hits100"] / 6400)
        for q in per_query:
            counts_q = q["stages"][stage]
            counts_q.update(recall10=counts_q["hits10"] / 10, recall100=counts_q["hits100"] / 100)
    failure, crossing = None, None
    if aggregate["page_closure"]["hits10"] < 608:
        failure, crossing = "discovery", "page_closure"
    elif aggregate["returned_ids"]["hits10"] < 608:
        failure = "downstream"
        crossing = next(stage for stage in STAGES[2:] if aggregate[stage]["hits10"] < 608)
    return dict(schema=RESULT_SCHEMA, config_sha256=config_sha256, inputs=inputs,
                execution_status="SUCCESS", scientific_status="FAIL" if failure else "GO",
                failure_class=failure, first_crossing_stage=crossing,
                eligible_for_cold_measurement=failure is None, launch_authority=False,
                denominators=dict(hits10=640, hits100=6400), floor_hits10=608,
                per_query=per_query, aggregate=aggregate, frozen_summary=summary,
                local_measurement={key: [q[key] for q in queries] for key in TIMES},
                logical_io={key: [q[key] for q in queries] for key in IO}, startup=startup,
                physical_s3_measured=False, cold_http_measured=False,
                chronology_authority=CHRONOLOGY,
                qualification_authority="authenticated root receipt; native/resource/cleanup gates not rerun")


def run(config_path, config_sha256, output, replay=False):
    report = evaluate(config_path, config_sha256)
    body = canonical(report)
    if replay:
        authenticated(output, sha(body), 1 << 20, len(body))
    else:
        with output_file(Path(output), {}) as write:
            write(body)
    return report


def self_check():
    """Small synthetic files only; enforced address-space and wall-clock ceilings."""
    resource.setrlimit(resource.RLIMIT_AS, (256 << 20, 256 << 20))
    signal.alarm(55)
    checks = 0
    with tempfile.TemporaryDirectory(prefix="borsuk-frozen-offline-") as temp:
        base = Path(temp)

        def put(name, body):
            path = base / name
            path.write_bytes(body)
            return dict(path=str(path), bytes=len(body), sha256=sha(body))

        def fixture(change=lambda data: None, boundary=None):
            rows, width = 769, 13
            order = list(reversed(range(rows)))
            order_body = struct.pack("<769Q", *order)
            inputs = dict(order=put("order.u64", order_body))
            c = dict(schema="borsuk-semantic-router-scorer-config-v3", dataset="CoHere",
                     profile="native100k", rows=rows, dimensions=1, first=0, count=64,
                     store_root=str(base / "never-open-store"), generation_prefix="semantic/index",
                     generation_root_sha256="a" * 64, scratch_parent=str(base / "scratch"),
                     requests=dict(path=str(base / "never-open-queries"), bytes=1024, sha256="b" * 64),
                     order=inputs["order"], max_memory_bytes=512 << 20)
            inputs["scorer_config"] = put("scorer.json", canonical(c))
            identity = dict(schema="borsuk-semantic-router-scorer-result-v3", phase="identity",
                            config_sha256=inputs["scorer_config"]["sha256"], physical_s3_measured=False,
                            **{k: str(i) * 64 for i, k in enumerate(HASHES)})
            item = {key: 0 for key in ("metadata_wave metadata_wave_wall_ns bytes reused_root_bytes "
                    "retained_root_bytes local_auth_wall_ns local_copy_wall_ns chunks head_wall_ns "
                    "logical_head_requests logical_get_requests payload_buffer_bound_bytes get_wall_ns "
                    "stream_wall_ns write_wall_ns").split()}
            item.update(name="manifest.json", bytes=100, logical_get_requests=1)
            startup = dict(phase="startup", truth_opened=False, profile="native100k",
                           evaluator_payload_charge=32 << 20, metadata=dict(metadata=[item],
                           staging_wall_ns=1, decode_wall_ns=1, source_head_requests=1,
                           source_head_wall_ns=1, router_head_requests=1, router_head_wall_ns=1))
            records, truths = [identity, startup], []
            for ordinal in range(64):
                physical = [768, *range(99)] if boundary is None else list(range(100))
                if boundary is not None and ordinal < 640 - boundary:
                    physical[9] = 300
                truth = [order[p] for p in physical]
                truths.extend(truth)
                selected = [0, 1, 2, 3] if boundary is None else [0]
                nominated = list(range(25 if boundary is None else 8))
                ranges = cover(selected, rows, width)
                returned = truth if boundary is None else [order[p] for p in range(100)]
                trace = dict(semantic_leaves=[0], semantic_units=nominated,
                             semantic_seed_additions=[], nomination_evaluated_units=nominated,
                             ranked_candidate_pages=selected, primary_page=0, discoveries=[])
                records.append(dict(phase="frozen_query", ordinal=ordinal, truth_opened=False,
                                    trace=trace, selected_pages=selected, ranges=ranges,
                                    returned_ids=returned, leaf_gets=1, leaf_bytes=len(nominated) * 6,
                                    source_gets=1, source_bytes=1000, sq8_gets=len(ranges),
                                    sq8_bytes=sum(r["end"] - r["start"] for r in ranges),
                                    query_wall_ns=10, query_process_cpu_ns=5,
                                    stages=dict(discovery=dict(start_ns=0, end_ns=1),
                                    source=dict(start_ns=1, end_ns=2), planning=dict(start_ns=2, end_ns=3),
                                    sq8=dict(start_ns=3, end_ns=4), leaf_peak_inflight=1)))
            summary = dict(status="FROZEN", complete=True, queries=64, first=0,
                           **{k: c[k] for k in ("dataset", "profile", "rows", "dimensions",
                                               "generation_prefix", "generation_root_sha256")},
                           **{k: identity[k] for k in (*HASHES, "config_sha256")},
                           requests_sha256=c["requests"]["sha256"], requests_bytes=c["requests"]["bytes"],
                           order_sha256=inputs["order"]["sha256"], order_bytes=len(order_body),
                           truth_opened=False, observed_process_peak_bytes=1 << 20, physical_s3_measured=False)
            records.extend([dict(phase="all_queries_frozen", count=64, status="FROZEN", complete=True,
                                 truth_opened=False, summary=summary),
                            dict(phase="terminal", summary=copy.deepcopy(summary))])
            receipt = dict(schema=RECEIPT_SCHEMA, scorer_config=inputs["scorer_config"], order=inputs["order"],
                           requests=c["requests"], **{k: identity[k] for k in HASHES},
                           generation_prefix=c["generation_prefix"], generation_root_sha256=c["generation_root_sha256"],
                           exit_status=0, resource_gate_passed=True, cleanup_complete=True,
                           native_qualification_passed=True, measurement_sealed_before_truth=True)
            data = dict(c=c, records=records, receipt=receipt, order_body=order_body,
                        truth_body=struct.pack("<6400q", *truths))
            change(data)
            # Reauthenticate mutated evidence so schema checks, not stale hashes, reject it.
            inputs["scorer_config"] = put("scorer.json", canonical(data["c"]))
            inputs["order"] = put("order.u64", data["order_body"])
            inputs["measurements"] = put("sealed.jsonl", data.get("measurement_body", b"".join(canonical(r) for r in data["records"])))
            receipt["measurements"] = inputs["measurements"]
            inputs["measurement_receipt"] = put("receipt.json", canonical(receipt))
            inputs["truth"] = put("truth.i64", data["truth_body"])
            config = dict(schema=CONFIG_SCHEMA, inputs=inputs)
            descriptor = put("config.json", canonical(config))
            return descriptor["path"], descriptor["sha256"], inputs

        def evaluate_fixture(change=lambda data: None, boundary=None):
            path, digest_value, _ = fixture(change, boundary)
            return evaluate(path, digest_value)

        def reject(label, change):
            nonlocal checks
            try:
                evaluate_fixture(change)
            except (ValueError, OSError, TypeError, KeyError):
                checks += 1
            else:
                raise AssertionError("accepted " + label)

        result = evaluate_fixture()
        assert result["scientific_status"] == "GO" and result["execution_status"] == "SUCCESS"
        assert all(v["hits10"] == 640 and v["hits100"] == 6400 for v in result["aggregate"].values())
        assert result["frozen_summary"]["rows"] == 769  # Reversed order + partial final unit/page.
        checks += 1
        for floor in (607, 608):
            r = evaluate_fixture(boundary=floor)
            assert r["aggregate"]["page_closure"]["hits10"] == floor
            assert r["aggregate"]["returned_ids"]["hits10"] == floor
            assert r["scientific_status"] == ("FAIL" if floor == 607 else "GO")
            assert r["execution_status"] == "SUCCESS"
            checks += 1

        def loss(data, stage):
            for q in data["records"][2:66]:
                t = q["trace"]
                q["returned_ids"] = list(range(368, 468))  # Physical rows 301..400, no GT overlap.
                if stage == "source_scored_units":
                    t["nomination_evaluated_units"] = [10]
                if stage == "source_ranked_pages":
                    t["ranked_candidate_pages"], t["primary_page"] = [1, 2, 3], 1
                if stage in ("source_ranked_pages", "sq8_admitted_ranges"):
                    t["primary_page"] = 1
                    if stage == "sq8_admitted_ranges":
                        t["ranked_candidate_pages"] = [1, 0, 2, 3]
                    q["selected_pages"] = [1, 2, 3]
                    q["ranges"] = cover(q["selected_pages"], 769, 13)
                    q["sq8_bytes"] = sum(r["end"] - r["start"] for r in q["ranges"])
        for stage in STAGES[2:]:
            r = evaluate_fixture(lambda data: loss(data, stage))
            assert r["failure_class"] == "downstream" and r["first_crossing_stage"] == stage, stage
            assert r["aggregate"][stage]["hits10"] == (64 if stage in STAGES[3:5] else 0)
            checks += 1

        def thin_nomination(data):
            for q in data["records"][2:66]:
                q["trace"]["semantic_units"] = [1, 8, 16, 24]
                q["leaf_bytes"] = 24
        r = evaluate_fixture(thin_nomination)
        assert r["aggregate"]["nominated_units"]["hits10"] == 64
        assert r["aggregate"]["nominated_units"]["hits100"] == 2112
        assert r["aggregate"]["page_closure"]["hits10"] == 640 and r["scientific_status"] == "GO"
        checks += 1

        def returned_floor(data, floor):
            for q in data["records"][2:2 + 640 - floor]:
                q["returned_ids"][9], q["returned_ids"][10] = q["returned_ids"][10], q["returned_ids"][9]
        for floor in (607, 608):
            r = evaluate_fixture(lambda data: returned_floor(data, floor))
            assert r["aggregate"]["returned_ids"]["hits10"] == floor
            assert r["aggregate"]["returned_ids"]["hits100"] == 6400
            assert r["scientific_status"] == ("FAIL" if floor == 607 else "GO")
            checks += 1

        mutations = [
            ("partial", lambda d: d["records"].pop()),
            ("reordered", lambda d: d["records"].__setitem__(slice(2, 4), d["records"][2:4][::-1])),
            ("duplicate ordinal", lambda d: d["records"][3].update(ordinal=0)),
            ("failed query", lambda d: d["records"][2].update(phase="query_failure")),
            ("truth used", lambda d: d["records"][2].update(truth_opened=True)),
            ("summary mismatch", lambda d: d["records"][-1]["summary"].update(queries=63)),
            ("summary bool", lambda d: d["records"][-1]["summary"].update(first=False)),
            ("failed marker", lambda d: d["records"][-2].update(status="FAIL")),
            ("v2 result", lambda d: d["records"][0].update(schema="borsuk-semantic-router-scorer-result-v2")),
            ("v2 config", lambda d: d["c"].update(schema="borsuk-semantic-router-scorer-config-v2")),
            ("unknown config", lambda d: d["c"].update(truth={})),
            ("partial truth", lambda d: d.update(truth_body=d["truth_body"][:-8])),
            ("duplicate truth", lambda d: d.update(truth_body=b"\0" * 51200)),
            ("negative truth", lambda d: d.update(truth_body=struct.pack("<q", -1) + d["truth_body"][8:])),
            ("range truth", lambda d: d.update(truth_body=struct.pack("<q", 769) + d["truth_body"][8:])),
            ("malformed JSON", lambda d: d.update(measurement_body=b"{\n" * 68)),
            ("duplicate JSON key", lambda d: d.update(measurement_body=b'{"phase":0,"phase":1}\n' * 68)),
            ("nonfinite JSON", lambda d: d.update(measurement_body=b'{"phase":NaN}\n' * 68)),
            ("unterminated JSONL", lambda d: d.update(measurement_body=b"\n".join(canonical(r).rstrip(b"\n") for r in d["records"]))),
            ("negative timing", lambda d: d["records"][2].update(query_wall_ns=-1)),
            ("bool timing", lambda d: d["records"][2].update(query_process_cpu_ns=True)),
            ("float timing", lambda d: d["records"][2].update(query_wall_ns=10.0)),
            ("bad interval", lambda d: d["records"][2]["stages"]["sq8"].update(start_ns=2)),
            ("bad leaf bytes", lambda d: d["records"][2].update(leaf_bytes=1)),
            ("bad SQ8 bytes", lambda d: d["records"][2].update(sq8_bytes=1)),
            ("source limit", lambda d: d["records"][2].update(source_gets=129)),
            ("bad range", lambda d: d["records"][2]["ranges"][0].update(end=9998)),
            ("bool range", lambda d: d["records"][2]["ranges"][0].update(start=False)),
            ("duplicate range", lambda d: d["records"][2]["ranges"].append(d["records"][2]["ranges"][0])),
            ("graph trace", lambda d: d["records"][2]["trace"].update(discoveries=[{}])),
            ("seed geometry", lambda d: d["records"][2]["trace"].update(semantic_seed_additions=[24])),
            ("primary binding", lambda d: d["records"][2]["trace"].update(primary_page=1)),
            ("metadata bool", lambda d: d["records"][1]["metadata"].update(staging_wall_ns=True)),
            ("over memory", lambda d: d["records"][-2]["summary"].update(observed_process_peak_bytes=(512 << 20) + 1)),
        ]
        for key in HASHES + ("config_sha256",):
            mutations.append(("identity " + key, lambda d, k=key: d["records"][0].update({k: "f" * 64})))
        for key in ("generation_root_sha256", "requests_sha256", "order_sha256"):
            mutations.append(("summary " + key, lambda d, k=key: d["records"][-2]["summary"].update({k: "f" * 64})))
        for key in ("resource_gate_passed", "cleanup_complete", "native_qualification_passed", "measurement_sealed_before_truth"):
            mutations.append(("receipt " + key, lambda d, k=key: d["receipt"].update({k: False})))
        mutations.append(("original exit failure", lambda d: d["receipt"].update(exit_status=2)))
        mutations.append(("boolean exit", lambda d: d["receipt"].update(exit_status=False)))
        mutations.append(("receipt config descriptor", lambda d: d["receipt"].update(scorer_config=dict(d["receipt"]["scorer_config"], bytes=1))))
        for field in ("semantic_units", "nomination_evaluated_units", "ranked_candidate_pages", "semantic_leaves"):
            for values in ([True], [-1], [99999], [0, 0]):
                mutations.append((field + repr(values), lambda d, k=field, v=values: d["records"][2]["trace"].update({k: v})))
        for values in ([True] + list(range(1, 100)), list(range(99)) + [98], [-1] + list(range(1, 100))):
            mutations.append(("returned IDs", lambda d, v=values: d["records"][2].update(returned_ids=v)))
        for label, mutation in mutations:
            reject(label, mutation)

        # Direct permutation checks avoid a stale descriptor masking malformed order.
        for values in ([0, 0, 2], [0, 1, 3]):
            try:
                inverse_order(struct.pack("<3Q", *values), 3)
            except ValueError:
                checks += 1
            else:
                raise AssertionError("bad permutation accepted")
        assert list(inverse_order(struct.pack("<3Q", 2, 0, 1), 3)) == [1, 2, 0]

        path, config_hash, inputs = fixture()
        output = base / "result.json"
        run(path, config_hash, output)
        before = output.read_bytes()
        run(path, config_hash, output, replay=True)
        try:
            run(path, config_hash, output)
        except FileExistsError:
            assert output.read_bytes() == before
            checks += 1
        else:
            raise AssertionError("output overwritten")
        output.write_bytes(before + b" ")
        try:
            run(path, config_hash, output, replay=True)
        except ValueError:
            checks += 1
        else:
            raise AssertionError("tampered replay accepted")

        # Instrument every os.open: invalid measurement/receipt/order must fail before GT open.
        original_open = os.open
        for mode in ("tamper", "receipt", "order", "returned"):
            mutation = (lambda d: d["receipt"].update(cleanup_complete=False)) if mode == "receipt" else (lambda d: None)
            path, config_hash, inputs = fixture(mutation)
            target = Path(inputs["measurements"]["path"] if mode in ("tamper", "returned") else inputs["order"]["path"])
            if mode in ("tamper", "order"):
                target.write_bytes(target.read_bytes()[:-1] + b"X")
            if mode == "returned":
                # Authenticated narrow ranges with out-of-range returned physical rows.
                path, config_hash, inputs = fixture(lambda d: d["records"][2].update(returned_ids=list(range(100))), boundary=640)
            opened = []

            def observed_open(name, *args, **kwargs):
                opened.append(str(name))
                return original_open(name, *args, **kwargs)

            os.open = observed_open
            try:
                try:
                    evaluate(path, config_hash)
                except ValueError:
                    assert inputs["truth"]["path"] not in opened
                    assert inputs["scorer_config"]["path"] in opened
                    checks += 1
                else:
                    raise AssertionError("accepted invalid " + mode)
            finally:
                os.open = original_open
        path, config_hash, inputs = fixture()
        opened = []
        os.open = observed_open
        try:
            evaluate(path, config_hash)
            assert opened[-1] == inputs["truth"]["path"]
            assert opened.count(inputs["truth"]["path"]) == 1
            assert str(base / "never-open-queries") not in opened
            checks += 1
        finally:
            os.open = original_open
        # The shared authentication helper rejects symlinks/FIFOs without blocking.
        for kind in ("symlink", "fifo"):
            target = Path(inputs["truth"]["path"])
            target.unlink()
            if kind == "symlink":
                target.symlink_to(base / "scorer.json")
            else:
                os.mkfifo(target)
            try:
                evaluate(path, config_hash)
            except (OSError, ValueError):
                checks += 1
            else:
                raise AssertionError("accepted nonregular truth")
        # Geometry cover includes partial tail and deterministic 32-range gap bridging.
        assert cover([3], 769, 13) == [dict(start=9984, end=9997)]
        assert len(cover(list(range(0, 66, 2)), 17000, 13)) == 32
    signal.alarm(0)
    print(f"PASS {checks} synthetic checks; no real panel/truth/native execution")


def main(args):
    if args == ["--self-check"]:
        self_check()
        return
    replay = bool(args and args[0] == "--replay")
    if replay:
        args = args[1:]
    require(len(args) == 3, "usage: [--replay] CONFIG CONFIG_SHA OUTPUT | --self-check")
    result = run(*args, replay=replay)
    print(json.dumps({k: result[k] for k in ("execution_status", "scientific_status", "failure_class")}))


if __name__ == "__main__":
    try:
        main(sys.argv[1:])
    except (ValueError, OSError, TypeError, KeyError, OverflowError, RecursionError) as error:
        print(f"invalid evidence: {error}", file=sys.stderr)
        sys.exit(2)
