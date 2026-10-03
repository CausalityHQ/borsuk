#!/usr/bin/env python3
"""Truth-free fixed48 virtual locality gate; no ANN, query/GT reads or payload rewrite.

Run synthetic --self-check first. Parent owns --output NEWPATH; --replay PATH
reauthenticates and recomputes an existing closed result without writing it.
All checks require a one-CPU, 512-MiB, zero-swap cgroup and finish within 120s.
"""
import argparse
import base64
import bisect
import itertools
import json
import math
import os
from pathlib import Path
import resource
import signal
import struct
import tempfile
from unittest.mock import patch

from replay_fixed48_source_get_caps import (
    ANCHOR, BASE, OFFERED, SOURCE_BUDGET, SOURCE_FILES,
    cover_pages, encoded, identity, read_body, recorded_parity, require,
)

BASE_COMMIT = "f6d605a3457f148809bd7162ba98606e032bb726"
SOURCE32 = BASE / "source32-geometry/replay_fixed48_source_get_caps.json"
SOURCE32_PIN = dict(bytes=742988, sha256="e53ad52bc3c0933551d03c4a449d4141963c5f3e5752b83af6764bcc046ea181")
MEMBERSHIP_PIN = dict(bytes=125000, sha256="a97f1bdbdbfaccc8009b56fd85c2c82e5f01405a650222d335b66c156fe8cdc1")
SQ8_BUDGET = 16773120
OUTPUT_LIMIT = 2 << 20
GATE = dict(query_gets_strictly_less_than=7142, query_bytes_at_most=2393364520,
            source_per_query_bytes_at_most=SOURCE_BUDGET, sq8_per_query_bytes_at_most=SQ8_BUDGET,
            max_gets_per_tier=32)


def pin_only(pin):
    return {k: pin[k] for k in ("bytes", "sha256")}


def reader(repo, authenticated):
    """Only closed metadata/trace bodies; request and truth pins stay metadata."""
    repo = repo.resolve()

    def read(name, pin, *, json_body=True):
        name = Path(name)
        require(not name.is_absolute() and ".." not in name.parts, f"invalid input path: {name}")
        require(not any(token in name.name.lower() for token in ("request", "quer", "truth")),
                f"forbidden query/truth input: {name}")
        path = repo / name
        require(path.resolve().is_relative_to(repo) and
                not any(p.is_symlink() for p in (path, *path.parents) if p != repo and repo in p.parents),
                f"input outside repository or symlink: {name}")
        body = read_body(path, pin)
        authenticated[str(name)] = identity(body)
        return json.loads(body) if json_body else body

    return read


def router_directory(body, discovery, rows, dimensions):
    require(identity(body) == dict(bytes=discovery["root_bytes"], sha256=discovery["root_sha256"]) and
            len(body) >= 512 and body[:8] == b"BORSUSR2", "authenticated router root version/identity")
    header, profile, declared_rows, d, units, leaves, requested, training, schema_len = struct.unpack_from("<IIQIIIIII", body, 8)
    centers = ((rows + 31) // 32 + 63) // 64
    require(header == 512 and profile == 2 and declared_rows == rows and d == dimensions and
            units == (rows + 31) // 32 and requested == centers and training in (1, requested) and
            centers <= leaves <= 2 * centers - 1 and 1 <= schema_len <= 256 and
            body[88:128] == bytes(40) and body[256 + schema_len:512] == bytes(256 - schema_len),
            "router root header geometry/reserved")
    require(body[256:256 + schema_len].decode() == discovery["input_schema"] and
            len(body) == 512 + leaves * (64 + 4 * dimensions), "router root schema/exact body length")
    centroid_bytes, membership_bytes, leaf_bytes, estimate, limit = struct.unpack_from("<QQQQQ", body, 48)
    require(centroid_bytes == 32 + units * dimensions * 2 and membership_bytes == units * 4 == discovery["membership_bytes"] and
            leaf_bytes == units * (4 + 2 * dimensions) == discovery["leaves_bytes"] and
            0 < estimate <= limit <= 512 << 20, "router root exact lengths/allocation bounds")
    for offset, field in ((128, "input_root_sha256"), (160, "centroids_sha256"),
                          (192, "membership_sha256"), (224, "leaves_sha256")):
        require(body[offset:offset + 32].hex() == discovery[field], f"router root discovery binding: {field}")
    directory, end, previous = [], 0, None
    for leaf in range(leaves):
        group, chunk, offset, size, count, source_rows = struct.unpack_from("<IIQIII", body, 512 + leaf * 64)
        ordered = chunk == 0 if previous is None else (
            chunk == previous[1] + 1 and previous[2] == 64 if group == previous[0] else group > previous[0] and chunk == 0)
        require(group < training and ordered and offset == end and 1 <= count <= 64 and
                size == count * (4 + 2 * dimensions) and 1 <= source_rows <= count * 32 and
                body[512 + leaf * 64 + 28:512 + leaf * 64 + 32] == bytes(4), "router leaf directory geometry/order")
        directory.append(dict(leaf_id=leaf, offset=offset, bytes=size, unit_count=count, source_rows=source_rows))
        end += size
        previous = group, chunk, count
    require(end == leaf_bytes and sum(leaf["unit_count"] for leaf in directory) == units and
            sum(leaf["source_rows"] for leaf in directory) == rows, "router incomplete leaf directory")
    require(all(math.isfinite(value) for (value,) in struct.iter_unpack("<f", memoryview(body)[512 + leaves * 64:])),
            "router nonfinite prototype")
    return directory


def declared_membership(body, discovery, terminal_pin, root_body, rows, dimensions):
    require(identity(body) == pin_only(terminal_pin) == MEMBERSHIP_PIN ==
            dict(bytes=discovery["membership_bytes"], sha256=discovery["membership_sha256"]),
            "membership terminal/discovery identity")
    units = (rows + 31) // 32
    require(len(body) == 4 * units, "membership exact unit count")
    directory = router_directory(root_body, discovery, rows, dimensions)
    leaves = len(directory)
    membership = [leaf for (leaf,) in struct.iter_unpack("<I", body)]
    roster = [set() for _ in range(leaves)]
    for unit, leaf in enumerate(membership):
        require(leaf < leaves, "membership leaf outside declared root roster")
        roster[leaf].add(unit)
    require(all(len(group) == leaf["unit_count"] and
                sum(min(32, rows - u * 32) for u in group) == leaf["source_rows"]
                for group, leaf in zip(roster, directory)), "membership incomplete/incorrect root leaf roster")
    return membership, roster


def authenticate(repo):
    authenticated = {}
    read = reader(repo, authenticated)
    pointer = lambda pin: read(pin["path"], pin)
    audit = read(OFFERED / "root-audit.json", ANCHOR)
    require(audit["actual_closed_validator_passed"] is True and
            audit["all_declared_artifacts_authenticated"] is True and audit["scientific_status"] == "PASS",
            "offered audit is not closed PASS")
    config = read(OFFERED / "config.json", audit["authenticated_artifacts"]["config.json"])
    rate = read(OFFERED / "screen/rate5-records.jsonl",
                audit["authenticated_artifacts"]["screen/rate5-records.jsonl"], json_body=False)
    cold_files = config["cold_run"]["files"]
    cold = pointer(cold_files["config.json"])
    publication = pointer(cold_files["screen/publication.json"])
    native = pointer(cold_files["native/native-source-manifest.json"])
    require(identity(encoded(native["source_sha256"]))["sha256"] == native["source_identity_sha256"],
            "native source manifest identity")
    saved = read(SOURCE32, SOURCE32_PIN)
    require(saved["schema"] == "borsuk-fixed48-source-get-cap-geometry-v1" and
            saved["status"] == "PASS_CONDITIONAL_GEOMETRY" and saved["query_count"] == 64 and
            saved["source_byte_cap"] == SOURCE_BUDGET and
            saved["native_source_identity_sha256"] == native["source_identity_sha256"],
            "pinned SOURCE32 replay identity/status")
    read("scripts/replay_fixed48_source_get_caps.py", saved["script"], json_body=False)
    for name in SOURCE_FILES:
        require(saved["source_files"][name]["sha256"] == native["source_sha256"][name],
                f"pinned SOURCE32 native source: {name}")
        read(name, saved["source_files"][name], json_body=False)

    proofs = cold["proofs"]
    terminal = pointer(proofs["scientific_terminal"])
    closed = pointer(proofs["historical_validation"])
    require(closed["closed_validation_passed"] is True and closed["scientific_status"] == "GO" and
            closed["state"] == "terminated" and closed["measurement_rerun"] is False and
            closed["terminal"] == pin_only(proofs["scientific_terminal"]), "scientific closed terminal binding")
    require(terminal["phase"] == terminal["status"] == "complete" and
            terminal["exit_code"] == terminal["original_exit_code"] == 0, "scientific terminal status")
    science = Path(proofs["scientific_terminal"]["path"]).parent

    def artifact(name, *, json_body=True):
        return read(science / name, terminal["artifacts"][name], json_body=json_body)

    receipt = artifact("screen/measurement-receipt.json")
    marker = artifact("screen/COMPLETE.json")
    scorer = artifact("screen/scorer-config.json")
    trace = artifact("screen/records.jsonl", json_body=False)
    require(receipt["exit_status"] == 0 and all(receipt[k] is True for k in
            ("cleanup_complete", "measurement_sealed_before_truth", "native_qualification_passed", "resource_gate_passed")),
            "unsealed/failed measurement receipt")
    require(marker["execution_status"] == "SUCCESS" and marker["scientific_status"] == "GO" and
            identity(encoded(marker["files"]))["sha256"] == marker["roster_sha256"], "scientific seal status/digest")
    require(identity(trace) == marker["files"]["records.jsonl"] == pin_only(receipt["measurements"]),
            "receipt/seal/trace binding")
    # Authenticate request identities across authorities without opening the body.
    require(marker["files"]["requests.jsonl"] == pin_only(receipt["requests"]) ==
            pin_only(scorer["requests"]) == terminal["artifacts"]["screen/requests.jsonl"],
            "request metadata binding (body forbidden)")
    require(pin_only(receipt["scorer_config"]) == terminal["artifacts"]["screen/scorer-config.json"],
            "scorer config byte binding")
    require((scorer["rows"], scorer["dimensions"], scorer["first"], scorer["count"], scorer["profile"]) ==
            (1_000_000, 768, 0, 64, "fresh1m"), "sealed panel geometry")
    require(receipt["generation_prefix"] == scorer["generation_prefix"], "receipt generation prefix")
    store = "screen/store/" + scorer["generation_prefix"] + "/"
    root = artifact(store + "manifest.json")
    plane = artifact(store + "plane/manifest.json")
    page = artifact(store + "page_manifest.json")
    require(scorer["generation_root_sha256"] == receipt["generation_root_sha256"] ==
            terminal["artifacts"][store + "manifest.json"]["sha256"], "sealed generation root")
    for field, name in (("plane_manifest_sha256", "plane/manifest.json"), ("page_manifest_sha256", "page_manifest.json")):
        require(root[field] == terminal["artifacts"][store + name]["sha256"] == publication["manifest"][field],
                f"generation/publication binding: {field}")
    discovery = root["discovery"]
    require(discovery == publication["manifest"]["discovery"] and
            discovery["mode"] == "semantic" and discovery["profile"] == "fresh1m", "offered discovery identity")
    require(plane["rows"] == page["rows"] == scorer["rows"] and
            plane["dimensions"] == page["dimensions"] == scorer["dimensions"] and
            plane["page_rows"] == 32 and page["page_rows"] == 256 and plane["record_bytes"] == 200 and
            plane["query_or_truth_used"] is False, "source/SQ8 geometry binding")
    require(discovery["input_schema"] == plane["schema"] and discovery["input_root_sha256"] == root["plane_manifest_sha256"],
            "discovery plane identity")
    require(plane["source_order_sha256"] == receipt["order"]["sha256"] == scorer["order"]["sha256"] ==
            discovery["source_order_sha256"] and pin_only(receipt["order"]) == pin_only(scorer["order"]), "order identity")
    for field in ("source_sha256", "sq8_sha256", "mean_sha256", "records_sha256"):
        require(discovery[field] == plane[field], f"discovery plane payload identity: {field}")
    require(page["object_sha256"] == root["sq8_object_sha256"] == plane["sq8_sha256"], "SQ8 object identity")
    for kind, local in (("membership", "router/membership.bin"), ("root", "router/root.bin"), ("leaves", "router/leaves.bin")):
        descriptor = dict(bytes=discovery[kind + "_bytes"], sha256=discovery[kind + "_sha256"])
        require(descriptor == terminal["artifacts"][store + local], f"terminal/discovery {kind} descriptor")
        if kind != "leaves":
            require(descriptor == dict(bytes=publication["arm"]["metadata_files"][local],
                                       sha256=publication["arm"]["metadata_sha256"][local]),
                    f"publication {kind} descriptor")
    membership_body = artifact(store + "router/membership.bin", json_body=False)
    router_body = artifact(store + "router/root.bin", json_body=False)
    membership, roster = declared_membership(membership_body, discovery,
                                             terminal["artifacts"][store + "router/membership.bin"],
                                             router_body, scorer["rows"], scorer["dimensions"])

    events = [json.loads(line) for line in trace.splitlines()]
    require([e["phase"] for e in events] == ["identity", "startup"] + ["frozen_query"] * 64 +
            ["all_queries_frozen", "terminal"], "exact closed scientific trace roster")
    summary = events[-1]["summary"]
    require(events[-2]["summary"] == summary and summary["complete"] is True and
            summary["truth_opened"] is False and summary["status"] == "FROZEN" and
            events[-2]["count"] == summary["queries"] == 64, "all64 frozen terminal")
    for key in ("binary_sha256", "router_source_sha256", "scorer_source_sha256"):
        require(events[0][key] == summary[key] == receipt[key], f"native trace identity: {key}")
    require(summary["config_sha256"] == events[0]["config_sha256"] == receipt["scorer_config"]["sha256"],
            "trace config identity")
    require(receipt["scorer_source_sha256"] == native["source_sha256"][SOURCE_FILES[4]] and
            receipt["router_source_sha256"] == native["source_sha256"][SOURCE_FILES[3]], "trace source identities")
    for field in ("requests", "order"):
        require(summary[field + "_sha256"] == receipt[field]["sha256"] and
                summary[field + "_bytes"] == receipt[field]["bytes"], f"trace {field} metadata identity")
    require(summary["generation_root_sha256"] == scorer["generation_root_sha256"], "trace generation identity")
    offered = [json.loads(line) for line in rate.splitlines()]
    queries = events[2:-2]
    require([q["ordinal"] for q in queries] == [q["query_ordinal"] for q in offered] ==
            [q["ordinal"] for q in saved["queries"]] == list(range(64)), "ordered all64 panel")
    for name, pin in authenticated.items():
        if name in saved["authenticated_inputs"]:
            require(pin == saved["authenticated_inputs"][name], f"SOURCE32 shared authority: {name}")
    authority = dict(generation_root_sha256=scorer["generation_root_sha256"],
                     scientific_terminal=pin_only(proofs["scientific_terminal"]),
                     membership=identity(membership_body), discovery=discovery,
                     leaf_count=len(roster), leaf_roster_source="authenticated BORSUSR2 root directory",
                     router_binary_body_read=True, router_root=identity(router_body),
                     native_source_identity_sha256=native["source_identity_sha256"])
    return dict(authenticated_inputs=authenticated, authority=authority, queries=queries, offered=offered,
                saved=saved, publication=publication, membership=membership, roster=roster,
                rows=scorer["rows"], dimensions=scorer["dimensions"], source_width=plane["record_bytes"])


def layout(membership, rows):
    require(type(rows) is int and rows > 0 and len(membership) == (rows + 31) // 32 and
            all(type(leaf) is int and leaf >= 0 for leaf in membership), "invalid permutation geometry")
    order = sorted(range(len(membership)), key=lambda unit: (membership[unit], unit))
    inverse = [0] * len(order)
    lengths, starts = [], [0]
    for virtual, original in enumerate(order):
        inverse[original] = virtual
        lengths.append(min(32, rows - original * 32))
        starts.append(starts[-1] + lengths[-1])
    require(starts[-1] == rows and all(order[inverse[u]] == u for u in range(len(order))),
            "permutation bijection/row count")
    return dict(order=order, inverse=inverse, starts=starts, lengths=lengths)


def map_row(row, permutation):
    require(type(row) is int and 0 <= row < permutation["starts"][-1], "original row bounds")
    return permutation["starts"][permutation["inverse"][row // 32]] + row % 32


def unmap_row(row, permutation):
    starts = permutation["starts"]
    require(type(row) is int and 0 <= row < starts[-1], "virtual row bounds")
    virtual = bisect.bisect_right(starts, row) - 1
    return permutation["order"][virtual] * 32 + row - starts[virtual]


def units_in_ranges(ranges, rows, width):
    """Every fetched row, including gaps. Closed native endpoints are unit aligned."""
    require(type(width) is int and width > 0 and ranges, "empty/invalid required ranges")
    result, previous = set(), 0
    for a, b in ranges:
        require(type(a) is int and type(b) is int and previous <= a < b <= rows * width and
                a % (32 * width) == 0 and (b % (32 * width) == 0 or b == rows * width),
                "invalid required range bounds/order/unit alignment")
        result.update(range(a // (32 * width), (b // width + 31) // 32))
        previous = b
    require(sum(min(32, rows - u * 32) * width for u in result) == sum(b - a for a, b in ranges),
            "required ranges exact row count")
    return result


def cover_units(selected, starts, width, max_gets=32):
    """Generalized native page cover over exact (possibly partial) virtual units."""
    require(type(width) is int and width > 0 and type(max_gets) is int and max_gets > 0 and
            len(starts) > 1 and starts[0] == 0 and
            all(type(a) is int and 1 <= b - a <= 32 for a, b in zip(starts, starts[1:])),
            "invalid virtual cover geometry")
    require(selected and all(type(u) is int and 0 <= u < len(starts) - 1 for u in selected), "invalid virtual units")
    runs = []
    for unit in sorted(set(selected)):
        if runs and runs[-1][1] == unit:
            runs[-1][1] += 1
        else:
            runs.append([unit, unit + 1])
    gaps = sorted((starts[runs[i + 1][0]] - starts[runs[i][1]], i) for i in range(len(runs) - 1))
    joined = {i for _, i in gaps[:max(0, len(runs) - max_gets)]}
    merged = [runs[0][:]]
    for i, run in enumerate(runs[1:]):
        if i in joined:
            merged[-1][1] = run[1]
        else:
            merged.append(run[:])
    ranges = [[starts[a] * width, starts[b] * width] for a, b in merged]
    return ranges, sum(b - a for a, b in ranges)


def tier(original_units, permutation, width):
    selected = {permutation["inverse"][u] for u in original_units}
    ranges, charged = cover_units(selected, permutation["starts"], width)
    cursor = 0
    for unit in sorted(selected):
        a, b = (permutation["starts"][i] * width for i in (unit, unit + 1))
        while cursor < len(ranges) and ranges[cursor][1] <= a:
            cursor += 1
        require(cursor < len(ranges) and ranges[cursor][0] <= a < b <= ranges[cursor][1],
                "implementation error: lost required unit rows")
    require(len(ranges) <= 32 and all(0 <= a < b <= permutation["starts"][-1] * width for a, b in ranges) and
            all(b < c for (_, b), (c, _) in zip(ranges, ranges[1:])) and
            charged == sum(b - a for a, b in ranges), "implementation error: malformed virtual cover")
    required_rows = sum(permutation["lengths"][u] for u in selected)
    return dict(gets=len(ranges), bytes=charged, ranges=ranges, required_units=len(selected),
                required_rows=required_rows, bridged_rows=charged // width - required_rows,
                required_virtual_units=identity(encoded(sorted(selected))), all_required_rows_preserved=True)


def admission(queries, totals):
    failures = []
    if totals["gets"] >= GATE["query_gets_strictly_less_than"]:
        failures.append(dict(criterion="aggregate_query_GETs", actual=totals["gets"], strictly_less_than=7142))
    if totals["bytes"] > GATE["query_bytes_at_most"]:
        failures.append(dict(criterion="aggregate_query_bytes", actual=totals["bytes"], at_most=2393364520))
    for query in queries:
        for name, budget in (("source", SOURCE_BUDGET), ("sq8", SQ8_BUDGET)):
            result = query[name]
            for criterion, violated in (("byte_admission", result["bytes"] > budget),
                                        ("GET_admission", result["gets"] > 32),
                                        ("required_row_preservation", result["all_required_rows_preserved"] is not True)):
                if violated:
                    failures.append(dict(ordinal=query["ordinal"], tier=name, criterion=criterion,
                                         gets=result["gets"], bytes=result["bytes"], byte_cap=budget))
    return failures


def source_closure(query, roster, unit_count, dimensions):
    ordinal, trace = query["ordinal"], query["trace"]
    leaves, semantic, seeds, nominations = (trace[k] for k in
                                            ("semantic_leaves", "semantic_units", "semantic_seed_additions", "nomination_evaluated_units"))
    require(len(leaves) == len(set(leaves)) == query["leaf_gets"] == 48 and
            all(type(leaf) is int and 0 <= leaf < len(roster) for leaf in leaves), f"query {ordinal} selected leaf roster")
    expected = set().union(*(roster[leaf] for leaf in leaves))
    require(len(set(semantic)) == len(semantic) and set(semantic) == expected and
            len(semantic) * (4 + dimensions * 2) == query["leaf_bytes"], f"query {ordinal} semantic membership/leaf bytes")
    units = semantic + seeds
    require(len(set(units)) == len(units) and len(set(nominations)) == len(nominations) <= 4096 and
            all(type(u) is int and 0 <= u < unit_count for u in units + nominations),
            f"query {ordinal} duplicate/oversized/out-of-bounds units")
    closure = {u // 8 for u in units}
    require(set(units) <= set(nominations) and closure == {u // 8 for u in nominations} ==
            set(trace["ranked_candidate_pages"]) and len(closure) <= 512, f"query {ordinal} nomination closure")
    # Completion units and seeds are retained even outside selected leaves.
    required = {u for page in closure for u in range(page * 8, min((page + 1) * 8, unit_count))}
    return closure, required


def replay(repo, pressure_check=lambda: None):
    inputs = authenticate(repo)
    rows, width, dimensions = inputs["rows"], inputs["source_width"], inputs["dimensions"]
    permutation = layout(inputs["membership"], rows)
    results, source_sums = [], {str(cap): dict(gets=0, bytes=0) for cap in (128, 32)}
    original, offered_total, source32_total = [dict(gets=0, bytes=0) for _ in range(3)]
    for query, row, saved in zip(inputs["queries"], inputs["offered"], inputs["saved"]["queries"]):
        pressure_check()
        ordinal, trace, response = query["ordinal"], query["trace"], row["response"]
        require(query["truth_opened"] is False and row["rate_index"] == 5 and row["offered_qps"] == 8 and
                row["outcome"] == "success" and row["http_status"] == 200, f"query {ordinal} status")
        require(response == json.loads(base64.b64decode(row["raw_response_base64"], validate=True)) and
                response["authority"] == row["expected_authority"] == inputs["publication"]["arm"]["authority"],
                f"query {ordinal} raw response/authority parity")
        recorded_parity(query, response)
        closure, source_units = source_closure(query, inputs["roster"], len(permutation["order"]), dimensions)
        seeds, nominations = trace["semantic_seed_additions"], trace["nomination_evaluated_units"]
        require(saved["closure_pages"] == sorted(closure) and saved["nomination_units"] == len(nominations) and
                saved["recorded_top10_leaf_source_sq8_parity"] is True and saved["sq8_recorded_ranges_replayed"] is True and
                saved["source_recorded_ranges_available"] is False, f"query {ordinal} SOURCE32 closure/parity labels")
        source = {}
        for cap in (128, 32):
            ranges, charged = cover_pages(closure, rows, width, cap, SOURCE_BUDGET)
            source[str(cap)] = dict(gets=len(ranges), bytes=charged, ranges=ranges,
                                    largest_range_bytes=max(b - a for a, b in ranges))
            require(source[str(cap)] == saved["source"][str(cap)], f"query {ordinal} original SOURCE{cap} endpoints")
            source_sums[str(cap)]["gets"] += len(ranges)
            source_sums[str(cap)]["bytes"] += charged
        require((source["128"]["gets"], source["128"]["bytes"]) == (query["source_gets"], query["source_bytes"]),
                f"query {ordinal} original SOURCE128 counts/bytes")
        require(source["32"]["bytes"] - source["128"]["bytes"] == saved["extra_bytes"] and
                source["128"]["gets"] - source["32"]["gets"] == saved["saved_gets"], f"query {ordinal} SOURCE32 deltas")
        require(len(set(query["selected_pages"])) == len(query["selected_pages"]) and
                set(query["selected_pages"]) <= closure, f"query {ordinal} SQ8 page closure")
        sq8_ranges, sq8_bytes = cover_pages(query["selected_pages"], rows, dimensions + 12, 32, SQ8_BUDGET)
        require(sq8_ranges == response["ranges"] == [[r["start"], r["end"]] for r in query["ranges"]] and
                (len(sq8_ranges), sq8_bytes) == (query["sq8_gets"], query["sq8_bytes"]), f"query {ordinal} SQ8 range replay")
        sq8_units = units_in_ranges(sq8_ranges, rows, dimensions + 12)
        # Recount A from trace+original geometry, independently from HTTP records.
        original["gets"] += query["leaf_gets"] + source["128"]["gets"] + len(sq8_ranges)
        original["bytes"] += query["leaf_bytes"] + source["128"]["bytes"] + sq8_bytes
        offered_total["gets"] += response["router_submitted_gets"] + response["source_submitted_gets"] + response["submitted_gets"]
        offered_total["bytes"] += response["router_verified_bytes"] + response["source_verified_bytes"] + response["verified_bytes"]
        source32_total["gets"] += query["leaf_gets"] + source["32"]["gets"] + len(sq8_ranges)
        source32_total["bytes"] += query["leaf_bytes"] + source["32"]["bytes"] + sq8_bytes
        results.append(dict(ordinal=ordinal, leaf=dict(gets=query["leaf_gets"], bytes=query["leaf_bytes"]),
                            closure_pages=sorted(closure), semantic_seed_additions=seeds,
                            original_source=source, original_sq8=dict(gets=len(sq8_ranges), bytes=sq8_bytes, ranges=sq8_ranges),
                            source=tier(source_units, permutation, width), sq8=tier(sq8_units, permutation, dimensions + 12),
                            native_parity_pending=True))
    require(original == offered_total == dict(gets=8261, bytes=2393364520), "independent A baseline recount")
    require(source32_total == dict(gets=7142, bytes=2581780520), "independent SOURCE32 baseline recount")
    for cap in ("128", "32"):
        arm = inputs["saved"]["totals"][cap]
        require(source_sums[cap] == dict(gets=arm["gets"], bytes=arm["bytes"]) and
                arm["max_query_bytes"] == max(q["original_source"][cap]["bytes"] for q in results) and
                arm["largest_range_bytes"] == max(q["original_source"][cap]["largest_range_bytes"] for q in results),
                f"SOURCE{cap} independent totals")
    totals = dict(gets=sum(q[t]["gets"] for q in results for t in ("leaf", "source", "sq8")),
                  bytes=sum(q[t]["bytes"] for q in results for t in ("leaf", "source", "sq8")))
    totals["tiers"] = {tier_name: dict(gets=sum(q[tier_name]["gets"] for q in results),
                                     bytes=sum(q[tier_name]["bytes"] for q in results),
                                     max_query_bytes=max(q[tier_name]["bytes"] for q in results))
                       for tier_name in ("leaf", "source", "sq8")}
    failures = admission(results, totals)
    pressure_check()
    return dict(schema="borsuk-fixed48-semantic-locality-geometry-v1", complete=True,
                status="FAIL" if failures else "PASS_CONDITIONAL_GEOMETRY", base_commit=BASE_COMMIT,
                script=identity(Path(__file__).read_bytes()), authenticated_inputs=inputs["authenticated_inputs"],
                authority=inputs["authority"],
                permutation=dict(ordering="(semantic_leaf_id, original_unit_id); stable rows within original unit",
                                 original_units_in_virtual_order=permutation["order"], unit_count=len(permutation["order"]),
                                 row_count=rows, inverse=identity(encoded(permutation["inverse"])),
                                 row_starts=identity(encoded(permutation["starts"])), lengths=identity(encoded(permutation["lengths"]))),
                baselines=dict(A=original, A_independent_offered=offered_total, A_SOURCE32=source32_total, source=source_sums),
                gate=GATE, query_count=len(results), queries=results, totals=totals, failures=failures,
                proof=dict(all_required_rows_preserved=True, source_full_original_page_closure=True,
                           sq8_all_original_fetched_rows_including_gaps=True, leaf_charges_unchanged=True,
                           startup_excluded_unchanged=True, query_vectors_read=False, request_bodies_read=False,
                           ground_truth_read=False, native_parity_pending=True,
                           historical_source_recorded_ranges_available=False,
                           reconstructed_original_source_endpoints_verified_against_pinned_SOURCE32=True),
                interpretation="Conditional virtual geometry only. No payload rewrite, native qualification, quality, latency, "
                "throughput, billing, maintenance or format-adoption claim. FAIL rejects this specific conservative locality "
                "hypothesis, not every hierarchical design. Authority/resource/code failure is INVALID.")


def self_check():
    # Identity order would lose semantic locality; a fixed stride loses partial tails.
    got = layout([2, 0, 1, 0], 99)
    assert got == dict(order=[1, 3, 2, 0], inverse=[3, 0, 2, 1],
                       starts=[0, 32, 35, 67, 99], lengths=[32, 3, 32, 32])
    for row in range(99):
        assert unmap_row(map_row(row, got), got) == row
    assert map_row(96, got) == 32 and map_row(98, got) == 34
    assert cover_units({0, 2, 4}, [0, 32, 64, 67, 99, 131], 9, 2) == ([[0, 603], [891, 1179]], 891)
    # A shorter partial-unit gap must win over an earlier full-unit gap.
    assert cover_units({0, 2, 4}, [0, 32, 64, 96, 99, 131], 9, 2) == ([[0, 288], [576, 1179]], 891)
    cases = 0
    starts = [0, 32, 64, 67, 99, 131, 163, 195]
    for bits in range(1, 1 << 7):
        units = [u for u in range(7) if bits & (1 << u)]
        boundaries = [i for i in range(len(units) - 1) if units[i + 1] > units[i] + 1]
        for cap in range(1, 8):
            candidates = []
            for cuts in itertools.combinations(boundaries, min(cap - 1, len(boundaries))):
                split = [-1, *cuts, len(units) - 1]
                ranges = [[starts[units[a + 1]] * 9, starts[units[b] + 1] * 9] for a, b in zip(split, split[1:])]
                joined = tuple(i for i in boundaries if i not in cuts)
                candidates.append((sum(b - a for a, b in ranges), joined, ranges))
            cost, _, want = min(candidates)
            assert cover_units(units, starts, 9, cap) == (want, cost)
            cases += 1
    # SOURCE excludes original cover gaps; SQ8 explicitly retains them.
    permutation = layout([u % 3 for u in range(17)], 513)
    source_units = units_in_ranges([[0, 256 * 9], [512 * 9, 513 * 9]], 513, 9)
    sq8_units = units_in_ranges([[0, 513 * 9]], 513, 9)
    assert source_units == set(range(8)) | {16} and sq8_units == set(range(17))
    source, sq8 = tier(source_units, permutation, 9), tier(sq8_units, permutation, 9)
    assert source["required_rows"] == 257 and sq8["required_rows"] == 513 and sq8["bytes"] == 4617
    # A seed from an unselected leaf must retain its entire original page.
    seed_query = dict(ordinal=0, leaf_gets=48, leaf_bytes=48 * 1540,
                      trace=dict(semantic_leaves=list(range(48)), semantic_units=list(range(48)),
                                 semantic_seed_additions=[52], nomination_evaluated_units=list(range(53)),
                                 ranked_candidate_pages=list(range(7))))
    closure, seed_units = source_closure(seed_query, [{u} for u in range(53)], 53, 768)
    assert closure == set(range(7)) and seed_units == set(range(53))
    for u in source_units:
        for row in range(u * 32, min((u + 1) * 32, 513)):
            virtual = map_row(row, permutation)
            assert any(a <= virtual * 9 < (virtual + 1) * 9 <= b for a, b in source["ranges"])
    query = dict(ordinal=0, source=dict(bytes=SOURCE_BUDGET, gets=32, all_required_rows_preserved=True),
                 sq8=dict(bytes=SQ8_BUDGET, gets=32, all_required_rows_preserved=True))
    total = dict(gets=7141, bytes=2393364520)
    assert not admission([query], total)
    assert admission([query], dict(total, gets=7142))[0]["criterion"] == "aggregate_query_GETs"
    assert admission([query], dict(total, bytes=2393364521))[0]["criterion"] == "aggregate_query_bytes"
    for name in ("source", "sq8"):
        for field, value in (("bytes", query[name]["bytes"] + 1), ("gets", 33), ("all_required_rows_preserved", False)):
            bad = dict(query, **{name: dict(query[name], **{field: value})})
            assert admission([bad], total)[0]["tier"] == name

    def rejected(action):
        try:
            action()
        except (ValueError, OSError):
            return
        raise AssertionError("invalid geometry/authority/output accepted")

    wrong_trace = dict(seed_query["trace"], semantic_units=list(range(47)))
    rejected(lambda: source_closure(dict(seed_query, trace=wrong_trace), [{u} for u in range(53)], 53, 768))
    for action in (lambda: layout([0], 33), lambda: layout([True], 1),
                   lambda: cover_units(set(), [0, 32], 9), lambda: cover_units({1}, [0, 32], 9),
                   lambda: cover_units({0}, [0, 33], 9), lambda: cover_units({0}, [0, 32], 9, 0),
                   lambda: units_in_ranges([[0, 10]], 33, 9), lambda: units_in_ranges([[0, 288], [0, 288]], 33, 9)):
        rejected(action)
    with tempfile.TemporaryDirectory() as directory:
        repo = Path(directory)
        anchor, saved_pin, member_pin = synthetic_authority(repo)
        opened = []
        original_read = Path.read_bytes

        def guarded_read(path):
            opened.append(str(path.relative_to(repo)))
            assert not any(token in path.name.lower() for token in ("request", "quer", "truth")), "query/GT body opened"
            return original_read(path)

        with patch.dict(globals(), ANCHOR=anchor, SOURCE32_PIN=saved_pin, MEMBERSHIP_PIN=member_pin):
            with patch.object(Path, "read_bytes", guarded_read):
                synthetic = authenticate(repo)
                assert len(synthetic["membership"]) == 31250 and len(synthetic["roster"]) == 489
                assert synthetic["authority"]["router_binary_body_read"] is True
                read = reader(repo, {})
                for name in ("requests.jsonl", "truth.i64", "queries.raw", "../escape", "/outside"):
                    rejected(lambda name=name: read(name, identity(b"never read"), json_body=False))
            before = len(opened)
            assert before == len(synthetic["authenticated_inputs"])
            discovery = synthetic["authority"]["discovery"]
            root_body = (repo / "synthetic-science/screen/store/synthetic/router/root.bin").read_bytes()
            member = struct.pack("<31250I", *synthetic["membership"])
            rejected(lambda: declared_membership(member, dict(discovery, membership_sha256="0" * 64), member_pin,
                                                 root_body, 1000000, 768))
            bad = member[:-4] + struct.pack("<I", 489)
            with patch.dict(globals(), MEMBERSHIP_PIN=identity(bad)):
                consistent_root = root_body[:192] + bytes.fromhex(identity(bad)["sha256"]) + root_body[224:]
                rejected(lambda: declared_membership(bad, dict(discovery, membership_sha256=identity(bad)["sha256"],
                                    root_sha256=identity(consistent_root)["sha256"]), identity(bad), consistent_root, 1000000, 768))
            bad = struct.pack("<I", 1) + member[4:]
            with patch.dict(globals(), MEMBERSHIP_PIN=identity(bad)):
                consistent_root = root_body[:192] + bytes.fromhex(identity(bad)["sha256"]) + root_body[224:]
                rejected(lambda: declared_membership(bad, dict(discovery, membership_sha256=identity(bad)["sha256"],
                                    root_sha256=identity(consistent_root)["sha256"]), identity(bad), consistent_root, 1000000, 768))
            for offset, value in ((0, b"X"), (512 + 20, struct.pack("<I", 63)),
                                  (512 + 489 * 64, struct.pack("<f", float("nan")))):
                tampered = root_body[:offset] + value + root_body[offset + len(value):]
                rejected(lambda: router_directory(tampered, dict(discovery, root_sha256=identity(tampered)["sha256"]),
                                                   1000000, 768))
            # Both same-length hash tamper and truncation fail the anchored chain.
            audit_path = repo / OFFERED / "root-audit.json"
            audit_body = audit_path.read_bytes()
            for tampered in (b"X" + audit_body[1:], audit_body[:-1]):
                audit_path.write_bytes(tampered)
                rejected(lambda: authenticate(repo))
            audit_path.write_bytes(audit_body)
        target = repo / "closed.json"
        closed = dict(complete=True, status="FAIL")
        atomic_output(target, closed)
        assert json.loads(target.read_bytes()) == closed
        rejected(lambda: atomic_output(target, closed))
        assert json.loads(target.read_bytes()) == closed
        rejected(lambda: atomic_output(repo / "oversize.json", dict(body="x" * OUTPUT_LIMIT)))
        assert not (repo / "oversize.json").exists()
        rejected(lambda: atomic_output(repo / "unclosed.json", dict(complete=False)))
        with patch.object(os, "link", side_effect=OSError("synthetic publication failure")):
            rejected(lambda: atomic_output(repo / "interrupted.json", closed))
        assert not (repo / "interrupted.json").exists() and not list(repo.glob(".locality-*"))
    return dict(self_check="PASS", exhaustive_cover_cases=cases, synthetic_authority_reads=before,
                checks="bijection/reverse/partial-tail/smallest-gap/ties/coverage/seed-closure/SQ8-gaps/"
                "admission/tamper/no-GT/no-query-read/atomic-no-replacement")


def synthetic_authority(repo):
    """Tiny synthetic bodies with a complete authority topology; no real replay."""
    def body(name, value):
        path = repo / name
        path.parent.mkdir(parents=True, exist_ok=True)
        data = value if isinstance(value, bytes) else encoded(value)
        path.write_bytes(data)
        return dict(path=str(name), **identity(data))

    science = Path("synthetic-science")
    store = "screen/store/synthetic/"
    artifacts = {}

    def artifact(name, value):
        descriptor = body(science / name, value)
        artifacts[name] = pin_only(descriptor)
        return pin_only(descriptor)

    source_pins = {name: pin_only(body(name, b"synthetic native source")) for name in SOURCE_FILES}
    native = dict(source_sha256={name: pin["sha256"] for name, pin in source_pins.items()})
    native["source_identity_sha256"] = identity(encoded(native["source_sha256"]))["sha256"]
    native_pin = body("synthetic-cold/native/native-source-manifest.json", native)
    helper_pin = body("scripts/replay_fixed48_source_get_caps.py", b"synthetic helper")
    member = struct.pack("<31250I", *(u // 64 for u in range(31250)))
    membership = artifact(store + "router/membership.bin", member)
    plane = dict(rows=1000000, dimensions=768, page_rows=32, record_bytes=200, query_or_truth_used=False,
                 schema="borsuk-two-bit-plane-v3", **{field: "1" * 64 for field in
                 ("source_sha256", "sq8_sha256", "mean_sha256", "records_sha256", "source_order_sha256")})
    plane_pin = artifact(store + "plane/manifest.json", plane)
    page_pin = artifact(store + "page_manifest.json", dict(rows=1000000, dimensions=768, page_rows=256, object_sha256="1" * 64))
    discovery = dict(mode="semantic", profile="fresh1m", membership_bytes=len(member), membership_sha256=membership["sha256"],
                     root_bytes=512 + 489 * 3136, root_sha256="2" * 64, leaves_bytes=31250 * 1540, leaves_sha256="3" * 64,
                     centroids_sha256="5" * 64,
                     input_schema=plane["schema"], input_root_sha256=plane_pin["sha256"],
                     **{field: plane[field] for field in ("source_sha256", "sq8_sha256", "mean_sha256", "records_sha256", "source_order_sha256")})
    router = bytearray(discovery["root_bytes"])
    router[:8] = b"BORSUSR2"
    struct.pack_into("<IIQIIIIII", router, 8, 512, 2, 1000000, 768, 31250, 489, 489, 489, len(plane["schema"]))
    struct.pack_into("<QQQQQ", router, 48, 32 + 31250 * 768 * 2, len(member), 31250 * 1540, 100, 512 << 20)
    for offset, field in ((128, "input_root_sha256"), (160, "centroids_sha256"),
                          (192, "membership_sha256"), (224, "leaves_sha256")):
        router[offset:offset + 32] = bytes.fromhex(discovery[field])
    router[256:256 + len(plane["schema"])] = plane["schema"].encode()
    for leaf in range(489):
        count = min(64, 31250 - leaf * 64)
        struct.pack_into("<IIQIII", router, 512 + leaf * 64, leaf, 0, leaf * 64 * 1540, count * 1540, count, count * 32)
        router[512 + leaf * 64 + 32:512 + leaf * 64 + 64] = bytes.fromhex("6" * 64)
    router_pin = artifact(store + "router/root.bin", bytes(router))
    discovery["root_sha256"] = router_pin["sha256"]
    root = dict(discovery=discovery, plane_manifest_sha256=plane_pin["sha256"], page_manifest_sha256=page_pin["sha256"],
                sq8_object_sha256="1" * 64)
    root_pin = artifact(store + "manifest.json", root)
    for kind, name in (("leaves", "router/leaves.bin"),):
        artifacts[store + name] = dict(bytes=discovery[kind + "_bytes"], sha256=discovery[kind + "_sha256"])
    requests = identity(b"never read")
    artifacts["screen/requests.jsonl"] = requests
    order = dict(bytes=8000000, sha256="1" * 64)
    scorer = dict(rows=1000000, dimensions=768, first=0, count=64, profile="fresh1m", generation_prefix="synthetic",
                  generation_root_sha256=root_pin["sha256"], requests=requests, order=order)
    scorer_pin = artifact("screen/scorer-config.json", scorer)
    identities = dict(binary_sha256="4" * 64, router_source_sha256=source_pins[SOURCE_FILES[3]]["sha256"],
                      scorer_source_sha256=source_pins[SOURCE_FILES[4]]["sha256"], config_sha256=scorer_pin["sha256"])
    summary = dict(complete=True, truth_opened=False, status="FROZEN", queries=64,
                   generation_root_sha256=root_pin["sha256"], **identities,
                   requests_sha256=requests["sha256"], requests_bytes=requests["bytes"],
                   order_sha256=order["sha256"], order_bytes=order["bytes"])
    events = [dict(phase="identity", **identities), dict(phase="startup")]
    events += [dict(phase="frozen_query", ordinal=i) for i in range(64)]
    events += [dict(phase="all_queries_frozen", count=64, summary=summary), dict(phase="terminal", summary=summary)]
    trace_pin = artifact("screen/records.jsonl", b"\n".join(encoded(event) for event in events) + b"\n")
    receipt = dict(exit_status=0, cleanup_complete=True, measurement_sealed_before_truth=True,
                   native_qualification_passed=True, resource_gate_passed=True, measurements=trace_pin,
                   scorer_config=scorer_pin, requests=requests, order=order,
                   generation_prefix="synthetic", generation_root_sha256=root_pin["sha256"],
                   **{key: identities[key] for key in ("binary_sha256", "router_source_sha256", "scorer_source_sha256")})
    artifact("screen/measurement-receipt.json", receipt)
    files = {"requests.jsonl": requests, "records.jsonl": trace_pin}
    artifact("screen/COMPLETE.json", dict(execution_status="SUCCESS", scientific_status="GO", files=files,
                                          roster_sha256=identity(encoded(files))["sha256"]))
    terminal = body(science / "terminal.json", dict(phase="complete", status="complete", exit_code=0,
                                                   original_exit_code=0, artifacts=artifacts))
    validation = body(science / "validation.json", dict(closed_validation_passed=True, scientific_status="GO",
                         state="terminated", measurement_rerun=False, terminal=pin_only(terminal)))
    cold = body("synthetic-cold/config.json", dict(proofs=dict(scientific_terminal=terminal, historical_validation=validation)))
    publication = body("synthetic-cold/publication.json", dict(manifest=root, arm=dict(
        metadata_files={"router/root.bin": discovery["root_bytes"], "router/membership.bin": len(member)},
        metadata_sha256={"router/root.bin": discovery["root_sha256"], "router/membership.bin": membership["sha256"]})))
    config = body(OFFERED / "config.json", dict(cold_run=dict(files={"config.json": cold,
                     "screen/publication.json": publication, "native/native-source-manifest.json": native_pin})))
    rate = body(OFFERED / "screen/rate5-records.jsonl", b"\n".join(encoded(dict(query_ordinal=i)) for i in range(64)) + b"\n")
    saved = body(SOURCE32, dict(schema="borsuk-fixed48-source-get-cap-geometry-v1", status="PASS_CONDITIONAL_GEOMETRY",
                               query_count=64, source_byte_cap=SOURCE_BUDGET,
                               native_source_identity_sha256=native["source_identity_sha256"], script=pin_only(helper_pin),
                               source_files=source_pins, queries=[dict(ordinal=i) for i in range(64)], authenticated_inputs={}))
    audit = body(OFFERED / "root-audit.json", dict(actual_closed_validator_passed=True,
                 all_declared_artifacts_authenticated=True, scientific_status="PASS", authenticated_artifacts={
                     "config.json": pin_only(config), "screen/rate5-records.jsonl": pin_only(rate)}))
    for name in ("requests.jsonl", "queries.raw", "truth.i64"):
        body(name, b"never read")
    return pin_only(audit), pin_only(saved), membership


def atomic_output(path, result):
    require(result.get("complete") is True, "output is not closed")
    data = encoded(result) + b"\n"
    require(len(data) <= OUTPUT_LIMIT, "output exceeds 2 MiB")
    require(not os.path.lexists(path), "output already exists; evidence is immutable")
    temporary = None
    try:
        with tempfile.NamedTemporaryFile(prefix=".locality-", dir=path.parent, delete=False) as out:
            temporary = Path(out.name)
            out.write(data)
            out.flush()
            os.fsync(out.fileno())
        # Atomic publication of a fully closed body, with no replacement race.
        os.link(temporary, path)
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)


def resource_guard():
    resource.setrlimit(resource.RLIMIT_AS, (512 << 20, 512 << 20))
    resource.setrlimit(resource.RLIMIT_CPU, (120, 120))
    os.sched_setaffinity(0, {min(os.sched_getaffinity(0))})
    def timeout(*_):
        raise TimeoutError("120-second wall cap")

    signal.signal(signal.SIGALRM, timeout)
    signal.alarm(120)
    group = Path("/sys/fs/cgroup") / Path("/proc/self/cgroup").read_text().strip().split("0::", 1)[1].lstrip("/")
    memory = (group / "memory.max").read_text().strip()
    swap = (group / "memory.swap.max").read_text().strip()
    quota, period = (group / "cpu.max").read_text().split()
    require(memory != "max" and int(memory) <= 512 << 20 and swap == "0" and
            quota != "max" and int(quota) <= int(period), "requires cgroup <=512 MiB/one CPU/zero swap")

    def pressure_check():
        events = dict(line.split() for line in (group / "memory.events").read_text().splitlines())
        require(all(int(events.get(key, 0)) == 0 for key in ("max", "oom", "oom_kill", "oom_group_kill")),
                "INVALID: memory pressure/resource event")
        pressure = (group / "memory.pressure").read_text().splitlines()
        require(all(int(line.split("total=")[1]) == 0 for line in pressure), "INVALID: memory pressure")

    pressure_check()
    return pressure_check


if __name__ == "__main__":
    if not __debug__:
        raise SystemExit("INVALID: self-check requires assertions enabled")
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo", type=Path, default=Path(__file__).resolve().parents[1])
    modes = parser.add_mutually_exclusive_group(required=True)
    modes.add_argument("--self-check", action="store_true")
    modes.add_argument("--output", type=Path)
    modes.add_argument("--replay", type=Path)
    args = parser.parse_args()
    try:
        pressure_check = resource_guard()
        checks = self_check()
        pressure_check()
        if args.self_check:
            print(json.dumps(checks, sort_keys=True))
        else:
            if args.output:
                require(not os.path.lexists(args.output), "output already exists; evidence is immutable")
            previous = None
            if args.replay:
                require(args.replay.is_file() and not args.replay.is_symlink() and
                        args.replay.stat().st_size <= OUTPUT_LIMIT, "invalid existing result path/size")
                previous = json.loads(args.replay.read_bytes())
            result = replay(args.repo, pressure_check)
            result["verification"] = checks
            require(len(encoded(result)) + 1 <= OUTPUT_LIMIT, "output exceeds 2 MiB")
            if args.output:
                pressure_check()
                atomic_output(args.output, result)
            else:
                require(previous == result, "existing replay result does not match authenticated recomputation")
            print(json.dumps(dict(status=result["status"], complete=True, totals=result["totals"],
                                  failures=result["failures"], replay_verified=bool(args.replay)), sort_keys=True))
    except (ValueError, OSError, KeyError, TypeError, AssertionError, MemoryError, RuntimeError, struct.error) as error:
        print(json.dumps(dict(status="INVALID", complete=False, error=f"{type(error).__name__}: {error}"), sort_keys=True))
        raise SystemExit(2) from error
