#!/usr/bin/env python3
"""Reduce sealed, qualified global-leaf nominations; never run a native search.

CLI: CONFIG CONFIG_SHA256 NEW_OUTPUT | --self-check
CONFIG is root-frozen JSON, with strict schemas described by contract() below.
All paths are absolute local regular files; descriptors pin exact bytes/SHA256.
Both full nomination bodies and the closed root receipt are authenticated and
validated before either LE-u32 logical/source-ordinal truth panel is opened.
No physical map, corpus records, network, ranking, or parameter search is used.
Run synthetic checks in a <=256MiB/no-swap/one-CPU cgroup with a 120s deadline.
"""

import argparse
import copy
import hashlib
import json
import math
import os
from pathlib import Path
import stat
import struct
import sys
import tempfile


MIB = 1 << 20
SCHEMA = "borsuk-global-leaf-sealed-coverage-v1"
NATIVE = "borsuk-hierarchical-cells-nomination-v1"
POLICIES = ["hierarchical8_and24", "global_top24"]
DATASETS = ["relaion", "cohere"]
SPLIT = "already consumed historical first64; not fresh holdout"
PROTOCOL = dict(bytes=3288, sha256="49c413b3624fa6135a119205990487596662d89b3637221b8373366f0f0d2b91")
AUDIT = dict(bytes=186248, sha256="ed420f8a4562fe36b0343c66356cf5f573d6493a875e0621fd4ba1f230484e3e")
ROOTS = dict(relaion="fec06fb0eee4a1380a42356af3ef43d38137a95b4bdb09d457b434b5369c076b",
             cohere="89aee41edba2f4bc447fc5fb66ecee3f470e4ff50627c6f294da6208fdbcb6b2")
REQUESTS = dict(relaion=dict(bytes=937167, sha256="6da3f26a2a5f90b51d345726be5cab2e1a69fb75000cfe83f451442e1f797330"),
                cohere=dict(bytes=983232, sha256="23afccf4caac37a9bc413d63a7106f0ba3170b270730197af3ebf6a998804654"))
TRUTHS = dict(relaion="3ad233f399ba5172051e747f24dbde402bdd357a69460c349ec5ed5872ee5a5c",
              cohere="f6630d0edf06539752c3fbf129ae01e58d3a3cf7b6aefa4decaa9c979e8ba355")
BASELINES = dict(relaion=(5513, 55), cohere=(4788, 51))
BUILD_ARTIFACTS = "generation plane canonical order records mean sq8".split()
BUILD_INTS = "cell_rows sample_rows max_depth max_build_payload_bytes max_output_bytes".split()
STATS = "submitted_gets requested_bytes verified_bytes failed_gets".split()


def require(ok, message):
    if not ok:
        raise ValueError(message)


def keys(obj, names, label):
    require(type(obj) is dict and set(obj) == set(names.split() if isinstance(names, str) else names),
            label + ": exact fields")
    return obj


def integer(value, low=0, high=(1 << 63) - 1):
    require(type(value) is int and low <= value <= high, "integer domain")
    return value


def sha(value):
    require(type(value) is str and len(value) == 64 and all(c in "0123456789abcdef" for c in value),
            "lowercase SHA256")
    return value


def encoded(value, sort=True):
    return json.dumps(value, sort_keys=sort, separators=(",", ":"), ensure_ascii=False, allow_nan=False).encode()


def same(value, expected, label):
    # JSON equality distinguishes false/0 and true/1 at every nested field.
    require(encoded(value) == encoded(expected), label)


def pin(body):
    return dict(bytes=len(body), sha256=hashlib.sha256(body).hexdigest())


def content_pin(desc):
    return {k: desc[k] for k in ("bytes", "sha256")}


def descriptor(desc, cap):
    keys(desc, "path bytes sha256", "artifact")
    require(type(desc["path"]) is str and Path(desc["path"]).is_absolute(), "absolute artifact path")
    integer(desc["bytes"], 1, cap)
    sha(desc["sha256"])
    return desc


def regular(path):
    fd = os.open(path, os.O_RDONLY | os.O_NONBLOCK | os.O_NOFOLLOW)
    try:
        require(stat.S_ISREG(os.fstat(fd).st_mode), "regular file required")
        return os.fdopen(fd, "rb")
    except BaseException:
        os.close(fd)
        raise


def read_artifact(desc, cap, retain=True):
    descriptor(desc, cap)
    digest, chunks, size = hashlib.sha256(), [], 0
    with regular(desc["path"]) as stream:
        require(os.fstat(stream.fileno()).st_size == desc["bytes"], "artifact exact size")
        while size < desc["bytes"]:
            block = stream.read(min(65536, desc["bytes"] - size))
            require(bool(block), "artifact truncated")
            digest.update(block)
            size += len(block)
            if retain:
                chunks.append(block)
        require(not stream.read(1) and digest.hexdigest() == desc["sha256"], "artifact SHA256/length")
    return b"".join(chunks) if retain else None


def parse(body):
    def pairs(items):
        result = {}
        for key, value in items:
            require(key not in result, "duplicate JSON field")
            result[key] = value
        return result

    def invalid_constant(value):
        raise ValueError("nonfinite JSON constant: " + value)

    return json.loads(body, object_pairs_hook=pairs, parse_constant=invalid_constant)


def load(desc, cap=MIB):
    return parse(read_artifact(desc, cap))


def f32_bits(value):
    require(type(value) in (float, int) and math.isfinite(value), "finite float")
    bits = struct.unpack("<I", struct.pack("<f", value))[0]
    require(bits & 0x7f800000 != 0x7f800000, "finite f32")
    return bits


def finite_bits(value):
    integer(value, 0, 0xffffffff)
    require(value & 0x7f800000 != 0x7f800000, "finite f32 bits")
    return value


def roster(values, cap=100000):
    require(type(values) is list and len(values) <= cap, "bounded source-ID roster")
    for value in values:
        integer(value, 0, 99999)
    ids = set(values)
    require(len(ids) == len(values), "duplicate source ID")
    return ids


def build_identity(value):
    keys(value, ["schema", *BUILD_ARTIFACTS, *BUILD_INTS], "BuildConfig")
    require(value["schema"] == "borsuk-hierarchical-cells-build-v1", "build schema")
    ordered = dict(schema=value["schema"])
    for name in BUILD_ARTIFACTS:
        d = descriptor(value[name], 1 << 40)  # descriptors only; never open original corpus/map
        ordered[name] = {key: d[key] for key in ("path", "bytes", "sha256")}
    for name in BUILD_INTS:
        ordered[name] = integer(value[name], 1)
    integer(value["cell_rows"], 1, 512)
    integer(value["sample_rows"], 2, 1024)
    integer(value["max_depth"], 1, 32)
    return pin(encoded(ordered, sort=False))["sha256"]  # Rust struct declaration order


def span(value, total, cap):
    keys(value, "offset bytes sha256", "span")
    integer(value["offset"], 0, total)
    integer(value["bytes"], 1, cap)
    require(value["offset"] + value["bytes"] <= total, "span bounds")
    sha(value["sha256"])
    return value


def layout(root_desc, directory_desc):
    root = load(root_desc, 65536)
    keys(root, "schema input rows dimensions seed mean low step root_directory directory_bytes directory_sha256 cell_bytes build", "manifest")
    same({k: root[k] for k in ("schema", "rows", "dimensions", "seed")},
         dict(schema="borsuk-hierarchical-cells-resident-v3", rows=100000, dimensions=768, seed=20260923), "original geometry")
    source_sha = build_identity(root["input"])
    for name in ("mean", "low", "step"):
        require(type(root[name]) is list and len(root[name]) == 768, "manifest vector")
        for value in root[name]:
            f32_bits(value)
            if name == "step":
                require(value > 0, "positive SQ8 step")
    build = root["build"]
    keys(build, "root_sha256 cells directories max_cell_rows max_depth geometry_fallbacks canonical_row_reads modeled_build_payload_bytes input_bytes output_bytes", "BuildReceipt")
    for name in build:
        if name != "root_sha256":
            integer(build[name])
    cells_count = integer(build["cells"], 24, 100000)
    pages_count = integer(build["directories"], 1, 100000)
    integer(build["max_cell_rows"], 1, root["input"]["cell_rows"])
    integer(build["max_depth"], 1, root["input"]["max_depth"])
    integer(root["directory_bytes"], 1, 64 * MIB)
    integer(root["cell_bytes"], 1)
    require(root["cell_bytes"] == 100000 * 988, "complete cell byte population")
    require(root_desc["bytes"] + root["directory_bytes"] + root["cell_bytes"] <= root["input"]["max_output_bytes"], "output payload cap")
    same(content_pin(directory_desc), dict(bytes=root["directory_bytes"], sha256=root["directory_sha256"]), "directory identity")
    body = read_artifact(directory_desc, 64 * MIB)
    leaves, pages, pending = {}, {}, [(root["root_directory"], 100000, 0)]
    while pending:
        item, population, depth = pending.pop()
        span(item, len(body), 65536)
        offset, length = item["offset"], item["bytes"]
        require(depth <= root["input"]["max_depth"] and offset not in pages, "directory topology/depth")
        require(len(pages) < pages_count, "directory page count")
        pages[offset] = length
        part = body[offset:offset+length]
        same(pin(part), content_pin(item), "directory page SHA256")
        page = keys(parse(part), "children", "directory")
        require(type(page["children"]) is list and 1 <= len(page["children"]) <= 2, "directory fanout")
        total = 0
        for node in page["children"]:
            keys(node, "rows prototype target", "node")
            rows = integer(node["rows"], 1, 100000)
            total += rows
            require(type(node["prototype"]) is list and len(node["prototype"]) == 768, "prototype geometry")
            bits = [f32_bits(v) for v in node["prototype"]]
            target = node["target"]
            require(type(target) is dict and "kind" in target, "target kind")
            if target["kind"] == "directory":
                keys(target, "kind span", "directory target")
                span(target["span"], offset, 65536)
                pending.append((target["span"], rows, depth + 1))
                continue
            keys(target, "kind cell", "leaf target")
            require(target["kind"] == "cell", "leaf kind")
            cell = keys(target["cell"], "id first_row whole source refinement", "cell")
            cid = integer(cell["id"], 0, cells_count - 1)
            require(cid not in leaves, "duplicate cell ID")
            integer(rows, 1, root["input"]["cell_rows"])
            integer(cell["first_row"], 0, 100000 - rows)
            for tier, width in (("source", 208), ("whole", 988)):
                span(cell[tier], root["cell_bytes"], rows * width)
                require(cell[tier]["bytes"] == rows * width, "cell byte geometry")
            require(cell["source"]["offset"] == cell["whole"]["offset"], "source/whole offset")
            require(type(cell["refinement"]) is list and len(cell["refinement"]) == (rows + 31) // 32, "refinement count")
            end = cell["source"]["offset"] + cell["source"]["bytes"]
            for index, block in enumerate(cell["refinement"]):
                expected = min(32, rows - index * 32) * 780
                span(block, root["cell_bytes"], expected)
                require(block["offset"] == end and block["bytes"] == expected, "refinement extent")
                end += expected
            require(end == cell["whole"]["offset"] + cell["whole"]["bytes"], "whole extent")
            leaves[cid] = dict(rows=rows, prototype_bits=bits, first_row=cell["first_row"],
                               source_offset=cell["source"]["offset"], source_bytes=cell["source"]["bytes"],
                               source_sha256=cell["source"]["sha256"], whole_cell_bytes=cell["whole"]["bytes"])
        require(total == population, "directory population")
    end = 0
    for offset, length in sorted(pages.items()):
        require(offset == end, "directory complete disjoint coverage")
        end += length
    require(end == len(body) and len(pages) == pages_count and len(leaves) == cells_count, "complete layout counts")
    row, byte = 0, 0
    for cid, leaf in sorted(leaves.items()):
        require(cid < cells_count and leaf["first_row"] == row and leaf["source_offset"] == byte, "complete cell layout")
        row += leaf["rows"]
        byte += leaf["whole_cell_bytes"]
    require(row == 100000 and byte == root["cell_bytes"], "cell population/byte coverage")
    return root, leaves, source_sha


def stats(gets, size):
    return dict(submitted_gets=gets, requested_bytes=size, verified_bytes=size, failed_gets=0)


def payload(limits, build):
    return (8 * 32 * (768 * 4 + ((build["cell_rows"] + 31) // 32) * 128 + 512)
            + 768 * 16 + 24 * build["cell_rows"] * 128 + 2 * limits["max_source_bytes"] + 65536)


def nomination_config(desc, dataset):
    config = load(desc, 65536)
    keys(config, "schema candidate_root requests first count limits max_resident_directory_payload_bytes max_evaluator_payload_bytes max_result_bytes", "nomination config")
    same([config["schema"], config["first"], config["count"]], [NATIVE, 0, 64], "nomination panel")
    descriptor(config["candidate_root"], 65536)
    descriptor(config["requests"], 32 * MIB)
    require(Path(config["candidate_root"]["path"]).name == "manifest.json", "native candidate root filename")
    # Native absolute paths belong to the closed execution host. Offline copies
    # may move, but their bytes cannot change and the original config stays pinned.
    same(content_pin(config["candidate_root"]), content_pin(dataset["candidate_root"]), "candidate config binding")
    same(content_pin(config["requests"]), content_pin(dataset["requests"]), "request config binding")
    limits = keys(config["limits"], "max_source_gets max_source_bytes max_query_payload_bytes", "limits")
    same(limits["max_source_gets"], 24, "exact source GET limit")
    integer(limits["max_source_bytes"], 1, 16 * MIB)
    integer(limits["max_query_payload_bytes"], 1, 512 * MIB)
    integer(config["max_result_bytes"], 8192, 128 * MIB)
    integer(config["max_resident_directory_payload_bytes"], 1, 512 * MIB)
    integer(config["max_evaluator_payload_bytes"], 1, 256 * MIB)
    require(4 * dataset["requests"]["bytes"] + 32 * MIB <= config["max_evaluator_payload_bytes"], "evaluator admission")
    return config


def query_hashes(desc):
    lines = read_artifact(desc, 32 * MIB).splitlines()
    require(len(lines) == 64, "complete original query panel")
    hashes = []
    for ordinal, line in enumerate(lines):
        request = keys(parse(line), "ordinal query", "request")
        same(request["ordinal"], ordinal, "request ordinal")
        query = request["query"]
        require(type(query) is list and len(query) == 768, "query dimensions")
        bits = [f32_bits(value) for value in query]
        require(any(v & 0x7fffffff for v in bits), "nonzero query")
        hashes.append(pin(struct.pack("<768I", *bits))["sha256"])
    return hashes


def validate_receipt(receipt, policy, root, leaves, limits, membership, owners):
    keys(receipt, "policy resident_leaf_cells routing_distance_evaluations_bound routing_coordinate_evaluations_bound selected primary_ids covered_ids accounting", "NominationReceipt")
    distance_count = len(leaves) if policy == POLICIES[1] else 64 * (root["input"]["max_depth"] + 1) + 24
    same([receipt["policy"], receipt["resident_leaf_cells"], receipt["routing_distance_evaluations_bound"], receipt["routing_coordinate_evaluations_bound"]],
         [policy, len(leaves), distance_count, distance_count * 768], "routing accounting")
    selected = receipt["selected"]
    require(type(selected) is list and len(selected) == 24, "exact 24 selected cells")
    covered, primary, cids, ordering = set(), set(), [], []
    primary_cells, source_bytes, whole_bytes = 0, 0, 0
    for rank, cell in enumerate(selected):
        keys(cell, "cell_id distance distance_bits primary prototype_bits first_row source_ids source_offset source_bytes source_sha256 whole_cell_bytes", "NominatedCell")
        cid = integer(cell["cell_id"], 0, len(leaves) - 1)
        require(cid not in cids, "duplicate selected cell")
        cids.append(cid)
        leaf = leaves[cid]
        for name in ("first_row", "source_offset", "source_bytes", "source_sha256", "whole_cell_bytes", "prototype_bits"):
            same(cell[name], leaf[name], "authenticated leaf " + name)
        bits = finite_bits(cell["distance_bits"])
        require(bits == f32_bits(cell["distance"]), "distance bits/value")
        # Rust total_cmp orders -0 before +0. Finite negative values sort too,
        # but squared-Euclidean distances cannot be negative (except -0).
        require(cell["distance"] >= 0, "nonnegative squared distance")
        ordering.append(((~bits & 0xffffffff) if bits >> 31 else bits ^ 0x80000000, cid))
        require(type(cell["primary"]) is bool, "primary flag")
        if policy == POLICIES[1]:
            require(cell["primary"] is (rank < 8), "global primary first8")
        ids = roster(cell["source_ids"], 512)
        require(len(ids) == leaf["rows"] and not covered & ids, "cell source population/disjointness")
        if cid in membership:
            same(cell["source_ids"], membership[cid], "same cell source order across queries/policies")
        else:
            for source_id in ids:
                require(source_id not in owners, "source ID assigned to multiple cells")
                owners[source_id] = cid
            membership[cid] = cell["source_ids"]
        covered.update(ids)
        if cell["primary"]:
            primary_cells += 1
            primary.update(ids)
        source_bytes += cell["source_bytes"]
        whole_bytes += cell["whole_cell_bytes"]
    require(ordering == sorted(ordering) and primary_cells == 8, "distance/cell-ID ties and primary8")
    same(receipt["primary_ids"], sorted(primary), "exact sorted primary IDs")
    same(receipt["covered_ids"], sorted(covered), "exact sorted covered IDs")
    require(source_bytes <= limits["max_source_bytes"], "source byte cap")
    model = payload(limits, root["input"])
    require(model <= limits["max_query_payload_bytes"], "query payload admission")
    expected = {tier: stats(0, 0) for tier in ("directory", "whole_cell", "refinement")}
    expected.update(source=stats(24, source_bytes), modeled_query_payload_bytes=model,
                    waves=[dict(dependency=1, stage="source", submitted_gets=24,
                                requested_bytes=source_bytes, verified_bytes=source_bytes, max_parallel_gets=1)])
    same(receipt["accounting"], expected, "actual source-only accounting")
    return dict(primary_ids=sorted(primary), covered_ids=sorted(covered), selected_cell_ids=cids,
                primary_cell_ids=[cell["cell_id"] for cell in selected if cell["primary"]],
                actual_source_bytes=source_bytes, modeled_whole_cell_bytes=whole_bytes,
                covered_id_count=len(covered), primary_id_count=len(primary),
                routing_distance_evaluations_bound=distance_count,
                routing_coordinate_evaluations_bound=distance_count * 768)


def nominations(dataset, source):
    config = nomination_config(dataset["nomination_config"], dataset)
    root, leaves, source_sha = layout(dataset["candidate_root"], dataset["directories"])
    hashes = query_hashes(dataset["requests"])
    desc = descriptor(dataset["nominations"], config["max_result_bytes"])
    # Full body hash is checked before parsing. Streaming the second pass keeps
    # memory bounded; hashing it again detects a swap/change between passes.
    read_artifact(desc, config["max_result_bytes"], retain=False)
    identity = dict(phase="identity", schema=NATIVE, config_sha256=dataset["nomination_config"]["sha256"],
                    candidate_root_sha256=dataset["candidate_root"]["sha256"], requests_sha256=dataset["requests"]["sha256"],
                    module_source_sha256=source["module"]["sha256"], binary_source_sha256=source["binary_source"]["sha256"],
                    source_identity=root["input"], source_identity_sha256=source_sha, first=0, count=64,
                    policies=POLICIES, limits=config["limits"], truth_opened=False, record_scoring_performed=False,
                    scientific_qualification=False, quality_or_performance_claim=False, physical_s3_measured=False,
                    modeled_evaluator_payload_bytes=4 * dataset["requests"]["bytes"] + 32 * MIB,
                    startup=stats(1, dataset["candidate_root"]["bytes"]), startup_directory=stats(1, root["directory_bytes"]),
                    candidate_descriptor_authentication_bytes=dataset["candidate_root"]["bytes"],
                    max_resident_directory_payload_bytes=config["max_resident_directory_payload_bytes"])
    parsed = 4 * root["directory_bytes"] + 256 * (root["build"]["cells"] + root["build"]["directories"]) + 8 * 65536
    admission = dict(encoded_bytes=root["directory_bytes"], modeled_parsed_payload_bytes=parsed,
                     modeled_preload_peak_bytes=parsed + root["directory_bytes"])
    require(admission["modeled_preload_peak_bytes"] <= config["max_resident_directory_payload_bytes"], "directory admission")
    digest, prefix, size, prefix_size = hashlib.sha256(), hashlib.sha256(), 0, 0
    rows, membership, owners = [], {}, {}
    with regular(desc["path"]) as stream:
        require(os.fstat(stream.fileno()).st_size == desc["bytes"], "nomination size changed")
        for index in range(131):
            line = stream.readline(8 * MIB + 1)
            require(line.endswith(b"\n") and len(line) <= 8 * MIB and size + len(line) <= desc["bytes"], "exact event newline/cap/roster")
            event = parse(line)
            if index == 0:
                keys(event, [*identity, "directory_admission"], "identity")
                same({k: event[k] for k in identity}, identity, "native identity bindings")
                da = keys(event["directory_admission"], [*admission, "parsed_owned_capacity_bytes"], "directory admission")
                same({k: da[k] for k in admission}, admission, "directory memory model")
                integer(da["parsed_owned_capacity_bytes"], 1, 4 * root["directory_bytes"])
            elif index <= 128:
                ordinal, arm = divmod(index - 1, 2)
                keys(event, "phase ordinal query_sha256 truth_opened receipt", "selection event")
                same({k: event[k] for k in ("phase", "ordinal", "query_sha256", "truth_opened")},
                     dict(phase="selection_frozen", ordinal=ordinal, query_sha256=hashes[ordinal], truth_opened=False), "selection ordinal/original query hash/truth")
                row = validate_receipt(event["receipt"], POLICIES[arm], root, leaves, config["limits"], membership, owners)
                row.update(ordinal=ordinal, query_sha256=hashes[ordinal], policy=POLICIES[arm])
                rows.append(row)
            elif index == 129:
                marker = dict(phase="all_selections_frozen", schema=NATIVE, prefix_bytes=prefix_size,
                              prefix_sha256=prefix.hexdigest(), first=0, count=64, selection_receipts=128,
                              policies=POLICIES, truth_opened=False,
                              scope="one_dataset_parent_must_seal_both_before_offline_truth")
                marker.update({k: identity[k] for k in ("config_sha256", "candidate_root_sha256", "requests_sha256",
                                                       "module_source_sha256", "binary_source_sha256", "source_identity_sha256")})
                same(event, marker, "fsynced prefix marker/identity")
            else:
                same(event, dict(phase="terminal", status="NOMINATIONS_FROZEN", complete=True, queries=64,
                                 selection_receipts=128, truth_opened=False, scientific_qualification=False,
                                 quality_or_performance_claim=False), "complete native terminal")
            if index < 129:
                prefix.update(line)
                prefix_size += len(line)
            digest.update(line)
            size += len(line)
        require(not stream.read(1) and size == desc["bytes"] and digest.hexdigest() == desc["sha256"], "exact131 events/body unchanged")
    return dict(rows=rows, prefix=dict(bytes=prefix_size, sha256=prefix.hexdigest()),
                source_identity_sha256=source_sha, resident_leaf_cells=len(leaves),
                startup_local_reads={k: identity[k] for k in ("startup", "startup_directory")})


def source_descriptors(source):
    keys(source, "revision archive module binary_source executable reducer", "current source")
    require(type(source["revision"]) is str and len(source["revision"]) == 40
            and all(c in "0123456789abcdef" for c in source["revision"]), "full current revision")
    for name in ("archive", "module", "binary_source", "executable", "reducer"):
        read_artifact(source[name], 1024 * MIB if name in ("archive", "executable") else 4 * MIB, retain=False)
    same(content_pin(source["reducer"]), pin(Path(__file__).read_bytes()), "running reducer source identity")


def closed_receipt(desc, config, sealed):
    receipt = load(desc)
    expected = dict(schema=SCHEMA + "-closed-receipt", status="CLOSED_VALID", execution_exit_code=0,
                    source_qualified=True, resources_qualified=True, cleanup_complete=True,
                    both_sealed_before_truth=True, truth_opened=False,
                    protocol=config["protocol"], historical_audit=config["historical_audit"], source=config["source"])
    keys(receipt, [*expected, "datasets"], "closed root receipt")
    same({k: receipt[k] for k in expected}, expected, "closed execution/resource/cleanup/source qualification")
    keys(receipt["datasets"], DATASETS, "receipt both datasets")
    for name in DATASETS:
        ds = config["datasets"][name]
        entry = receipt["datasets"][name]
        expected_entry = {k: ds[k] for k in ("nomination_config", "nominations", "candidate_root", "directories", "requests")}
        expected_entry.update(prefix=sealed[name]["prefix"], source_identity_sha256=sealed[name]["source_identity_sha256"],
                              execution_exit_code=0, sealed_before_truth=True, fsynced=True, complete=True)
        keys(entry, [*expected_entry, "resources"], "closed dataset receipt")
        same({k: entry[k] for k in expected_entry}, expected_entry, "closed dataset seal/execution binding")
        resources = keys(entry["resources"], "memory_limit_bytes memory_peak_bytes swap_limit_bytes swap_peak_bytes cpu_count oom oom_kill", "nomination resources")
        same({k: resources[k] for k in ("swap_limit_bytes", "swap_peak_bytes", "cpu_count", "oom", "oom_kill")},
             dict(swap_limit_bytes=0, swap_peak_bytes=0, cpu_count=1, oom=0, oom_kill=0), "no swap/CPU1/OOM qualification")
        limit = integer(resources["memory_limit_bytes"], 1, 512 * MIB)
        integer(resources["memory_peak_bytes"], 1, limit)
    return receipt


def authenticate(config):
    keys(config, "schema protocol historical_audit source closed_receipt datasets", "reducer config")
    require(config["schema"] == SCHEMA, "reducer schema")
    keys(config["datasets"], DATASETS, "both required datasets")
    # Merely naming a truth body as a source/proof must not move its open ahead
    # of the boundary. Metadata inspection is allowed; no truth fd is opened.
    truth_descs, pretruth_descs = [], [config["protocol"], config["historical_audit"], config["closed_receipt"]]
    keys(config["source"], "revision archive module binary_source executable reducer", "current source")
    pretruth_descs.extend(config["source"][k] for k in ("archive", "module", "binary_source", "executable", "reducer"))
    for name in DATASETS:
        ds = keys(config["datasets"][name], "candidate_root directories requests nomination_config nominations truth truth_id_space", "dataset")
        truth_descs.append(descriptor(ds["truth"], 25600))
        pretruth_descs.extend(ds[k] for k in ("candidate_root", "directories", "requests", "nomination_config", "nominations"))
    truth_paths = {Path(d["path"]).resolve() for d in truth_descs}
    truth_inodes = {(s.st_dev, s.st_ino) for s in (os.stat(d["path"]) for d in truth_descs)}
    for desc in pretruth_descs:
        descriptor(desc, 1024 * MIB)
        require(Path(desc["path"]).resolve() not in truth_paths, "pretruth artifact aliases truth path")
        metadata = os.stat(desc["path"])
        require((metadata.st_dev, metadata.st_ino) not in truth_inodes, "pretruth artifact aliases truth inode")
        require(all(content_pin(desc) != content_pin(t) for t in truth_descs), "pretruth artifact aliases truth content")
    same(content_pin(config["protocol"]), PROTOCOL, "frozen preregistration pin")
    same(content_pin(config["historical_audit"]), AUDIT, "historical audit pin")
    protocol, audit = load(config["protocol"]), load(config["historical_audit"])
    require(protocol["split"] == SPLIT and audit["audit_status"] == "VALID", "protocol/audit scope")
    source_descriptors(config["source"])
    sealed = {}
    for name in DATASETS:
        ds = keys(config["datasets"][name], "candidate_root directories requests nomination_config nominations truth truth_id_space", "dataset")
        for key in ("candidate_root", "directories", "requests", "nomination_config", "nominations", "truth"):
            descriptor(ds[key], 128 * MIB)
        require(ds["candidate_root"]["sha256"] == ROOTS[name], "exact original root hash")
        same(content_pin(ds["requests"]), REQUESTS[name], "exact original queries")
        same(content_pin(ds["truth"]), dict(bytes=25600, sha256=TRUTHS[name]), "original truth descriptor")
        require(ds["truth_id_space"] == "logical_source_ordinal_le_u32", "logical truth ID space; no physical map")
        sealed[name] = nominations(ds, config["source"])
    # Explicit root authority is mandatory. A native marker alone is never
    # treated as execution, resource, source, or cleanup qualification.
    closed_receipt(config["closed_receipt"], config, sealed)
    return sealed, audit


def hits_summary(values):
    require(len(values) == 64, "full64 aggregate")
    for value in values:
        integer(value, 0, 100)
    return dict(total_hits=sum(values), mean_coverage_fraction=sum(values) / 6400,
                p05_hits=sorted(values)[3])


def counters(values):
    return dict(total=sum(values), mean=sum(values) / 64, minimum=min(values), maximum=max(values))


def reduce_dataset(seal, truth, historical):
    ids = struct.unpack("<6400I", truth)
    truths = [roster(list(ids[i:i+100]), 100) for i in range(0, 6400, 100)]
    paired = []
    policies = {p: [] for p in POLICIES}
    for ordinal, gold in enumerate(truths):
        pair = []
        for arm, policy in enumerate(POLICIES):
            row = copy.deepcopy(seal["rows"][ordinal * 2 + arm])
            row["primary_hits"] = len(gold.intersection(row.pop("primary_ids")))
            row["fetched_hits"] = len(gold.intersection(row.pop("covered_ids")))
            row["boundary_recovered_hits"] = row["fetched_hits"] - row["primary_hits"]
            policies[policy].append(row)
            pair.append(row)
        paired.append(dict(ordinal=ordinal, query_sha256=pair[0]["query_sha256"],
                           **{key + "_delta_global_minus_hierarchy": pair[1][key] - pair[0][key]
                              for key in ("primary_hits", "fetched_hits", "covered_id_count", "actual_source_bytes", "modeled_whole_cell_bytes")}))
    summaries = {}
    for policy, rows in policies.items():
        summaries[policy] = dict(primary=hits_summary([r["primary_hits"] for r in rows]),
                                fetched=hits_summary([r["fetched_hits"] for r in rows]), per_query=rows,
                                **{key: counters([r[key] for r in rows]) for key in
                                   ("actual_source_bytes", "modeled_whole_cell_bytes", "covered_id_count", "primary_id_count",
                                    "routing_distance_evaluations_bound", "routing_coordinate_evaluations_bound")})
    # Same-control authentication includes per-query hits and occupancy/bytes,
    # rather than accepting only matching means on a changed historical panel.
    require(len(historical["per_query"]) == 64, "historical full64")
    for row, original in zip(policies[POLICIES[0]], historical["per_query"]):
        same([row["ordinal"], row["primary_hits"], row["fetched_hits"], row["covered_id_count"], row["modeled_whole_cell_bytes"]],
             [original["ordinal"], original["native_loss"]["primary_hits"], original["coverage_ceiling_hits"],
              original["roster_sizes"]["covered"], original["local_reads"]["requested_bytes"]], "historical hierarchy per-query reproduction")
    return dict(policies=summaries, paired_per_query=paired, prefix=seal["prefix"],
                resident_leaf_cells=seal["resident_leaf_cells"], startup_local_reads=seal["startup_local_reads"])


def decisions(datasets):
    supports, adequate = True, True
    for name in DATASETS:
        control = datasets[name]["policies"][POLICIES[0]]["fetched"]
        candidate = datasets[name]["policies"][POLICIES[1]]["fetched"]
        same([control["total_hits"], control["p05_hits"]], list(BASELINES[name]), "original coverage baseline")
        supports &= candidate["total_hits"] > control["total_hits"] and candidate["p05_hits"] >= control["p05_hits"]
        adequate &= candidate["total_hits"] >= 6272 and candidate["p05_hits"] >= 95
    return dict(supports_pruning_contribution=supports, adequate_consumed_coverage=adequate,
                global24_common_next_remedy=supports and adequate,
                otherwise="global24 not common next remedy; does not exonerate all pruning policies or establish quantizer/metric cause")


def evaluate(config, config_sha):
    sealed, audit = authenticate(config)
    # This is the ONLY truth-open boundary. Every dataset, marker, nomination
    # body, source descriptor, and closed execution proof has already passed.
    truth_bodies = {name: read_artifact(config["datasets"][name]["truth"], 25600) for name in DATASETS}
    datasets = {name: reduce_dataset(sealed[name], truth_bodies[name], audit["datasets"][name]) for name in DATASETS}
    return dict(schema=SCHEMA, status="VALID", config_sha256=config_sha, split=SPLIT, rows=100000,
                dimensions=768, k=100, queries=64, policies=POLICIES, datasets=datasets, decision=decisions(datasets),
                authenticated_inputs=config, scope=dict(fresh_holdout=False, returned_recall=False,
                    ranking_performed=False, sq2_or_sq8_scoring=False, physical_map_used=False,
                    s3_measurement=False, vendor_claim=False, production_promotion=False,
                    source_membership="qualified native authenticated source-range rosters; no independent source-record replay",
                    bytes="actual authenticated local SOURCE reads; WholeCell bytes modeled separately and never fetched here"))


def write_new(path, result):
    body = encoded(result) + b"\n"
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW
    with os.fdopen(os.open(path, flags, 0o644), "wb") as stream:
        stream.write(body)
        stream.flush()
        os.fsync(stream.fileno())
    fd = os.open(Path(path).parent, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)
    return pin(body)


def run(config_path, expected_sha, output):
    require(not os.path.lexists(output), "output exists; never overwrite")
    sha(expected_sha)
    with regular(config_path) as stream:
        body = stream.read(MIB + 1)
    require(0 < len(body) <= MIB and pin(body)["sha256"] == expected_sha, "CONFIG SHA256/cap")
    return write_new(output, evaluate(parse(body), expected_sha))


def contract():
    """Machine-readable integration contract; no protocol/freeze is authored here."""
    return dict(schema=SCHEMA + "-contract", cli="python3 scripts/reduce_hierarchical_global_leaf_coverage.py CONFIG CONFIG_SHA256 NEW_OUTPUT",
                self_check="python3 scripts/reduce_hierarchical_global_leaf_coverage.py --self-check",
                artifact="{path:absolute_local_regular_file, bytes:positive_integer, sha256:lowercase64}; no symlink/FIFO; no gzip substitution",
                config=dict(schema=SCHEMA, protocol="artifact pinned to PROTOCOL", historical_audit="artifact pinned to AUDIT",
                            source=dict(revision="full40 qualified current source revision", archive="artifact; streamed <=1GiB",
                                        module="artifact of current Rust module", binary_source="artifact of current Rust CLI source",
                                        executable="artifact of qualified executable; streamed <=1GiB",
                                        reducer="artifact matching this running Python file"),
                            closed_receipt="artifact of root-owned aggregate JSON (schema below)",
                            datasets={name: dict(candidate_root="artifact; EXACT original manifest hash; restored body required",
                                                 directories="artifact; manifest directory_bytes/directory_sha256",
                                                 requests="artifact; EXACT original requests64 hash/bytes",
                                                 nomination_config="artifact; strict original native NominationConfig; root/requests bytes+SHA equal dataset descriptors; local copy paths may differ",
                                                 nominations="artifact; exact native sealed JSONL <=128MiB",
                                                 truth="artifact; EXACT original 25600-byte truth64",
                                                 truth_id_space="logical_source_ordinal_le_u32") for name in DATASETS}),
                closed_receipt=dict(schema=SCHEMA + "-closed-receipt", status="CLOSED_VALID", execution_exit_code=0,
                                    source_qualified=True, resources_qualified=True, cleanup_complete=True,
                                    both_sealed_before_truth=True, truth_opened=False,
                                    protocol="exact config.protocol descriptor", historical_audit="exact config.historical_audit descriptor",
                                    source="exact config.source object",
                                    datasets={name: dict(nomination_config="exact dataset descriptor", nominations="exact dataset descriptor",
                                                        candidate_root="exact dataset descriptor", directories="exact dataset descriptor", requests="exact dataset descriptor",
                                                        prefix="{bytes,sha256} computed from identity+128 selections INCLUDING newlines",
                                                        source_identity_sha256="Rust compact BuildConfig serialization SHA (declaration order)",
                                                        execution_exit_code=0, sealed_before_truth=True, fsynced=True, complete=True,
                                                        resources=dict(memory_limit_bytes="1..536870912", memory_peak_bytes="1..memory_limit_bytes",
                                                                       swap_limit_bytes=0, swap_peak_bytes=0, cpu_count=1, oom=0, oom_kill=0)) for name in DATASETS}),
                pins=dict(protocol=PROTOCOL, historical_audit=AUDIT, roots=ROOTS, requests=REQUESTS, truths=TRUTHS),
                roster=["identity", "128 selection_frozen: ordinal0..63, hierarchy then global", "all_selections_frozen", "terminal NOMINATIONS_FROZEN complete=true"],
                checks=["Strict fields/types/duplicate JSON rejection; all131 events, newline/line/body caps and full body pin",
                        "Both datasets: original layout, original query bytes, current source/binary bindings, prefix bytes/SHA and marker identities",
                        "Root receipt asserts exact source qualification and execution0/resources/cleanup/both-fsynced-before-truth; never inferred from marker",
                        "All pretruth admission passes before either GT is opened; both truths pinned LEu32 64x100 unique logical IDs0..99999",
                        "Authenticated complete directory topology, source ranges/population/prototypes; selected24 unique primary8, total_cmp/cell-ID ordering",
                        "Source-ID roster disjointness, consistent per-cell membership across all selections; source-ID uniqueness and original source ordinal bounds",
                        "Native source-only Accounting exact payload/wave/counters; actual SOURCE bytes vs modeled WholeCell bytes",
                        "Independent primary/fetched truth intersections, paired per-query deltas and p05 fourth-lowest",
                        "Hierarchy reproduces original audit primary/fetched hits, covered counts and WholeCell bytes per query; frozen baseline and decision thresholds",
                        "Output deterministic and create-exclusive+fsynced; existing output refused before truth, invalid exit2"],
                limits="No native execution, corpus replay, physical mapping, ranking, remote/S3/vendor claim; original consumed first64 explicit",
                trust="Trusted CONFIG_SHA supplied out of band by root; aggregate receipt is root's attestation backed by its independently verified evidence. This reducer does not execute qualification gates or independently replay source records/distance arithmetic.",
                required_before_root_freeze="Fill authenticated descriptors and closed receipt using actual qualified current source, executable, two sealed runs and cleanup; synthetic fixtures are not production evidence.")


def synthetic_fixture(directory):
    """Pure synthetic files only; never read repository corpus or real truth."""
    def save(name, body):
        if not isinstance(body, bytes):
            body = encoded(body)
        path = directory / name
        path.write_bytes(body)
        return dict(path=str(path), **pin(body))

    source = {key: save(key, ("synthetic " + key).encode()) for key in ("archive", "module", "binary_source", "executable")}
    source.update(revision="1" * 40, reducer=dict(path=str(Path(__file__).resolve()), **pin(Path(__file__).read_bytes())))
    dummy = dict(path="/never-open-synthetic-corpus-or-physical-map", bytes=1, sha256="a" * 64)
    build = dict(schema="borsuk-hierarchical-cells-build-v1", **{n: dummy.copy() for n in BUILD_ARTIFACTS},
                 cell_rows=512, sample_rows=1024, max_depth=16, max_build_payload_bytes=256 * MIB, max_output_bytes=256 * MIB)
    # Four-row cells give small receipts; other cells fill the full100k population.
    sizes = [4] * 32 + [509 + (i < 108) for i in range(196)]
    require(sum(sizes) == 100000, "synthetic population")
    nodes, byte, row = [], 0, 0
    members = {}
    for cid, rows in enumerate(sizes):
        members[cid] = [((row + i) * 7919 + 123) % 100000 for i in range(rows)]
        refinements = []
        start = byte + rows * 208
        for first in range(0, rows, 32):
            length = min(32, rows - first) * 780
            refinements.append(dict(offset=start, bytes=length, sha256="b" * 64))
            start += length
        cell = dict(id=cid, first_row=row, source=dict(offset=byte, bytes=rows * 208, sha256=pin(encoded(members[cid]))["sha256"]),
                    whole=dict(offset=byte, bytes=rows * 988, sha256="c" * 64), refinement=refinements)
        nodes.append(dict(rows=rows, prototype=[0.0] * 768, target=dict(kind="cell", cell=cell)))
        byte += rows * 988
        row += rows
    pages = bytearray()
    page_count = 0

    def tree(children):
        nonlocal page_count
        if len(children) > 2:
            mid = len(children) // 2
            children = [tree(children[:mid]), tree(children[mid:])]
        body = encoded(dict(children=children))
        location = dict(offset=len(pages), **pin(body))
        pages.extend(body)
        page_count += 1
        return dict(rows=sum(c["rows"] for c in children), prototype=[0.0] * 768,
                    target=dict(kind="directory", span=location))

    root_node = tree(nodes)
    dirs = save("directories.bin", bytes(pages))
    manifest = dict(schema="borsuk-hierarchical-cells-resident-v3", input=build, rows=100000, dimensions=768, seed=20260923,
                    mean=[0.0] * 768, low=[0.0] * 768, step=[1.0] * 768, root_directory=root_node["target"]["span"],
                    directory_bytes=len(pages), directory_sha256=dirs["sha256"], cell_bytes=byte,
                    build=dict(root_sha256="", cells=len(nodes), directories=page_count, max_cell_rows=510, max_depth=9,
                               geometry_fallbacks=0, canonical_row_reads=100000, modeled_build_payload_bytes=1, input_bytes=1, output_bytes=1))
    root_desc = save("manifest.json", manifest)
    query = [2.0] + [0.0] * 767
    requests = save("requests64", b"".join(encoded(dict(ordinal=i, query=query)) + b"\n" for i in range(64)))
    query_sha = pin(struct.pack("<768f", *query))["sha256"]
    limits = dict(max_source_gets=24, max_source_bytes=16 * MIB, max_query_payload_bytes=128 * MIB)
    nconfig = dict(schema=NATIVE, candidate_root={**root_desc, "path": "/closed-synthetic-host/manifest.json"},
                   requests={**requests, "path": "/closed-synthetic-host/requests64"}, first=0, count=64, limits=limits,
                   max_resident_directory_payload_bytes=128 * MIB, max_evaluator_payload_bytes=128 * MIB, max_result_bytes=128 * MIB)
    config_desc = save("nomination-config.json", nconfig)
    parsed = 4 * len(pages) + 256 * (len(nodes) + page_count) + 8 * 65536
    identity = dict(phase="identity", schema=NATIVE, config_sha256=config_desc["sha256"], candidate_root_sha256=root_desc["sha256"],
                    requests_sha256=requests["sha256"], module_source_sha256=source["module"]["sha256"],
                    binary_source_sha256=source["binary_source"]["sha256"], source_identity=build, source_identity_sha256=build_identity(build),
                    first=0, count=64, policies=POLICIES, limits=limits, truth_opened=False, record_scoring_performed=False,
                    scientific_qualification=False, quality_or_performance_claim=False, physical_s3_measured=False,
                    modeled_evaluator_payload_bytes=requests["bytes"] * 4 + 32 * MIB,
                    startup=stats(1, root_desc["bytes"]), startup_directory=stats(1, len(pages)),
                    candidate_descriptor_authentication_bytes=root_desc["bytes"], max_resident_directory_payload_bytes=128 * MIB,
                    directory_admission=dict(encoded_bytes=len(pages), modeled_parsed_payload_bytes=parsed,
                                             modeled_preload_peak_bytes=parsed + len(pages), parsed_owned_capacity_bytes=100))
    events = [identity]
    receipts = []
    for arm, policy in enumerate(POLICIES):
        selected, primary, covered = [], [], []
        for rank, cid in enumerate(range(arm * 8, arm * 8 + 24)):
            cell = nodes[cid]["target"]["cell"]
            ids = members[cid]
            selected.append(dict(cell_id=cid, distance=1.0, distance_bits=0x3f800000, primary=rank < 8,
                                 prototype_bits=[0] * 768, first_row=cell["first_row"], source_ids=ids,
                                 source_offset=cell["source"]["offset"], source_bytes=cell["source"]["bytes"],
                                 source_sha256=cell["source"]["sha256"], whole_cell_bytes=cell["whole"]["bytes"]))
            covered.extend(ids)
            if rank < 8:
                primary.extend(ids)
        size = 96 * 208
        a = {tier: stats(0, 0) for tier in ("directory", "whole_cell", "refinement")}
        a.update(source=stats(24, size), modeled_query_payload_bytes=payload(limits, build),
                 waves=[dict(stage="source", dependency=1, submitted_gets=24, requested_bytes=size, verified_bytes=size, max_parallel_gets=1)])
        count = len(nodes) if arm else 64 * 17 + 24
        receipts.append(dict(policy=policy, resident_leaf_cells=len(nodes), routing_distance_evaluations_bound=count,
                             routing_coordinate_evaluations_bound=count * 768, selected=selected,
                             primary_ids=sorted(primary), covered_ids=sorted(covered), accounting=a))
    for ordinal in range(64):
        for receipt in receipts:
            events.append(dict(phase="selection_frozen", ordinal=ordinal, query_sha256=query_sha, truth_opened=False, receipt=receipt))
    prefix_body = b"".join(encoded(e) + b"\n" for e in events)
    prefix = pin(prefix_body)
    marker = dict(phase="all_selections_frozen", schema=NATIVE, prefix_bytes=prefix["bytes"], prefix_sha256=prefix["sha256"],
                  first=0, count=64, selection_receipts=128, policies=POLICIES, truth_opened=False,
                  scope="one_dataset_parent_must_seal_both_before_offline_truth")
    marker.update({k: identity[k] for k in ("config_sha256", "candidate_root_sha256", "requests_sha256", "module_source_sha256", "binary_source_sha256", "source_identity_sha256")})
    terminal = dict(phase="terminal", status="NOMINATIONS_FROZEN", complete=True, queries=64, selection_receipts=128,
                    truth_opened=False, scientific_qualification=False, quality_or_performance_claim=False)
    body = prefix_body + encoded(marker) + b"\n" + encoded(terminal) + b"\n"
    nom_desc = save("nominations.jsonl", body)
    truth_rows, historical = [], []
    tiny = [sid for cid in range(32) for sid in members[cid]]
    outside = members[32]
    for i in range(64):
        gold = tiny[:4] + tiny[32:88+i] + tiny[96:128] if i < 4 else tiny[:96]
        gold += outside[:100-len(gold)]
        truth_rows.append(gold)
        historical.append(dict(ordinal=i, native_loss=dict(primary_hits=4 if i < 4 else 32),
                               coverage_ceiling_hits=60 + i if i < 4 else 96,
                               roster_sizes=dict(covered=96), local_reads=dict(requested_bytes=96 * 988)))
    truth_desc = save("truth.u32", struct.pack("<6400I", *(sid for row in truth_rows for sid in row)))
    protocol_desc = save("protocol.json", dict(split=SPLIT))
    audit_desc = save("audit.json", dict(audit_status="VALID", datasets={name: dict(per_query=historical) for name in DATASETS}))
    ds = dict(candidate_root=root_desc, directories=dirs, requests=requests, nomination_config=config_desc,
              nominations=nom_desc, truth=truth_desc, truth_id_space="logical_source_ordinal_le_u32")
    config = dict(schema=SCHEMA, protocol=protocol_desc, historical_audit=audit_desc, source=source,
                  datasets={name: copy.deepcopy(ds) for name in DATASETS})
    receipt = dict(schema=SCHEMA + "-closed-receipt", status="CLOSED_VALID", execution_exit_code=0,
                   source_qualified=True, resources_qualified=True, cleanup_complete=True, both_sealed_before_truth=True, truth_opened=False,
                   protocol=protocol_desc, historical_audit=audit_desc, source=source, datasets={})
    for name in DATASETS:
        entry = {k: ds[k] for k in ("nomination_config", "nominations", "candidate_root", "directories", "requests")}
        entry.update(prefix=prefix, source_identity_sha256=identity["source_identity_sha256"], execution_exit_code=0,
                     sealed_before_truth=True, fsynced=True, complete=True,
                     resources=dict(memory_limit_bytes=256 * MIB, memory_peak_bytes=MIB, swap_limit_bytes=0, swap_peak_bytes=0,
                                    cpu_count=1, oom=0, oom_kill=0))
        receipt["datasets"][name] = entry
    config["closed_receipt"] = save("closed.json", receipt)
    overrides = dict(PROTOCOL=content_pin(protocol_desc), AUDIT=content_pin(audit_desc),
                     ROOTS={name: root_desc["sha256"] for name in DATASETS}, REQUESTS={name: content_pin(requests) for name in DATASETS},
                     TRUTHS={name: truth_desc["sha256"] for name in DATASETS}, BASELINES={name: (6006, 63) for name in DATASETS})
    return config, overrides, body, receipt, save


def self_check():
    checks, opens = [], []
    production_pins = {name: copy.deepcopy(globals()[name]) for name in ("PROTOCOL", "AUDIT", "ROOTS", "REQUESTS", "TRUTHS", "BASELINES")}
    original_read = read_artifact
    with tempfile.TemporaryDirectory(prefix="sealed-coverage-synthetic-") as temporary:
        directory = Path(temporary)
        config, overrides, body, receipt, save = synthetic_fixture(directory)
        truth_paths = {config["datasets"][name]["truth"]["path"] for name in DATASETS}

        def tracked_read(desc, cap, retain=True):
            if desc["path"] in truth_paths:
                opens.append(desc["path"])
            return original_read(desc, cap, retain)

        def reject(label, mutation, before_truth=True):
            current = copy.deepcopy(config)
            mutation(current)
            opens.clear()
            try:
                evaluate(current, "f" * 64)
            except (ValueError, OSError, KeyError, TypeError, OverflowError, struct.error):
                require(not before_truth or not opens, "truth opened before rejection: " + label)
            else:
                raise AssertionError("accepted invalid fixture: " + label)
            checks.append(label)

        def replace_nominations(current, replacement, target="cohere"):
            desc = save("bad-nominations.jsonl", replacement)
            current["datasets"][target]["nominations"] = desc
            # Re-pin the root proof too: a semantic mutant must fail its own
            # validator, not merely an unrelated stale outer receipt binding.
            proof = copy.deepcopy(receipt)
            proof["datasets"][target].update(nominations=desc, prefix=pin(b"".join(replacement.splitlines(keepends=True)[:129])))
            current["closed_receipt"] = save("mutant-closed-proof.json", proof)

        def bad_event(index, mutation, reseal=True, target="cohere"):
            def change(current):
                lines = body.splitlines(keepends=True)
                event = parse(lines[index])
                mutation(event)
                lines[index] = encoded(event) + b"\n"
                if reseal and index < 129:
                    marker = parse(lines[129])
                    prefix = pin(b"".join(lines[:129]))
                    marker.update(prefix_bytes=prefix["bytes"], prefix_sha256=prefix["sha256"])
                    lines[129] = encoded(marker) + b"\n"
                replace_nominations(current, b"".join(lines), target)
            return change

        def bad_proof(mutation):
            def change(current):
                proof = copy.deepcopy(receipt)
                mutation(proof)
                current["closed_receipt"] = save("bad-proof.json", proof)
            return change

        try:
            globals().update(overrides)
            globals()["read_artifact"] = tracked_read
            first = evaluate(config, "f" * 64)
            require(len(opens) == 2, "exactly two truth reads after seals")
            require(first["datasets"]["relaion"]["policies"][POLICIES[0]]["fetched"] == dict(total_hits=6006, mean_coverage_fraction=6006/6400, p05_hits=63), "known hierarchy intersections/p05")
            require(first["datasets"]["cohere"]["policies"][POLICIES[1]]["fetched"] == dict(total_hits=4198, mean_coverage_fraction=4198/6400, p05_hits=64), "known global intersections/p05")
            paired = first["datasets"]["relaion"]["paired_per_query"]
            require(paired[0]["fetched_hits_delta_global_minus_hierarchy"] == 28
                    and paired[-1]["fetched_hits_delta_global_minus_hierarchy"] == -32, "known paired deltas")
            for rows in (d["policies"][POLICIES[0]]["per_query"] for d in first["datasets"].values()):
                require(rows[0]["actual_source_bytes"] == 19968 and rows[0]["modeled_whole_cell_bytes"] == 94848, "SOURCE versus WholeCell bytes")
                require(rows[0]["primary_hits"] == 4 and rows[-1]["primary_hits"] == 32, "known primary intersections")
            second = evaluate(config, "f" * 64)
            require(encoded(first) == encoded(second), "deterministic repeated result")
            a, b = directory / "result-a.json", directory / "result-b.json"
            require(write_new(a, first) == write_new(b, second), "repeat output pin")
            config_desc = save("reducer-config.json", config)
            cli_output = directory / "cli-result.json"
            run(config_desc["path"], config_desc["sha256"], cli_output)
            expected_cli = copy.deepcopy(first)
            expected_cli["config_sha256"] = config_desc["sha256"]
            require(cli_output.read_bytes() == encoded(expected_cli) + b"\n", "CONFIG CONFIG_SHA NEW_OUTPUT path")
            opens.clear()
            try:
                run(config_desc["path"], "e" * 64, directory / "must-not-exist.json")
            except ValueError:
                require(not opens and not (directory / "must-not-exist.json").exists(), "bad CONFIG hash before GT/no output")
            else:
                raise AssertionError("accepted wrong CONFIG hash")
            opens.clear()
            try:
                run(config_desc["path"], config_desc["sha256"], a)
            except ValueError:
                require(not opens and a.read_bytes() == b.read_bytes(), "no overwrite before GT")
            else:
                raise AssertionError("overwrote output")
            checks.extend(["known primary/fetched intersections", "known paired deltas", "SOURCE versus WholeCell bytes",
                           "fourth-lowest p05", "deterministic repeated output", "no overwrite before GT", "noncontiguous source IDs; no physical map",
                           "CONFIG CONFIG_SHA NEW_OUTPUT", "bad CONFIG SHA before GT/no output", "relocated exact sealed artifacts"])
            for name in DATASETS:
                reject("missing " + name, lambda c, n=name: c["datasets"].pop(n))
                reject("wrong original root " + name, lambda c, n=name: c["datasets"][n]["candidate_root"].update(sha256="d" * 64))
                reject("nomination body tamper " + name, lambda c, n=name: c["datasets"][n]["nominations"].update(sha256="d" * 64))
                reject("missing marker " + name, lambda c, n=name: replace_nominations(c, b"".join(body.splitlines(keepends=True)[:129] + body.splitlines(keepends=True)[130:]), n))
                reject("marker digest " + name, bad_event(129, lambda e: e.update(prefix_sha256="d" * 64), target=name))
                reject("missing receipt dataset " + name, bad_proof(lambda p, n=name: p["datasets"].pop(n)))
                reject("unsealed receipt " + name, bad_proof(lambda p, n=name: p["datasets"][n].update(sealed_before_truth=False)))
            for field in ("execution_exit_code", "source_qualified", "resources_qualified", "cleanup_complete", "both_sealed_before_truth"):
                reject("missing proof " + field, bad_proof(lambda p, f=field: p.pop(f)))
                reject("failed proof " + field, bad_proof(lambda p, f=field: p.update({f: 1 if f == "execution_exit_code" else False})))
            reject("missing closed receipt", lambda c: c.pop("closed_receipt"))
            reject("receipt body tamper", lambda c: c["closed_receipt"].update(sha256="d" * 64))
            reject("proof wrong nomination binding", bad_proof(lambda p: p["datasets"]["cohere"]["nominations"].update(sha256="e" * 64)))
            reject("proof swap", bad_proof(lambda p: p["datasets"]["cohere"]["resources"].update(swap_peak_bytes=1)))
            reject("proof memory peak", bad_proof(lambda p: p["datasets"]["cohere"]["resources"].update(memory_peak_bytes=512*MIB)))
            reject("proof CPU", bad_proof(lambda p: p["datasets"]["cohere"]["resources"].update(cpu_count=2)))
            reject("proof nonzero per-dataset execution", bad_proof(lambda p: p["datasets"]["cohere"].update(execution_exit_code=2)))
            reject("source body tamper", lambda c: c["source"]["module"].update(sha256="d" * 64))
            reject("source revision", lambda c: c["source"].update(revision="abbreviated"))
            reject("pretruth truth-path alias", lambda c: c["source"].update(module=c["datasets"]["relaion"]["truth"].copy()))
            hardlink = directory / "truth-hardlink"
            os.link(config["datasets"]["relaion"]["truth"]["path"], hardlink)
            reject("pretruth truth-inode alias", lambda c: c["source"]["module"].update(path=str(hardlink)))
            symlink, fifo = directory / "source-symlink", directory / "source-fifo"
            symlink.symlink_to(config["source"]["module"]["path"])
            os.mkfifo(fifo)
            reject("symlink source rejected", lambda c: c["source"]["module"].update(path=str(symlink)))
            reject("FIFO source rejected without blocking", lambda c: c["source"]["module"].update(path=str(fifo)))
            reject("truth ID space", lambda c: c["datasets"]["cohere"].update(truth_id_space="physical"))
            reject("truth descriptor", lambda c: c["datasets"]["cohere"]["truth"].update(bytes=25604))
            reject("original requests", lambda c: c["datasets"]["cohere"]["requests"].update(sha256="e" * 64))
            reject("ordinal gap", bad_event(127, lambda e: e.update(ordinal=64)))
            reject("boolean ordinal", bad_event(1, lambda e: e.update(ordinal=False)))
            reject("policy swap", bad_event(2, lambda e: e["receipt"].update(policy=POLICIES[0])))
            reject("unequal original query hashes", bad_event(2, lambda e: e.update(query_sha256="e" * 64)))
            reject("unresealed prefix", bad_event(1, lambda e: e.update(query_sha256="e" * 64), reseal=False))
            reject("marker byte count", bad_event(129, lambda e: e.update(prefix_bytes=e["prefix_bytes"]+1)))
            reject("marker source binding", bad_event(129, lambda e: e.update(source_identity_sha256="e"*64)))
            reject("incomplete terminal", bad_event(130, lambda e: e.update(complete=False)))
            reject("truth-open terminal", bad_event(130, lambda e: e.update(truth_opened=True)))
            reject("event after terminal", lambda c: replace_nominations(c, body+b"{}\n"))
            reject("missing final newline", lambda c: replace_nominations(c, body[:-1]))
            reject("unknown event field", bad_event(1, lambda e: e.update(truth=[])))
            reject("underfilled24", bad_event(1, lambda e: e["receipt"]["selected"].pop()))
            reject("duplicate cell", bad_event(1, lambda e: e["receipt"]["selected"][1].update(cell_id=0)))
            reject("primary7", bad_event(1, lambda e: e["receipt"]["selected"][0].update(primary=False)))
            reject("global primary not first8", bad_event(2, lambda e: e["receipt"]["selected"][0].update(primary=False)))
            reject("cell tie order", bad_event(1, lambda e: e["receipt"]["selected"].reverse()))
            reject("distance bits mismatch", bad_event(1, lambda e: e["receipt"]["selected"][0].update(distance_bits=0)))
            reject("infinite distance", bad_event(1, lambda e: e["receipt"]["selected"][0].update(distance_bits=0x7f800000)))
            reject("prototype tamper", bad_event(1, lambda e: e["receipt"]["selected"][0]["prototype_bits"].__setitem__(0, 1)))
            reject("first_row is not source ordinal", bad_event(1, lambda e: e["receipt"]["selected"][0].update(first_row=123)))
            reject("source range tamper", bad_event(1, lambda e: e["receipt"]["selected"][0].update(source_offset=1)))
            reject("source SHA tamper", bad_event(1, lambda e: e["receipt"]["selected"][0].update(source_sha256="e"*64)))
            reject("source population", bad_event(1, lambda e: e["receipt"]["selected"][0]["source_ids"].pop()))
            reject("source-ID out of range", bad_event(1, lambda e: e["receipt"]["selected"][0]["source_ids"].__setitem__(0, 100000)))
            reject("duplicate source ID", bad_event(1, lambda e: e["receipt"]["selected"][0]["source_ids"].__setitem__(1, 123)))
            reject("cell membership changed", bad_event(3, lambda e: e["receipt"]["selected"][0]["source_ids"].reverse()))
            reject("covered roster tamper", bad_event(1, lambda e: e["receipt"]["covered_ids"].pop()))
            reject("source accounting tamper", bad_event(1, lambda e: e["receipt"]["accounting"]["source"].update(requested_bytes=1)))
            reject("unqualified whole-cell read", bad_event(1, lambda e: e["receipt"]["accounting"]["whole_cell"].update(submitted_gets=1)))
            reject("routing accounting", bad_event(2, lambda e: e["receipt"].update(routing_distance_evaluations_bound=24)))
            reject("directory body tamper", lambda c: c["datasets"]["cohere"]["directories"].update(sha256="e"*64))
            for label, payload_body in (("duplicate JSON field", b'{"schema":1,"schema":2}'), ("nonfinite JSON", b'{"distance":NaN}')):
                try:
                    parse(payload_body)
                except ValueError:
                    checks.append(label)
                else:
                    raise AssertionError(label)
            # Malformed truth must also fail, but only AFTER the seal boundary.
            original_truth = Path(config["datasets"]["relaion"]["truth"]["path"]).read_bytes()
            for label, values in (("duplicate truth", [0]*6400), ("truth out of range", [100000]+list(range(1,6400)))):
                malformed = struct.pack("<6400I", *values)
                desc = save("bad-truth.u32", malformed)
                truth_paths.add(desc["path"])
                TRUTHS["cohere"] = desc["sha256"]
                reject(label, lambda c, d=desc: c["datasets"]["cohere"].update(truth=d), before_truth=False)
                require(len(opens) == 2, "truth validation occurs after seals")
                TRUTHS["cohere"] = pin(original_truth)["sha256"]
            # Direct decision boundary cases use independently stipulated counts.
            def decision_case(total, p05, control_equal=False):
                data = {n: dict(policies={POLICIES[0]: dict(fetched=dict(total_hits=6006, p05_hits=63)),
                                          POLICIES[1]: dict(fetched=dict(total_hits=6006 if control_equal else total, p05_hits=p05))}) for n in DATASETS}
                return decisions(data)
            require(decision_case(6272, 95)["adequate_consumed_coverage"], "inclusive98/95")
            require(not decision_case(6271, 95)["adequate_consumed_coverage"] and not decision_case(6272, 94)["adequate_consumed_coverage"], "both adequate thresholds")
            require(not decision_case(6272, 95, True)["supports_pruning_contribution"], "strict mean improvement")
            require(not decision_case(6272, 62)["supports_pruning_contribution"] and decision_case(6272, 63)["supports_pruning_contribution"], "p05 nondecrease")
            checks.extend(["inclusive98/95 decision", "strict mean improvement", "p05 nondecrease decision"])
        finally:
            globals().update(production_pins)
            globals()["read_artifact"] = original_read
    return dict(status="PASS", synthetic_only=True, checks=len(checks), passed=checks,
                source_sha256=pin(Path(__file__).read_bytes())["sha256"])


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("config", nargs="?")
    parser.add_argument("config_sha256", nargs="?")
    parser.add_argument("new_output", nargs="?")
    parser.add_argument("--self-check", action="store_true")
    args = parser.parse_args()
    try:
        if args.self_check:
            require(args.config is args.config_sha256 is args.new_output is None, "--self-check takes no paths")
            result = self_check()
        else:
            require(all((args.config, args.config_sha256, args.new_output)), "CONFIG CONFIG_SHA256 NEW_OUTPUT required")
            result = dict(status="VALID", output=run(args.config, args.config_sha256, args.new_output))
        print(encoded(result).decode())
    except (ValueError, OSError, KeyError, TypeError, OverflowError, struct.error, RecursionError) as error:
        print("INVALID: " + str(error), file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main())
