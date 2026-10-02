#!/usr/bin/env python3
"""Current BORSUSR2 coverage ceilings, with nomination frozen before truth.

--self-check uses synthetic fixtures only. Otherwise supply --mode nominate|reduce,
--config-file, --config-sha256 and --output-file. Config schemas and exact fields
are validated below. No corpus, scoring, network, or native walk is performed.
"""

import argparse
from array import array
from contextlib import contextmanager
import math
import os
from pathlib import Path
import platform
import stat
import struct
import sys
import tempfile

# Only format-independent validation/math primitives, never the archival reader.
from check_semantic_router_coverage import (
    canonical, cosine_query, decode, digest, f32, fields, integer, require, sha,
)

MIB = 1024 * 1024
ROOT_CAP = 4 * MIB
JSON_CAP = 4 * MIB
REPORT_CAP = 16 * MIB
POLICIES = ("existing_first8_boundary1.15_max16", "fixed_top32_same_squared_distance_ranking")
NOMINATE_SCHEMA = "borsuk-semantic-binary-coverage-nominate-config-v1"
REDUCE_SCHEMA = "borsuk-semantic-binary-coverage-reduce-config-v1"
FREEZE_SCHEMA = "borsuk-semantic-binary-coverage-nomination-v1"
RESULT_SCHEMA = "borsuk-semantic-binary-coverage-result-v1"
SOURCE_FIELDS = "profile schema root_sha256 centroids_sha256 rows dimensions"
PIN_FIELDS = "path bytes sha256"
NOMINATE_FIELDS = "schema router_root membership leaves order requests panel protocol source provenance"
REDUCE_FIELDS = "schema nomination requests panel protocol truth truth_id_space order"
RECORD_FIELDS = ("selected_leaf_ids units page_closure selected_leaf_bytes selected_source_rows "
                 "page_closure_rows seed_page seed_additions prospective_walk_units "
                 "within_current_walk_guard")
FREEZE_FIELDS = ("schema phase truth_opened profile rows dimensions unit_rows page_rows count policies "
                 "claims config inputs provenance code_identity panel_selected_sha256 root_bytes "
                 "leaf_count membership_bytes authentication modeled_construction_allocation_bytes "
                 "modeled_allocation_is_not_rss records")
CLAIMS = dict(coverage_only=True, returned_recall_measured=False,
              cold_http_measured=False, physical_s3_query_gets_measured=False,
              source_sq8_admission_qualified=False, complete_historical_coverage=False,
              current_production_guard_not_changed=True,
              current_production_walk_guard=1031, prospective_initial_walk_units_max=2055)


def pin(value, cap):
    fields(value, PIN_FIELDS, "artifact pointer")
    require(type(value["path"]) is str and value["path"] and "\0" not in value["path"],
            "artifact path")
    integer(value["bytes"], 1, cap, "artifact bytes")
    digest(value["sha256"])
    return value


def directory_fd(path):
    """Walk components without following symlinks, including ancestor directories."""
    path = Path(path)
    require(".." not in path.parts, "parent traversal")
    fd = os.open("/" if path.is_absolute() else ".", os.O_RDONLY | os.O_DIRECTORY)
    try:
        for part in path.parts:
            if part in ("/", "."):
                continue
            child = os.open(part, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW,
                            dir_fd=fd)
            os.close(fd)
            fd = child
        return fd
    except BaseException:
        os.close(fd)
        raise


@contextmanager
def regular(path, size=None, cap=REPORT_CAP, immutable=False):
    path = Path(path)
    require(path.name not in ("", ".", ".."), "file path")
    parent = directory_fd(path.parent)
    try:
        fd = os.open(path.name, os.O_RDONLY | os.O_NONBLOCK | os.O_NOFOLLOW, dir_fd=parent)
    finally:
        os.close(parent)
    with os.fdopen(fd, "rb", buffering=0) as stream:
        before = os.fstat(stream.fileno())
        require(stat.S_ISREG(before.st_mode) and 0 < before.st_size <= cap,
                "regular file type/byte cap")
        require(size is None or before.st_size == size, "artifact length")
        require(not immutable or before.st_mode & 0o222 == 0, "nomination must be read-only")
        yield stream
        after = os.fstat(stream.fileno())
        require((before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns,
                 before.st_ctime_ns) ==
                (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns,
                 after.st_ctime_ns), "artifact changed during read")


def authenticated(identity, cap, immutable=False):
    pin(identity, cap)
    with regular(identity["path"], identity["bytes"], cap, immutable) as stream:
        body = stream.read(identity["bytes"] + 1)
    require(len(body) == identity["bytes"] and sha(body) == identity["sha256"],
            "artifact length/SHA256 mismatch")
    return body


def publish(path, value):
    """Publish a fully synced read-only report atomically, refusing every overwrite."""
    body = canonical(value)
    require(len(body) <= REPORT_CAP, "output byte cap")
    path = Path(path)
    parent = directory_fd(path.parent)
    temporary = ".coverage-" + os.urandom(12).hex()
    try:
        fd = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW,
                     0o600, dir_fd=parent)
        with os.fdopen(fd, "wb") as stream:
            stream.write(body)
            stream.flush()
            os.fchmod(stream.fileno(), 0o444)
            os.fsync(stream.fileno())
        os.link(temporary, path.name, src_dir_fd=parent, dst_dir_fd=parent,
                follow_symlinks=False)
        os.fsync(parent)
    finally:
        try:
            os.unlink(temporary, dir_fd=parent)
        except FileNotFoundError:
            pass
        os.close(parent)
    return dict(path=str(path), bytes=len(body), sha256=sha(body))


def allocation_estimate(rows, d):
    """Exact 64-bit Rust admit() accounting, including Vec<f32> size 24."""
    u = (rows + 31) // 32
    c = (u + 63) // 64
    l = 2 * c - 1
    b = 32 + u * d * 2
    f = u * d * 4 + u * 4
    v = u * d * 4 + u * (24 + 64)
    i = c * (d * 4 + 128)
    h = (c - 1 + 30) // 31
    r = h * (32 * d * 16 + u * 64 + 32 * 128)
    p = u * (d * 2 + 4)
    w = 512 + l * (64 + d * 4)
    prototypes = 3 * l * (d * 4 + 256)
    common = 2 * 64 * 1024 + b + f
    return max(common + 2 * v + r + 2 * i + 8 * MIB,
               common + v + i + 64 * u + p + 4 * u + prototypes + 12 * MIB,
               common + 2 * p + 16 * u + 3 * w + prototypes + 8 * MIB)


def source_identity(source, production=True):
    fields(source, SOURCE_FIELDS, "source identity")
    require(type(source["schema"]) is str and 1 <= len(source["schema"].encode()) <= 256,
            "source schema")
    digest(source["root_sha256"])
    digest(source["centroids_sha256"])
    if production:
        require(source["profile"] == "fresh1m" and type(source["rows"]) is int and
                source["rows"] == 1000000 and type(source["dimensions"]) is int and
                source["dimensions"] == 768, "production requires Fresh1m/1000000/768")
    else:
        # Only the private synthetic self-check path admits native partial tails.
        require(source["profile"] == "native100k", "synthetic native profile")
        integer(source["rows"], 1, 100000, "native rows")
        integer(source["dimensions"], 1, 768, "native dimensions")


def parse_root(body, source):
    source_identity(source)
    return _parse_root(body, source)


def _parse_root(body, source):
    rows, d = source["rows"], source["dimensions"]
    code = 2 if source["profile"] == "fresh1m" else 1
    cap = ROOT_CAP if code == 2 else MIB
    require(512 <= len(body) <= cap and body[:8] == b"BORSUSR2", "BORSUSR2 header/version/cap")
    u32 = lambda offset: struct.unpack_from("<I", body, offset)[0]
    u64 = lambda offset: struct.unpack_from("<Q", body, offset)[0]
    u = (rows + 31) // 32
    requested = (u + 63) // 64
    leaves, training, schema_len = u32(32), u32(40), u32(44)
    require(u32(8) == 512 and u32(12) == code and u64(16) == rows and
            u32(24) == d and u32(28) == u and u32(36) == requested and
            training in (1, requested) and 1 <= leaves <= 2 * requested - 1 and
            1 <= schema_len <= 256 and not any(body[88:128]) and
            not any(body[256 + schema_len:512]), "binary header geometry/reserved")
    require(body[256:256 + schema_len].decode("utf-8") == source["schema"],
            "binary source schema")
    require(body[128:160].hex() == source["root_sha256"] and
            body[160:192].hex() == source["centroids_sha256"], "binary source digests")
    estimate = allocation_estimate(rows, d)
    limit = u64(80)
    require(estimate <= limit <= (512 if code == 2 else 128) * MIB and
            u64(72) == estimate, "binary exact allocation accounting")
    record_bytes = 4 + d * 2
    payload_bytes = u * record_bytes
    plane = 512 + leaves * 64
    require(len(body) == plane + leaves * d * 4 and u64(48) == 32 + u * d * 2 and
            u64(56) == u * 4 and u64(64) == payload_bytes, "binary exact lengths")
    directory = []
    end = total_units = total_rows = 0
    previous = None
    for leaf_id in range(leaves):
        start = 512 + leaf_id * 64
        group, chunk = u32(start), u32(start + 4)
        size, count, count_rows = u32(start + 16), u32(start + 20), u32(start + 24)
        ordered = (chunk == 0 if previous is None else
                   chunk == previous[1] + 1 and previous[2] == 64
                   if previous[0] == group else group > previous[0] and chunk == 0)
        require(group < training and ordered and 1 <= count <= 64 and
                u64(start + 8) == end and size == count * record_bytes and
                1 <= count_rows <= count * 32 and not any(body[start + 28:start + 32]),
                "binary directory geometry/reserved/order")
        prototype = struct.unpack_from("<" + str(d) + "f", body, plane + leaf_id * d * 4)
        require(all(math.isfinite(v) for v in prototype), "nonfinite root prototype")
        directory.append(dict(leaf_id=leaf_id, group=group, chunk=chunk, offset=end,
                              bytes=size, unit_count=count, source_rows=count_rows,
                              sha256=body[start + 32:start + 64].hex(), prototype=prototype))
        end += size
        total_units += count
        total_rows += count_rows
        previous = group, chunk, count
    require(end == payload_bytes and total_units == u and total_rows == rows,
            "binary complete directory")
    return dict(rows=rows, dimensions=d, unit_count=u, leaves=directory,
                root_bytes=len(body), membership_bytes=u * 4,
                membership_sha256=body[192:224].hex(), leaves_bytes=payload_bytes,
                leaves_sha256=body[224:256].hex(), modeled_peak_allocation_bytes=estimate,
                modeled_allocation_limit_bytes=limit)


def validate_membership(body, root):
    require(len(body) == root["membership_bytes"] and sha(body) == root["membership_sha256"],
            "membership length/SHA256")
    membership = array("I")
    membership.frombytes(body)
    require(membership.itemsize == 4, "u32 platform width")
    if sys.byteorder != "little":
        membership.byteswap()
    counts = [0] * len(root["leaves"])
    rows = counts.copy()
    for unit, leaf in enumerate(membership):
        require(leaf < len(counts), "membership leaf ID")
        counts[leaf] += 1
        rows[leaf] += min(32, root["rows"] - unit * 32)
    for leaf in root["leaves"]:
        i = leaf["leaf_id"]
        require(counts[i] == leaf["unit_count"] and rows[i] == leaf["source_rows"],
                "membership partition counts/rows")
    return membership


def ranking(query, leaves):
    ranked = []
    for leaf in leaves:
        distance = 0.0
        for q, p in zip(query, leaf["prototype"]):
            delta = q - p
            distance += delta * delta
        require(math.isfinite(distance), "nonfinite root distance")
        ranked.append((distance, leaf["leaf_id"]))
    ranked.sort()
    return ranked


def selections(ranked):
    count = min(8, len(ranked))
    if count == 8:
        for distance, _ in ranked[8:16]:
            if distance > 1.15 * ranked[7][0]:
                break
            count += 1
    return ([i for _, i in ranked[:count]], [i for _, i in ranked[:32]])


def leaf_units(stream, leaf, root, membership):
    stream.seek(leaf["offset"])
    body = stream.read(leaf["bytes"])
    require(len(body) == leaf["bytes"] and sha(body) == leaf["sha256"], "selected leaf SHA256/length")
    d = root["dimensions"]
    width = 4 + d * 2
    units = set()
    count_rows = 0
    sums = [0.0] * d
    for start in range(0, len(body), width):
        unit = struct.unpack_from("<I", body, start)[0]
        require(unit < len(membership) and membership[unit] == leaf["leaf_id"] and
                unit not in units, "selected leaf disjoint membership partition")
        units.add(unit)
        count_rows += min(32, root["rows"] - unit * 32)
        vector = struct.unpack_from("<" + str(d) + "e", body, start + 4)
        require(all(math.isfinite(v) for v in vector), "nonfinite selected FP16")
        for coordinate, value in enumerate(vector):
            sums[coordinate] += value
    require(len(units) == leaf["unit_count"] and count_rows == leaf["source_rows"],
            "selected leaf counts/rows")
    expected = tuple(f32(s / len(units)) for s in sums)
    require(struct.pack("<" + str(d) + "f", *expected) ==
            struct.pack("<" + str(d) + "f", *leaf["prototype"]), "selected leaf prototype identity")
    return units


def nomination_record(ids, stream, root, membership, validated=None):
    require(1 <= len(ids) <= 32 and len(ids) == len(set(ids)) and
            all(type(i) is int and 0 <= i < len(root["leaves"]) for i in ids), "selected leaf IDs")
    payload = sum(root["leaves"][i]["bytes"] for i in ids)
    require(payload <= ROOT_CAP, "selected payload cap")
    units = set()
    for i in ids:
        selected = (leaf_units(stream, root["leaves"][i], root, membership)
                    if validated is None else validated[i])
        require(not units.intersection(selected), "disjoint selected units")
        units.update(selected)
    pages = {u // 8 for u in units}
    require(1 <= len(units) <= 2048 and 1 <= len(pages) <= 2048, "unit/page admission")
    seed = min(pages)
    additions = sorted(set(range(seed * 8, min((seed + 1) * 8, root["unit_count"]))) - units)
    walk = len(units) + len(additions)
    require(walk <= 2055, "prospective walk ceiling")
    return dict(selected_leaf_ids=ids, units=sorted(units), page_closure=sorted(pages),
                selected_leaf_bytes=payload,
                selected_source_rows=sum(min(32, root["rows"] - 32 * u) for u in units),
                page_closure_rows=sum(min(256, root["rows"] - 256 * p) for p in pages),
                seed_page=seed, seed_additions=additions, prospective_walk_units=walk,
                within_current_walk_guard=walk <= 1031)


def panel_identity(identity):
    panel_body = authenticated(identity, JSON_CAP)
    panel = decode(panel_body)
    require(type(panel) is dict and type(panel.get("selected")) is list and
            len(panel["selected"]) == 64, "fixed64 panel")
    selected = panel["selected"]
    # Selector/preparation value_sha hashes compact canonical JSON without LF.
    require(panel.get("selected_sha256") == sha(canonical(selected)[:-1]), "panel selected SHA256")
    source_ids = []
    for ordinal, entry in enumerate(selected):
        require(type(entry) is dict and type(entry.get("query_ordinal")) is int and
                entry["query_ordinal"] == ordinal, "panel query order")
        source_ids.append(integer(entry.get("source_ordinal"), 1000000, 9999999,
                                  "panel source ordinal"))
    require(len(set(source_ids)) == 64, "duplicate panel source ordinal")
    return source_ids, panel


def load_panel(config, dimensions):
    source_ids, panel = panel_identity(config["panel"])
    requests = authenticated(config["requests"], JSON_CAP)
    lines = requests.splitlines()
    require(len(lines) == 64, "expected exactly64 request lines")
    queries = []
    for ordinal, line in enumerate(lines):
        request = decode(line)
        fields(request, "ordinal query", "request")
        require(type(request["ordinal"]) is int and request["ordinal"] == ordinal, "request order")
        require(type(request["query"]) is list and len(request["query"]) == dimensions,
                "query dimensions")
        queries.append(cosine_query(request["query"]))
    return queries, source_ids, panel


def load_protocol(identity):
    protocol = decode(authenticated(identity, JSON_CAP))
    require(type(protocol) is dict and protocol.get("schema") ==
            "borsuk-cohere-top32-fresh-coverage-prospective-v1", "prospective protocol schema")
    expected = dict(rows=1000000, dimensions=768, count=64, truth_k=100,
                    coverage_only=True, nomination_freeze_before_truth=True,
                    current_production_walk_guard=1031, prospective_initial_walk_units_max=2055,
                    selected_leaf_payload_bytes_max=ROOT_CAP, nominated_units_max=2048,
                    page_closure_pages_max=2048, current_production_guard_not_changed=True)
    require(all(type(protocol.get(k)) is type(v) and protocol[k] == v for k, v in expected.items()),
            "prospective protocol frozen bounds")
    require(protocol.get("comparison_policies") == list(POLICIES) and
            protocol.get("threshold") == dict(denominator10=640, hits10_minimum=608,
                                               mean_page_closure_recall_at_10_minimum=0.95),
            "prospective policies/threshold")
    return protocol


def provenance(value):
    fields(value, "source_commit source_archive_sha256 builder_commit builder_binary_sha256 "
           "source_identity resource_metadata", "provenance")
    for key in ("source_commit", "builder_commit"):
        require(type(value[key]) is str and len(value[key]) == 40 and
                all(c in "0123456789abcdef" for c in value[key]), "producer commit")
    for key in ("source_archive_sha256", "builder_binary_sha256"):
        digest(value[key])
    for key in ("source_identity", "resource_metadata"):
        decode(authenticated(value[key], JSON_CAP))
    return value


def code_identity():
    script = Path(__file__).absolute()
    files = [script, script.with_name("check_semantic_router_coverage.py"),
             script.parent.parent / "crates/borsuk/src/semantic_unit_router.rs",
             script.parent.parent / "crates/borsuk/src/sq8_source.rs"]
    identities = {}
    for path in files:
        with regular(path, cap=JSON_CAP) as stream:
            body = stream.read(JSON_CAP + 1)
        require(len(body) <= JSON_CAP, "code cap")
        identities[path.name] = dict(bytes=len(body), sha256=sha(body))
    return dict(files=identities, python=platform.python_version(),
                implementation=platform.python_implementation(),
                normalization="input FP32; ordered f64 squared norm; tolerance1e-6; f64 division to f32",
                ranking="ordered f64 squared Euclidean distance, then leaf ID")


def nominate(config, config_identity):
    fields(config, NOMINATE_FIELDS, "nominate config (truth prohibited)")
    require(config["schema"] == NOMINATE_SCHEMA, "nominate config schema")
    source_identity(config["source"])
    load_protocol(config["protocol"])
    provenance(config["provenance"])
    queries, source_ids, panel = load_panel(config, 768)
    root = parse_root(authenticated(config["router_root"], ROOT_CAP), config["source"])
    return _nominate(config, config_identity, root, queries, source_ids, panel)


def _nominate(config, config_identity, root, queries, source_ids, panel):
    """Shared core; production geometry was admitted before this private fixture seam."""
    membership = validate_membership(authenticated(config["membership"], ROOT_CAP), root)
    require(config["membership"]["sha256"] == root["membership_sha256"] and
            config["leaves"]["sha256"] == root["leaves_sha256"], "router body identity binding")
    pin(config["leaves"], root["leaves_bytes"])
    require(config["leaves"]["bytes"] == root["leaves_bytes"], "leaves exact length")
    pin(config["order"], root["rows"] * 8)
    require(config["order"]["bytes"] == root["rows"] * 8, "order exact length")
    records = []
    with regular(config["leaves"]["path"], root["leaves_bytes"], root["leaves_bytes"]) as stream:
        for ordinal, query in enumerate(queries):
            chosen = selections(ranking(query, root["leaves"]))
            # Existing leaves are a prefix of top32: authenticate their union once.
            require(sum(root["leaves"][i]["bytes"] for i in chosen[1]) <= ROOT_CAP,
                    "per-query distinct selected payload cap")
            validated = {i: leaf_units(stream, root["leaves"][i], root, membership)
                         for i in chosen[1]}
            policies = {name: nomination_record(ids, stream, root, membership, validated)
                        for name, ids in zip(POLICIES, chosen)}
            records.append(dict(query_ordinal=ordinal, source_ordinal=source_ids[ordinal],
                                policies=policies))
    require(len(records) == 64, "freeze all64 before truth")
    return dict(schema=FREEZE_SCHEMA, phase="nomination_frozen_before_truth", truth_opened=False,
                profile=config["source"]["profile"], rows=root["rows"], dimensions=root["dimensions"],
                unit_rows=32, page_rows=256, count=64, policies=list(POLICIES),
                claims=CLAIMS.copy(), config=config_identity, inputs=config,
                provenance=config["provenance"], code_identity=code_identity(),
                panel_selected_sha256=panel["selected_sha256"],
                root_bytes=root["root_bytes"], leaf_count=len(root["leaves"]),
                membership_bytes=root["membership_bytes"],
                authentication=dict(root="whole SHA256", membership="whole SHA256 and complete partition",
                                    leaves="exact file size; selected range SHA256/FP16/prototype/partition; unselected bytes unopened",
                                    order="pointer frozen; whole SHA256 and bijection checked in reduction"),
                modeled_construction_allocation_bytes=root["modeled_peak_allocation_bytes"],
                modeled_allocation_is_not_rss=True, records=records)


def load_order(identity, rows):
    require(identity["bytes"] == rows * 8, "order exact length")
    body = authenticated(identity, rows * 8)
    inverse = array("i", [-1]) * rows
    require(inverse.itemsize == 4, "i32 platform width")
    for physical, (logical,) in enumerate(struct.iter_unpack("<Q", body)):
        require(logical < rows and inverse[logical] == -1, "order must be a complete bijection")
        inverse[logical] = physical
    return inverse


def frozen_record(record, rows, leaf_count, maximum, dimensions):
    fields(record, RECORD_FIELDS, "frozen policy record")
    ids = record["selected_leaf_ids"]
    require(type(ids) is list and 1 <= len(ids) <= maximum and
            all(type(i) is int and 0 <= i < leaf_count for i in ids) and
            len(set(ids)) == len(ids), "frozen leaf IDs")
    units, pages = record["units"], record["page_closure"]
    require(type(units) is list and 1 <= len(units) <= maximum * 64 and
            all(type(i) is int and 0 <= i < (rows + 31) // 32 for i in units) and
            units == sorted(set(units)) and len(ids) <= len(units) <= len(ids) * 64,
            "frozen unit partition")
    require(type(pages) is list and all(type(i) is int for i in pages) and
            pages == sorted({u // 8 for u in units}) and len(pages) <= 2048,
            "frozen page closure")
    seed = pages[0]
    additions = sorted(set(range(seed * 8, min((seed + 1) * 8, (rows + 31) // 32))) - set(units))
    expected = dict(seed_page=seed, seed_additions=additions,
                    prospective_walk_units=len(units) + len(additions),
                    within_current_walk_guard=len(units) + len(additions) <= 1031,
                    selected_source_rows=sum(min(32, rows - u * 32) for u in units),
                    page_closure_rows=sum(min(256, rows - p * 256) for p in pages))
    require(all(type(record[k]) is type(v) and record[k] == v for k, v in expected.items()),
            "frozen counts/seed/guard")
    integer(record["selected_leaf_bytes"], 1, ROOT_CAP, "frozen selected payload")
    require(record["selected_leaf_bytes"] == len(units) * (4 + dimensions * 2),
            "frozen exact selected payload accounting")


def reduce(config, config_identity):
    fields(config, REDUCE_FIELDS, "reduce config")
    require(config["schema"] == REDUCE_SCHEMA and config["truth_id_space"] == "source_ordinal",
            "truth must be LEi64 FIRST1M source ordinals")
    nomination_body = authenticated(config["nomination"], REPORT_CAP, immutable=True)
    frozen = decode(nomination_body)
    require(canonical(frozen) == nomination_body, "canonical immutable nomination")
    fields(frozen, FREEZE_FIELDS, "frozen nomination")
    require(type(frozen) is dict and frozen.get("schema") == FREEZE_SCHEMA and
            frozen.get("phase") == "nomination_frozen_before_truth" and
            frozen.get("truth_opened") is False and frozen.get("profile") == "fresh1m" and
            type(frozen.get("rows")) is int and frozen["rows"] == 1000000 and
            type(frozen.get("dimensions")) is int and frozen["dimensions"] == 768,
            "production frozen nomination identity/geometry")
    source_identity(frozen["inputs"]["source"])
    require(frozen["provenance"] == frozen["inputs"]["provenance"], "frozen provenance binding")
    return _reduce(config, config_identity, frozen)


def _reduce(config, config_identity, frozen):
    require(type(frozen["count"]) is int and frozen["count"] == 64 and
            frozen["policies"] == list(POLICIES) and canonical(frozen["claims"]) == canonical(CLAIMS) and
            type(frozen["unit_rows"]) is int and frozen["unit_rows"] == 32 and
            type(frozen["page_rows"]) is int and frozen["page_rows"] == 256,
            "frozen protocol identity")
    integer(frozen["root_bytes"], 512, ROOT_CAP, "frozen root bytes")
    require(type(frozen["membership_bytes"]) is int and
            frozen["membership_bytes"] == ((frozen["rows"] + 31) // 32) * 4,
            "frozen membership bytes")
    fields(frozen["inputs"], NOMINATE_FIELDS, "frozen inputs (truth prohibited)")
    for name in ("requests", "panel", "protocol", "order"):
        require(config[name] == frozen["inputs"][name], "frozen input pointer changed: " + name)
    load_protocol(config["protocol"])
    queries, source_ids, panel = load_panel(config, frozen["dimensions"])
    require(panel["selected_sha256"] == frozen["panel_selected_sha256"], "frozen panel identity")
    records = frozen["records"]
    require(type(records) is list and len(records) == 64, "all64 immutable nominations required")
    rows = frozen["rows"]
    requested = (((rows + 31) // 32 + 63) // 64)
    leaf_count = integer(frozen["leaf_count"], requested, 2 * requested - 1, "frozen leaf count")
    for ordinal, entry in enumerate(records):
        fields(entry, "query_ordinal source_ordinal policies", "frozen query")
        require(type(entry["query_ordinal"]) is int and entry["query_ordinal"] == ordinal and
                type(entry["source_ordinal"]) is int and entry["source_ordinal"] == source_ids[ordinal],
                "frozen query/panel order")
        fields(entry["policies"], " ".join(POLICIES), "frozen policies")
        for name, maximum in zip(POLICIES, (16, 32)):
            frozen_record(entry["policies"][name], rows, leaf_count, maximum, frozen["dimensions"])
        old, new = (entry["policies"][name] for name in POLICIES)
        require(old["selected_leaf_ids"] == new["selected_leaf_ids"][:len(old["selected_leaf_ids"])]
                and set(old["units"]) <= set(new["units"]), "same frozen ranking/policy containment")
    # Every nomination/request/panel/order check precedes the first truth open.
    inverse = load_order(config["order"], rows)
    require(config["truth"]["bytes"] == 64 * 100 * 8, "truth exact64x100 i64 length")
    truth = authenticated(config["truth"], 64 * 100 * 8)
    per_query = []
    for ordinal, entry in enumerate(records):
        gold = struct.unpack_from("<100q", truth, ordinal * 800)
        require(all(0 <= source < rows for source in gold) and len(set(gold)) == 100,
                "truth source ordinal range/uniqueness")
        physical = [inverse[source] for source in gold]
        results = {}
        for name in POLICIES:
            nomination = entry["policies"][name]
            units, pages = set(nomination["units"]), set(nomination["page_closure"])
            values = dict(nomination)
            values.update(selected_units=len(units), page_closure_pages=len(pages),
                          root_bytes=frozen["root_bytes"],
                          root_plus_selected_leaf_bytes=frozen["root_bytes"] + nomination["selected_leaf_bytes"])
            for k in (10, 100):
                for label, width, selected in (("nomination_unit", 32, units), ("page_closure", 256, pages)):
                    hits = sum(row // width in selected for row in physical[:k])
                    values[label + "_hits" + str(k)] = hits
                    values[label + "_R" + str(k)] = hits / k
            results[name] = values
        per_query.append(dict(query_ordinal=ordinal, source_ordinal=source_ids[ordinal], policies=results))
    summary = {}
    for name in POLICIES:
        values = {}
        for label in ("nomination_unit", "page_closure"):
            for k in (10, 100):
                hits = sum(row["policies"][name][label + "_hits" + str(k)] for row in per_query)
                values[label + "_hits" + str(k)] = hits
                values[label + "_R" + str(k)] = hits / (64 * k)
        values["within_current_walk_guard_queries"] = sum(
            row["policies"][name]["within_current_walk_guard"] for row in per_query)
        summary[name] = values
    passed = summary[POLICIES[1]]["page_closure_hits10"] >= 608
    return dict(schema=RESULT_SCHEMA, phase="truth_reduction_after_immutable_nomination",
                status="GO-for-native-investigation" if passed else "FAIL", claims=CLAIMS.copy(),
                threshold=dict(hits10_minimum=608, denominator10=640), summary=summary,
                records=per_query, config=config_identity, inputs=config,
                nomination_inputs=frozen["inputs"], provenance=frozen["provenance"],
                nomination_code_identity=frozen["code_identity"], reduction_code_identity=code_identity(),
                row_identity=dict(truth="LEi64 source ordinal 0..999999;100 distinct per query in exactGT order",
                                  order="LEu64 physical position -> source ordinal bijection",
                                  mapping="truth source ordinal -> inverse order -> physical //32 unit, //256 page"))


def self_check():
    """Bounded synthetic format, phase-order, coverage and fail-closed checks."""
    import copy
    import subprocess
    import time

    started = time.monotonic()

    def check(condition, message):
        require(condition, "self-check: " + message)

    def rejected(action, message):
        try:
            action()
        except (ValueError, OSError, KeyError, TypeError, struct.error, UnicodeError):
            return
        raise ValueError("self-check accepted " + message)

    def fixture(rows, d, profile, payload=True):
        source = dict(profile=profile, schema="synthetic-unit-source", rows=rows, dimensions=d,
                      root_sha256=sha(b"synthetic source"), centroids_sha256=sha(b"synthetic centroids"))
        u = (rows + 31) // 32
        count = (u + 63) // 64
        body = bytearray(512 + count * (64 + d * 4))
        body[:8] = b"BORSUSR2"
        for offset, value in ((8, 512), (12, 2 if profile == "fresh1m" else 1),
                              (24, d), (28, u), (32, count), (36, count), (40, 1),
                              (44, len(source["schema"]))):
            struct.pack_into("<I", body, offset, value)
        width = 4 + 2 * d
        for offset, value in ((16, rows), (48, 32 + u * d * 2), (56, 4 * u),
                              (64, u * width), (72, allocation_estimate(rows, d)),
                              (80, (512 if profile == "fresh1m" else 128) * MIB)):
            struct.pack_into("<Q", body, offset, value)
        membership = b"".join(struct.pack("<I", unit // 64) for unit in range(u))
        leaves = bytearray()
        for leaf_id in range(count):
            units = list(range(leaf_id * 64, min((leaf_id + 1) * 64, u)))
            leaf_body = (b"".join(struct.pack("<I", unit) +
                                  struct.pack("<" + str(d) + "e", 1, *([0] * (d - 1)))
                                  for unit in units) if payload else b"")
            start = 512 + leaf_id * 64
            struct.pack_into("<IIQIII", body, start, 0, leaf_id, leaf_id * 64 * width,
                             len(units) * width, len(units),
                             sum(min(32, rows - unit * 32) for unit in units))
            body[start + 32:start + 64] = bytes.fromhex(sha(leaf_body))
            struct.pack_into("<" + str(d) + "f", body, 512 + count * 64 + leaf_id * d * 4,
                             1, *([0] * (d - 1)))
            leaves.extend(leaf_body)
        for offset, identity in ((128, source["root_sha256"]), (160, source["centroids_sha256"]),
                                 (192, sha(membership)), (224, sha(leaves))):
            body[offset:offset + 32] = bytes.fromhex(identity)
        body[256:256 + len(source["schema"])] = source["schema"].encode()
        return bytes(body), membership, bytes(leaves), source

    query = cosine_query([3, 4])
    check(query == [f32(0.6), f32(0.8)], "native FP32 normalization")
    check(cosine_query([1, 0]) == [1, 0], "unit norm tolerance")
    # Conversion must precede the ordered norm, including a rounding-sensitive input.
    raw = [16777217, 16777216, 3]
    rounded = [f32(x) for x in raw]
    norm = math.sqrt(sum(x * x for x in rounded))
    check(cosine_query(raw) == [f32(x / norm) for x in rounded], "FP32 before f64 norm")
    ties = [dict(leaf_id=i, prototype=(1.0, 0.0)) for i in range(33)]
    check(selections(ranking(query, ties)) == (list(range(16)), list(range(32))), "rank ties by ID")
    check(selections([(0.0, i) for i in range(33)])[0] == list(range(16)), "zero-distance boundary")
    check(selections([(1.0, i) for i in range(8)] + [(1.16, i) for i in range(8, 33)])[0]
          == list(range(8)), "1.15 boundary stop")
    check(selections([(1.0, i) for i in range(8)] + [(1.15, i) for i in range(8, 33)])[0]
          == list(range(16)), "1.15 boundary inclusive")
    for values in ([0, 0], [float("nan"), 1], [float("inf"), 1], [1e300, 1], [True, 1]):
        rejected(lambda values=values: cosine_query(values), "invalid/nonfinite query")
    rejected(lambda: decode(b'{"x":1,"x":2}'), "duplicate JSON fields")
    rejected(lambda: decode(b'{"x":NaN}'), "JSON NaN")
    rejected(lambda: decode(b'{"x":Infinity}'), "JSON infinity")

    with tempfile.TemporaryDirectory(prefix="semantic-binary-check-") as name:
        folder = Path(name)

        def put(filename, body):
            path = folder / filename
            if path.exists():
                path.chmod(0o600)
            path.write_bytes(body)
            return dict(path=str(path), bytes=len(body), sha256=sha(body))

        binary, member_body, payload, source = fixture(65537, 2, "native100k")
        source_identity(source, production=False)
        root = _parse_root(binary, source)
        membership = validate_membership(member_body, root)
        check(root["unit_count"] == 2049 and root["leaves"][-1]["source_rows"] == 1,
              "native partial tail")
        rejected(lambda: parse_root(binary, source), "synthetic geometry in production parser")
        production_body, production_member, _, production_source = fixture(1000000, 768, "fresh1m", False)
        production = parse_root(production_body, production_source)
        validate_membership(production_member, production)
        check(production["unit_count"] == 31250 and production["leaves"][-1]["unit_count"] == 18,
              "production fixed1M/768 metadata")
        for offset, value in ((8, 511), (12, 1), (24, 767), (28, 31249), (32, 0),
                              (36, 1), (40, 2), (44, 257)):
            changed = bytearray(production_body)
            struct.pack_into("<I", changed, offset, value)
            rejected(lambda changed=changed: parse_root(changed, production_source), "production header")
        for offset in (0, 16, 48, 56, 64, 72, 80, 88, 128, 160, 511, 512 + 28):
            changed = bytearray(production_body)
            changed[offset] ^= 1
            rejected(lambda changed=changed: parse_root(changed, production_source), "header/reserved/accounting")
        rejected(lambda: parse_root(production_body[:-1], production_source), "truncated root")
        rejected(lambda: parse_root(production_body + b"\0", production_source), "root trailing byte")
        for offset, value in ((512, 1), (516, 1), (528, 1), (532, 0), (536, 0), (576 + 4, 3)):
            changed = bytearray(production_body)
            struct.pack_into("<I", changed, offset, value)
            rejected(lambda changed=changed: parse_root(changed, production_source), "directory")
        for value in (float("nan"), float("inf")):
            changed = bytearray(production_body)
            struct.pack_into("<f", changed, 512 + len(production["leaves"]) * 64, value)
            rejected(lambda changed=changed: parse_root(changed, production_source), "prototype NaN/Inf")
        del production, production_body, production_member

        leaf_pin = put("leaves.bin", payload)
        with regular(leaf_pin["path"], len(payload), len(payload)) as stream:
            old = nomination_record(list(range(16)), stream, root, membership)
            new = nomination_record(list(range(32)), stream, root, membership)
            tail = nomination_record([32], stream, root, membership)
        check(len(old["units"]) == 1024 and len(new["units"]) == 2048 and
              old["within_current_walk_guard"] and not new["within_current_walk_guard"], "walk guard exposed")
        check(tail["selected_source_rows"] == tail["page_closure_rows"] == 1 and
              tail["seed_additions"] == [], "tail nomination and closure")
        scattered = copy.deepcopy(old)
        scattered["units"] = [8 * i + 1 for i in range(1024)]
        scattered["page_closure"] = list(range(1024))
        scattered.update(seed_page=0, seed_additions=[0, 2, 3, 4, 5, 6, 7],
                         prospective_walk_units=1031, within_current_walk_guard=True,
                         selected_source_rows=32768, page_closure_rows=262144)
        frozen_record(scattered, 1000000, 977, 16, 2)
        check(scattered["prospective_walk_units"] == 1031, "current scattered guard plus7")

        # Refresh attacker-controlled leaf/header hashes to test semantic checks too.
        for kind in ("duplicate", "out-of-range", "membership", "nan", "inf", "prototype"):
            changed = bytearray(payload)
            bad_root = copy.deepcopy(root)
            if kind == "duplicate":
                struct.pack_into("<I", changed, 8, 0)
            elif kind == "out-of-range":
                struct.pack_into("<I", changed, 0, 999999)
            elif kind == "membership":
                struct.pack_into("<I", changed, 0, 64)
            elif kind in ("nan", "inf"):
                struct.pack_into("<e", changed, 4, float(kind))
            else:
                struct.pack_into("<e", changed, 4, 2.0)
            size = bad_root["leaves"][0]["bytes"]
            bad_root["leaves"][0]["sha256"] = sha(changed[:size])
            put("bad-leaf.bin", changed)
            with regular(folder / "bad-leaf.bin", len(changed), len(changed)) as stream:
                rejected(lambda: leaf_units(stream, bad_root["leaves"][0], bad_root, membership), kind)
        changed = bytearray(member_body)
        struct.pack_into("<I", changed, 0, 1)
        bad_root = dict(root, membership_sha256=sha(changed))
        rejected(lambda: validate_membership(changed, bad_root), "refreshed membership counts")
        changed = bytearray(member_body)
        struct.pack_into("<I", changed, 0, 999999)
        bad_root = dict(root, membership_sha256=sha(changed))
        rejected(lambda: validate_membership(changed, bad_root), "refreshed membership ID")
        rejected(lambda: validate_membership(member_body[:-1], root), "membership size")
        rejected(lambda: validate_membership(bytes(changed), root), "membership SHA")

        selected = [dict(query_ordinal=i, source_ordinal=1002000 + i) for i in range(64)]
        panel = dict(selected=selected, selected_sha256=sha(canonical(selected)[:-1]))
        protocol = dict(schema="borsuk-cohere-top32-fresh-coverage-prospective-v1",
                        rows=1000000, dimensions=768, count=64, truth_k=100,
                        coverage_only=True, nomination_freeze_before_truth=True,
                        current_production_walk_guard=1031, prospective_initial_walk_units_max=2055,
                        selected_leaf_payload_bytes_max=ROOT_CAP, nominated_units_max=2048,
                        page_closure_pages_max=2048, current_production_guard_not_changed=True,
                        comparison_policies=list(POLICIES), threshold=dict(
                            denominator10=640, hits10_minimum=608, mean_page_closure_recall_at_10_minimum=0.95))
        inputs = dict(schema=NOMINATE_SCHEMA, source=source,
                      router_root=put("root.bin", binary), membership=put("membership.bin", member_body),
                      leaves=leaf_pin, order=put("order.u64", b"".join(struct.pack("<Q", i)
                               for i in reversed(range(source["rows"])))),
                      requests=put("requests.jsonl", b"".join(canonical(dict(ordinal=i, query=[3, 4]))
                                    for i in range(64))), panel=put("panel.json", canonical(panel)),
                      protocol=put("protocol.json", canonical(protocol)),
                      provenance=dict(source_commit="1" * 40, source_archive_sha256="2" * 64,
                                      builder_commit="3" * 40, builder_binary_sha256="4" * 64,
                                      source_identity=put("source.json", canonical(source)),
                                      resource_metadata=put("resources.json", canonical(dict(synthetic=True)))))
        provenance(inputs["provenance"])
        queries, source_ids, loaded_panel = load_panel(inputs, 2)
        input_pin = put("nominate-config.json", canonical(inputs))
        frozen = _nominate(inputs, input_pin, root, queries, source_ids, loaded_panel)
        check(frozen["truth_opened"] is False and len(frozen["records"]) == 64, "freeze without truth")
        nominal_pin = publish(folder / "nomination.json", frozen)
        check((folder / "nomination.json").stat().st_mode & 0o222 == 0, "read-only freeze")
        check(authenticated(nominal_pin, REPORT_CAP, True) == canonical(frozen), "canonical deterministic freeze")
        rejected(lambda: publish(folder / "nomination.json", frozen), "no-overwrite output")
        rejected(lambda: reduce(dict(schema=REDUCE_SCHEMA, nomination=nominal_pin,
                                     requests=inputs["requests"], panel=inputs["panel"], protocol=inputs["protocol"],
                                     order=inputs["order"], truth=inputs["order"], truth_id_space="source_ordinal"),
                                input_pin), "native synthetic nomination in production reduction")
        # FIRST1M ordinals are source IDs, not physical offsets. Reverse order proves it.
        gold = [source["rows"] - 1 - physical for physical in range(32768, 32868)]
        reduce_config = dict(schema=REDUCE_SCHEMA, nomination=nominal_pin,
                             requests=inputs["requests"], panel=inputs["panel"], protocol=inputs["protocol"],
                             order=inputs["order"], truth_id_space="source_ordinal",
                             truth=put("truth.i64", struct.pack("<100q", *gold) * 64))
        config_pin = put("reduce-config.json", canonical(reduce_config))
        result = _reduce(reduce_config, config_pin, frozen)
        check(result["summary"][POLICIES[0]]["page_closure_hits10"] == 0 and
              result["summary"][POLICIES[1]]["page_closure_hits10"] == 640 and
              result["summary"][POLICIES[1]]["nomination_unit_hits100"] == 6400 and
              result["status"] == "GO-for-native-investigation", "old16 FAIL vs top32 GO source-order reversal")
        missing = struct.pack("<100q", *range(100)) * 64
        failure_config = dict(reduce_config, truth=put("missed.i64", missing))
        failure = _reduce(failure_config, config_pin, frozen)
        check(failure["status"] == "FAIL", "source IDs are never assumed physical")
        one_miss = [0] + gold[:99]
        for misses, expected in ((32, "GO-for-native-investigation"), (33, "FAIL")):
            threshold_truth = (struct.pack("<100q", *one_miss) * misses +
                               struct.pack("<100q", *gold) * (64 - misses))
            threshold_config = dict(reduce_config, truth=put("threshold.i64", threshold_truth))
            threshold_result = _reduce(threshold_config, config_pin, frozen)
            check(threshold_result["status"] == expected and
                  threshold_result["summary"][POLICIES[1]]["page_closure_hits10"] == 640 - misses,
                  "exact608/640 decision boundary")

        for changes in (dict(truth=reduce_config["truth"]), dict(unexpected=True)):
            rejected(lambda changes=changes: nominate(dict(inputs, **changes), input_pin),
                     "truth/extra fields in nominate config")
        rejected(lambda: nominate(inputs, input_pin), "production synthetic geometry")
        for gold_values in ([-1] + gold[1:], [source["rows"]] + gold[1:], [gold[1]] + gold[1:]):
            bad = dict(reduce_config, truth=put("bad-truth.i64", struct.pack("<100q", *gold_values) * 64))
            rejected(lambda bad=bad: _reduce(bad, config_pin, frozen), "truth negative/range/duplicate")
        for length in (51199, 51201):
            bad = dict(reduce_config, truth=put("bad-truth-length.i64", b"\0" * length))
            rejected(lambda bad=bad: _reduce(bad, config_pin, frozen), "truth exact length")
        inverse = load_order(inputs["order"], source["rows"])
        check(inverse[0] == source["rows"] - 1 and inverse[-1] == 0, "inverse source ordinal")
        order_body = bytearray(authenticated(inputs["order"], source["rows"] * 8))
        for value in (source["rows"], 0):
            struct.pack_into("<Q", order_body, 0, value)
            bad_order = put("bad-order.u64", order_body)
            rejected(lambda: load_order(bad_order, source["rows"]), "order range/duplicate")

        for kind in ("count", "order", "units", "pages", "ids", "guard", "policies", "truth"):
            bad_frozen = copy.deepcopy(frozen)
            record = bad_frozen["records"][0]["policies"][POLICIES[1]]
            if kind == "count":
                bad_frozen["records"].pop()
            elif kind == "order":
                bad_frozen["records"][0]["source_ordinal"] += 1
            elif kind == "units":
                record["units"].append(record["units"][-1])
            elif kind == "pages":
                record["page_closure"].pop()
            elif kind == "ids":
                record["selected_leaf_ids"].reverse()
            elif kind == "guard":
                record["within_current_walk_guard"] = True
            elif kind == "policies":
                bad_frozen["policies"].reverse()
            else:
                bad_frozen["inputs"]["truth"] = reduce_config["truth"]
            rejected(lambda bad_frozen=bad_frozen: _reduce(reduce_config, config_pin, bad_frozen),
                     "nomination tamper " + kind)
        bad_panel = copy.deepcopy(panel)
        bad_panel["selected"][0]["query_ordinal"] = 1
        bad_panel["selected_sha256"] = sha(canonical(bad_panel["selected"])[:-1])
        bad_inputs = dict(inputs, panel=put("bad-panel.json", canonical(bad_panel)))
        rejected(lambda: load_panel(bad_inputs, 2), "panel order tamper")
        bad_requests = dict(inputs, requests=put("bad-requests.jsonl", canonical(dict(ordinal=0, query=[1, 2]))))
        rejected(lambda: load_panel(bad_requests, 2), "request count tamper")
        bad_protocol = dict(protocol, current_production_walk_guard=2055)
        rejected(lambda: load_protocol(put("bad-protocol.json", canonical(bad_protocol))), "guard protocol tamper")

        # Bad nomination/order must fail before opening even a FIFO truth pointer.
        fifo = folder / "fifo"
        os.mkfifo(fifo)
        fifo_pin = dict(path=str(fifo), bytes=51200, sha256="0" * 64)
        bad_config = dict(reduce_config, truth=fifo_pin)
        bad_frozen = copy.deepcopy(frozen)
        bad_frozen["records"].pop()
        original_auth = globals()["authenticated"]
        truth_opens = []

        def watched_auth(identity, cap, immutable=False):
            if identity["path"] == str(fifo):
                truth_opens.append(True)
                raise ValueError("truth was opened too early")
            return original_auth(identity, cap, immutable)

        globals()["authenticated"] = watched_auth
        try:
            rejected(lambda: _reduce(bad_config, config_pin, bad_frozen), "missing nomination before truth")
            bad_order = dict(inputs["order"], sha256="0" * 64)
            bad_config["order"] = bad_order
            bad_frozen = copy.deepcopy(frozen)
            bad_frozen["inputs"]["order"] = bad_order
            rejected(lambda: _reduce(bad_config, config_pin, bad_frozen), "bad order before truth")
            check(not truth_opens, "no truth before complete authenticated freeze/order")
        finally:
            globals()["authenticated"] = original_auth
        rejected(lambda: authenticated(fifo_pin, 51200), "FIFO without blocking")
        symlink = folder / "link"
        symlink.symlink_to(folder / "root.bin")
        rejected(lambda: authenticated(dict(inputs["router_root"], path=str(symlink)), ROOT_CAP), "symlink file")
        directory_link = folder / "directory-link"
        directory_link.symlink_to(folder, target_is_directory=True)
        rejected(lambda: authenticated(dict(inputs["router_root"], path=str(directory_link / "root.bin")), ROOT_CAP),
                 "symlink ancestor")
        rejected(lambda: authenticated(dict(inputs["router_root"], path=str(folder)), ROOT_CAP), "directory file")
        rejected(lambda: authenticated(dict(inputs["router_root"], bytes=1), ROOT_CAP), "exact bounded read")
        rejected(lambda: authenticated(dict(inputs["router_root"], sha256="0" * 64), ROOT_CAP), "authenticated SHA")
        (folder / "nomination.json").chmod(0o644)
        rejected(lambda: authenticated(nominal_pin, REPORT_CAP, immutable=True), "writable nomination")
        (folder / "nomination.json").chmod(0o444)
        symlink_output = folder / "output-link"
        symlink_output.symlink_to(folder / "root.bin")
        rejected(lambda: publish(symlink_output, {}), "no overwrite symlink output")

        # -O never bypasses validation; a separate process probes the public CLI.
        bad_config_pin = put("truth-nominate-config.json", canonical(dict(inputs, truth=fifo_pin)))
        command = [sys.executable, "-O", str(Path(__file__).absolute()), "--mode", "nominate",
                   "--config-file", bad_config_pin["path"], "--config-sha256", bad_config_pin["sha256"],
                   "--output-file", str(folder / "must-not-exist.json")]
        run = subprocess.run(command, capture_output=True, timeout=5, check=False)
        check(run.returncode == 2 and b"truth prohibited" in run.stderr and
              not (folder / "must-not-exist.json").exists(), "-O fail-closed CLI/no truth output")
        command = [sys.executable, str(Path(__file__).absolute()), "--mode", "nominate", "--truth-file", str(fifo)]
        run = subprocess.run(command, capture_output=True, timeout=5, check=False)
        check(run.returncode == 2 and b"unrecognized arguments" in run.stderr, "no truth CLI acceptance")
    check(time.monotonic() - started < 55, "55s synthetic resource deadline")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--self-check", action="store_true")
    parser.add_argument("--mode", choices=("nominate", "reduce"))
    parser.add_argument("--config-file")
    parser.add_argument("--config-sha256")
    parser.add_argument("--output-file")
    args = parser.parse_args()
    if args.self_check:
        require(not any((args.mode, args.config_file, args.config_sha256, args.output_file)),
                "self-check is synthetic only")
        self_check()
        print("semantic binary coverage self-check: PASS")
        return 0
    if not all((args.mode, args.config_file, args.config_sha256, args.output_file)):
        parser.error("mode, authenticated config, and output are required")
    with regular(args.config_file, cap=JSON_CAP) as stream:
        body = stream.read(JSON_CAP + 1)
    digest(args.config_sha256)
    require(sha(body) == args.config_sha256, "config SHA256 mismatch")
    identity = dict(path=args.config_file, bytes=len(body), sha256=args.config_sha256)
    config = decode(body)
    result = (nominate if args.mode == "nominate" else reduce)(config, identity)
    output = publish(args.output_file, result)
    print(canonical(dict(output=output, status=result.get("status", "FROZEN"))).decode(), end="")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except (ValueError, OSError, KeyError, TypeError, struct.error, UnicodeError) as error:
        print("semantic binary coverage: " + str(error), file=sys.stderr)
        sys.exit(2)
