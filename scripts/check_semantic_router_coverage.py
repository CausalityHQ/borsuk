#!/usr/bin/env python3
"""Offline coverage ceiling for the frozen semantic-router development panel.

Usage: --router-dir DIR --manifest-sha256 SHA --requests-file JSONL
       --requests-sha256 SHA --truth-file U32 --truth-sha256 SHA
       --order-file U64 --order-sha256 SHA --output-file JSON
       --self-check runs synthetic checks only.

No scorer, corpus fitting, physical S3 measurement, RSS, latency, or QPS result.
"""

import argparse
import hashlib
import json
import math
import os
from pathlib import Path
import stat
import struct
import tempfile

SCHEMA = "borsuk-semantic-unit-router-research-v1"
ROOT_CAP = 1024 * 1024
LEAF_CAP = 2 * 1024 * 1024
REQUESTS_CAP = 32 * 1024 * 1024
TRUTH_BYTES = 1000 * 100 * 4
ALLOCATION_CAP = 128 * 1024 * 1024
BUILDER_FORMAT_REFERENCE_COMMIT = "3b53af2cf1dbd9611212cf59dba212df8f23b6bb"
PROTOCOL_HASHES = {
    "semantic-router-falsifier.md": "d324cd2e8f3d677ae18a8159e5547d8e2a261b0bd228c73c765f107ec4cb78bd",
    "semantic-router-plan.md": "c33246f192b4af22f17fc3fdac3528d759fa0a21071ab5b469ea305eb40f174d",
    "semantic-router-input-authority.json": "80de68e4cf1e1558e45364b56c045f90bb56d34f1a01eb49c3428688b3ddb1c2",
    "semantic-router-row-identity.json": "d553f58d184b023c4d323399a69c5badb36910ce1c9675d1f95fd9e1b749bd4f",
}
ALGORITHM = {
    "trainer": "train_logical_cell_centroids", "metric": "SquaredEuclidean",
    "iterations": 12, "max_leaf_units": 64, "nearest_ties": "center ordinal",
    "group_sort": "squared distance, original unit ID",
    "root_prototype": "unweighted unit mean; f64 accumulation to finite f32",
    "normalization": "none", "payload": "original FP16 little endian",
}


def require(condition, message):
    if not condition:
        raise ValueError(message)


def sha(body):
    return hashlib.sha256(body).hexdigest()


def canonical(value):
    return (json.dumps(value, sort_keys=True, separators=(",", ":"),
                       allow_nan=False) + "\n").encode()


def integer(value, low, high, name):
    require(type(value) is int and low <= value <= high, "invalid " + name)
    return value


def digest(value):
    require(isinstance(value, str) and len(value) == 64 and
            all(c in "0123456789abcdef" for c in value), "invalid SHA256")
    return value


def fields(value, names, name):
    require(type(value) is dict and set(value) == set(names.split()), name + " fields")


def decode(body):
    def pairs(items):
        result = {}
        for key, value in items:
            require(key not in result, "duplicate JSON field: " + key)
            result[key] = value
        return result

    def nonfinite(value):
        raise ValueError("nonfinite JSON number: " + value)

    return json.loads(body, object_pairs_hook=pairs, parse_constant=nonfinite)


def f32(value):
    require(type(value) in (int, float), "invalid number")
    try:
        require(math.isfinite(value), "nonfinite number")
        result = struct.unpack("<f", struct.pack("<f", value))[0]
    except (OverflowError, struct.error) as error:
        raise ValueError("number out of f32 range") from error
    require(math.isfinite(result), "nonfinite f32")
    return result


def cosine_query(values):
    query = [f32(value) for value in values]
    squared = 0.0
    for value in query:
        squared += value * value
    require(math.isfinite(squared) and squared > 0, "cosine query norm")
    if abs(squared - 1) > 1e-6:
        norm = math.sqrt(squared)
        query = [f32(value / norm) for value in query]
    return query


def authenticated(path, expected_sha, cap, expected_bytes=None):
    digest(expected_sha)
    # O_NONBLOCK permits rejecting a FIFO without waiting for a writer.
    fd = os.open(path, os.O_RDONLY | os.O_NONBLOCK | os.O_NOFOLLOW)
    with os.fdopen(fd, "rb") as source:
        info = os.fstat(source.fileno())
        require(stat.S_ISREG(info.st_mode) and info.st_size <= cap, "file type or byte cap")
        if expected_bytes is not None:
            require(info.st_size == expected_bytes, "artifact length")
        body = source.read(info.st_size + 1)
        require(len(body) == info.st_size, "artifact length changed")
    require(sha(body) == expected_sha, "artifact SHA256 mismatch")
    return body


def nominate(query, leaves):
    ranked = []
    for leaf in leaves:
        distance = 0.0
        for a, b in zip(query, leaf["prototype"]):
            delta = a - b
            distance += delta * delta
        ranked.append((distance, leaf["leaf_id"]))
    ranked.sort()
    require(all(math.isfinite(distance) for distance, _ in ranked), "nonfinite distance")
    chosen = ranked[:8]
    if len(chosen) == 8:
        boundary = chosen[-1][0]
        for distance, leaf_id in ranked[8:16]:
            if (distance == 0 if boundary == 0 else distance <= 1.15 * boundary):
                chosen.append((distance, leaf_id))
            else:
                break
    return [leaf_id for _, leaf_id in chosen]


def load_router(directory, manifest_sha):
    directory = Path(directory)
    body = authenticated(directory / "manifest.json", manifest_sha, ROOT_CAP)
    manifest = decode(body)
    fields(manifest, "schema input_schema input_root_sha256 input_centroids_sha256 "
           "input_centroids_bytes rows dimensions unit_rows page_rows unit_count "
           "final_unit_rows algorithm modeled_peak_allocation_bytes "
           "modeled_allocation_limit_bytes allocation_model membership leaf_payload leaves",
           "manifest")
    require(manifest["schema"] == SCHEMA and
            manifest["input_schema"] == "borsuk-two-bit-generation-v4", "router schema")
    digest(manifest["input_root_sha256"])
    digest(manifest["input_centroids_sha256"])
    rows = integer(manifest["rows"], 1, 100000, "rows")
    dimensions = integer(manifest["dimensions"], 1, 768, "dimensions")
    units = (rows + 31) // 32
    for key, expected in [("unit_rows", 32), ("page_rows", 256), ("unit_count", units),
                          ("final_unit_rows", rows - 32 * (units - 1)),
                          ("input_centroids_bytes", 32 + units * dimensions * 2)]:
        require(type(manifest[key]) is int and manifest[key] == expected,
                "router geometry: " + key)
    integer(manifest["modeled_peak_allocation_bytes"], 1, ALLOCATION_CAP, "allocation estimate")
    require(type(manifest["modeled_allocation_limit_bytes"]) is int and
            manifest["modeled_allocation_limit_bytes"] == ALLOCATION_CAP and
            manifest["allocation_model"] ==
            "conservative allocation capacities; not RSS or an enforced process limit",
            "builder allocation model")
    algorithm = manifest["algorithm"]
    fields(algorithm, " ".join(ALGORITHM) + " requested_centers training_centers", "algorithm")
    require(all(type(algorithm[k]) is type(v) and algorithm[k] == v
                for k, v in ALGORITHM.items()), "router algorithm")
    requested = (units + 63) // 64
    require(type(algorithm["requested_centers"]) is int and
            algorithm["requested_centers"] == requested, "requested centers")
    training = integer(algorithm["training_centers"], 1, requested, "training centers")
    record_bytes = 4 + dimensions * 2
    bodies = {}
    for key, filename, size in [("membership", "membership.bin", units * 4),
                                ("leaf_payload", "leaves.bin", units * record_bytes)]:
        identity = manifest[key]
        fields(identity, "bytes sha256", key)
        require(type(identity["bytes"]) is int and identity["bytes"] == size,
                key + " length")
        bodies[key] = authenticated(directory / filename, identity["sha256"], size, size)
    membership = struct.unpack("<" + str(units) + "I", bodies["membership"])
    payload = bodies["leaf_payload"]
    leaves = manifest["leaves"]
    require(type(leaves) is list and 1 <= len(leaves) <= min(units, 2 * requested - 1),
            "leaf count cap")
    require(all(leaf_id < len(leaves) for leaf_id in membership), "membership ID")
    seen = bytearray(units)
    # Reconstruct the bounded original centroid blob from original unit IDs,
    # authenticating FP16 identity without opening a source generation.
    centroids = bytearray(struct.pack("<8sQIIII", b"BORSUCP1", rows, dimensions, 32, 256, 0))
    centroids.extend(b"\0" * (units * dimensions * 2))
    offset = 0
    previous = None
    first_vector = None
    identical = True
    leaf_pages = []
    for leaf_id, leaf in enumerate(leaves):
        fields(leaf, "leaf_id group_ordinal chunk_ordinal offset bytes unit_count "
               "source_rows sha256 prototype", "leaf")
        require(type(leaf["leaf_id"]) is int and leaf["leaf_id"] == leaf_id, "leaf_id")
        count = integer(leaf["unit_count"], 1, 64, "unit_count")
        group = integer(leaf["group_ordinal"], 0, training - 1, "group_ordinal")
        chunk = integer(leaf["chunk_ordinal"], 0, units - 1, "chunk_ordinal")
        if previous is None or group != previous["group_ordinal"]:
            require(chunk == 0 and (previous is None or group > previous["group_ordinal"]),
                    "leaf group/chunk ordering")
        else:
            require(chunk == previous["chunk_ordinal"] + 1 and previous["unit_count"] == 64,
                    "leaf chunk ordering")
        require(type(leaf["offset"]) is int and leaf["offset"] == offset, "leaf offset")
        size = count * record_bytes
        require(type(leaf["bytes"]) is int and leaf["bytes"] == size, "leaf length")
        leaf_body = payload[offset:offset + size]
        require(len(leaf_body) == size and sha(leaf_body) == digest(leaf["sha256"]),
                "leaf length or SHA256")
        sums = [0.0] * dimensions
        source_rows = 0
        pages = set()
        for start in range(offset, offset + size, record_bytes):
            unit = struct.unpack_from("<I", payload, start)[0]
            require(unit < units and not seen[unit], "complete disjoint partition")
            require(membership[unit] == leaf_id, "membership disagreement")
            seen[unit] = 1
            pages.add(unit // 8)
            source_rows += min(32, rows - 32 * unit)
            vector = struct.unpack_from("<" + str(dimensions) + "e", payload, start + 4)
            require(all(math.isfinite(v) for v in vector), "nonfinite FP16 centroid")
            if first_vector is None:
                first_vector = vector
            identical = identical and vector == first_vector
            for d, value in enumerate(vector):
                sums[d] += value
            original = 32 + unit * dimensions * 2
            centroids[original:original + dimensions * 2] = payload[start + 4:start + record_bytes]
        require(type(leaf["source_rows"]) is int and leaf["source_rows"] == source_rows,
                "leaf source_rows")
        require(type(leaf["prototype"]) is list and len(leaf["prototype"]) == dimensions,
                "prototype dimensions")
        prototype = [f32(v) for v in leaf["prototype"]]
        expected = [f32(value / count) for value in sums]
        require(struct.pack("<" + str(dimensions) + "f", *prototype) ==
                struct.pack("<" + str(dimensions) + "f", *expected), "root prototype disagreement")
        leaf["prototype"] = prototype
        leaf_pages.append(len(pages))
        offset += size
        previous = leaf
    require(offset == len(payload) and all(seen), "incomplete partition")
    require(training == (1 if identical else requested), "training center identity")
    require(sha(centroids) == manifest["input_centroids_sha256"], "original centroid SHA256")
    return manifest, membership, len(body), leaf_pages


def load_panel(requests, requests_sha, truth, truth_sha, rows, dimensions):
    requests_body = authenticated(requests, requests_sha, REQUESTS_CAP)
    truth_body = authenticated(truth, truth_sha, TRUTH_BYTES, TRUTH_BYTES)
    lines = requests_body.splitlines()
    require(len(lines) == 1000, "expected 1000 request lines")
    panel = []
    for ordinal, line in enumerate(lines):
        request = decode(line)
        require(type(request) is dict and set(request) in (
            {"query_ordinal", "query"}, {"query_ordinal", "query", "nominees", "primary_count"}),
            "request fields")
        if "nominees" in request:
            nominees = request["nominees"]
            require(type(nominees) is list and 1 <= len(nominees) <= rows, "nominees shape")
            require(all(type(i) is int and 0 <= i < rows for i in nominees), "nominee ID")
            require(len(set(nominees)) == len(nominees), "duplicate nominee ID")
            integer(request["primary_count"], 1, len(nominees), "primary_count")
        require(type(request["query_ordinal"]) is int and request["query_ordinal"] == ordinal,
                "query/truth ordinal mismatch")
        require(type(request["query"]) is list and len(request["query"]) == dimensions,
                "query dimensions")
        query = cosine_query(request["query"])
        if ordinal < 64:
            panel.append(query)
        gold = struct.unpack_from("<100I", truth_body, ordinal * 400)
        require(all(row < rows for row in gold), "truth ID out of range")
        require(len(set(gold)) == 100, "duplicate truth ID")
    return panel, truth_body, len(requests_body)


def load_order(path, expected_sha, rows):
    body = authenticated(path, expected_sha, 100000 * 8, rows * 8)
    inverse = [-1] * rows
    for physical, (logical,) in enumerate(struct.iter_unpack("<Q", body)):
        require(logical < rows and inverse[logical] == -1, "order must be a complete permutation")
        inverse[logical] = physical
    require(all(physical >= 0 for physical in inverse), "incomplete order permutation")
    return inverse


def admit_query(root_bytes, leaf_bytes, leaf_count):
    require(root_bytes <= ROOT_CAP and leaf_bytes <= LEAF_CAP and 1 <= leaf_count <= 16,
            "query router byte/range cap")


def evaluate(directory, manifest_sha, requests, requests_sha, truth, truth_sha, order, order_sha):
    manifest, membership, root_bytes, leaf_pages = load_router(directory, manifest_sha)
    rows = manifest["rows"]
    inverse = load_order(order, order_sha, rows)
    queries, truth_body, requests_bytes = load_panel(
        requests, requests_sha, truth, truth_sha, rows, manifest["dimensions"])
    records = []
    for ordinal, query in enumerate(queries):
        selected = nominate(query, manifest["leaves"])
        selected_set = set(selected)
        units = {unit for unit, leaf_id in enumerate(membership) if leaf_id in selected_set}
        pages = {unit // 8 for unit in units}
        leaf_bytes = sum(manifest["leaves"][i]["bytes"] for i in selected)
        admit_query(root_bytes, leaf_bytes, len(selected))
        logical_gold = struct.unpack_from("<100I", truth_body, ordinal * 400)
        gold = [inverse[logical] for logical in logical_gold]
        record = dict(query_ordinal=ordinal, truth_ordinal=ordinal,
            selected_leaf_ids=selected, selected_leaves=len(selected), selected_units=len(units),
            selected_source_rows=sum(min(32, rows - 32 * unit) for unit in units),
            page_closure_pages=len(pages),
            page_closure_rows=sum(min(256, rows - 256 * page) for page in pages),
            root_bytes=root_bytes, selected_leaf_bytes=leaf_bytes,
            root_plus_selected_leaf_bytes=root_bytes + leaf_bytes,
            modeled_leaf_range_calls=len(selected), modeled_root_plus_leaf_calls=1 + len(selected))
        for k in (10, 100):
            record["selected_unit_truth" + str(k)] = sum(row // 32 in units for row in gold[:k]) / k
            record["page_closure_truth" + str(k)] = sum(row // 256 in pages for row in gold[:k]) / k
        records.append(record)
    summary = {}
    for key in records[0]:
        if key in ("query_ordinal", "truth_ordinal", "selected_leaf_ids"):
            continue
        values = sorted(record[key] for record in records)
        mean = sum(values) / 64
        if key.endswith(("truth10", "truth100")):
            k = 10 if key.endswith("truth10") else 100
            mean = sum(round(value * k) for value in values) / (64 * k)
        summary[key] = dict(mean=mean, min=values[0],
            p05=values[math.ceil(.05 * 64) - 1], p50=values[math.ceil(.5 * 64) - 1],
            p90=values[math.ceil(.9 * 64) - 1], p95=values[math.ceil(.95 * 64) - 1], max=values[-1])
    passed = summary["page_closure_truth10"]["mean"] >= .95
    return dict(schema="borsuk-semantic-router-coverage-v1",
        builder_format_reference_commit=BUILDER_FORMAT_REFERENCE_COMMIT,
        protocol_document_reference_sha256=PROTOCOL_HASHES,
        protocol_document_reference="fc66c161 snapshot; concrete protocol fields below are authoritative; "
            "parent experiment receipt pins final documents and builder source/binary",
        inputs=dict(router_manifest_sha256=manifest_sha, input_root_sha256=manifest["input_root_sha256"],
            input_centroids_sha256=manifest["input_centroids_sha256"],
            membership=manifest["membership"], leaf_payload=manifest["leaf_payload"],
            requests=dict(bytes=requests_bytes, sha256=requests_sha),
            truth=dict(bytes=TRUTH_BYTES, sha256=truth_sha),
            order=dict(bytes=rows * 8, sha256=order_sha)),
        population=dict(rows=rows, dimensions=manifest["dimensions"], unit_rows=32, page_rows=256,
            request_count=1000, truth_neighbors=100, evaluated_query_ordinals=list(range(64)),
            truth_ids="logical IDs; authenticated physical_position->logicalID LEu64 permutation, inverted before coverage",
            scope="consumed development panel; no generalization claim"),
        protocol=dict(metric="squared Euclidean; f64 over original f32 queries and prototypes",
            query_space="sq8_source::cosine_vector: JSON to f32, ordered f64 norm2; "
                "reject zero/nonfinite; keep if abs(norm2-1)<=1e-6; otherwise f64 divide by sqrt(norm2) to f32",
            prototype_space="unnormalized unweighted unit means, JSON to f32",
            ignored_request_metadata="nominees and primary_count validated when present, never used for nomination",
            root_ties="leaf ID", initial_leaves=8,
            epsilon_multiplier=1.15, epsilon_distance="squared Euclidean",
            zero_boundary="only zero-distance additions", max_leaves=16,
            admission="all original units in each selected leaf", root_cap_bytes=ROOT_CAP,
            selected_leaf_cap_bytes=LEAF_CAP, quantiles="nearest rank"),
        accounting=dict(mode="offline local validation; modeled range calls only; no physical S3 measurement",
            offline_validation_bytes=root_bytes + manifest["membership"]["bytes"] +
                manifest["leaf_payload"]["bytes"] + requests_bytes + TRUTH_BYTES + rows * 8,
            membership_preload_bytes=manifest["membership"]["bytes"], modeled_membership_preload_calls=1,
            membership_charge="separate one-time preload; excluded from per-query root/leaf totals",
            order_map_preload_bytes=rows * 8, order_map_role="offline truth evaluation only",
            query_root_charge="one full root per query, conservative without cache assumption",
            leaf_charge="one exact payload range per selected leaf; no coalescing",
            full_payload_validation="offline only; excluded from modeled query hydration",
            source_page_fetches="coverage closure only; source/SQ8 requests and bytes unmeasured"),
        gate=dict(metric="mean page_closure_truth10", threshold=.95, passed=passed,
            decision="SURVIVES_COVERAGE_ONLY" if passed else "KILL_DISCOVERY_CEILING",
            interpretation="recall ceiling only; returned quality unmeasured"),
        leaves=[dict(leaf_id=i, physical_pages=pages) for i, pages in enumerate(leaf_pages)],
        records=records, summary=summary)


def publish(path, report):
    path = Path(path)
    require(not os.path.lexists(path), "output already exists")
    body = canonical(report)
    # Linking a complete same-filesystem temporary file is atomic and refuses
    # replacement, including a target created after the preflight check.
    with tempfile.NamedTemporaryFile(dir=path.parent, prefix=".semantic-coverage-", delete=False) as staged:
        staged_path = Path(staged.name)
        try:
            staged.write(body)
            staged.flush()
            os.fsync(staged.fileno())
            os.link(staged_path, path)
        finally:
            staged_path.unlink()
    directory = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(directory)
    finally:
        os.close(directory)


def self_check():
    assert cosine_query([3.0, 4.0]) == cosine_query([6.0, 8.0]), "query scale invariance"
    near_unit = f32(1.0000002)
    assert cosine_query([near_unit]) == [near_unit], "preserved near-unit tolerance"
    assert f32(1.00000001) == 1.0, "prototype decimal must round to original f32"
    # These synthetic geometries distinguish squared-distance epsilon from a
    # radius epsilon, reject truncation, and expose original-ID/page confusion.
    leaves = [dict(leaf_id=i, prototype=[1.0]) for i in reversed(range(20))]
    assert nominate([0.0], leaves) == list(range(16)), "ties and sixteen cap"
    leaves = [dict(leaf_id=i, prototype=[0.0]) for i in range(9)]
    leaves.append(dict(leaf_id=9, prototype=[0.001]))
    assert nominate([0.0], leaves) == list(range(9)), "zero boundary"
    leaves = [dict(leaf_id=i, prototype=[0.0]) for i in range(7)]
    leaves += [dict(leaf_id=7, prototype=[10.0]),
               dict(leaf_id=8, prototype=[10.5]),
               dict(leaf_id=9, prototype=[11.0])]
    assert nominate([0.0], leaves) == list(range(9)), "squared epsilon"
    assert nominate([0.0], leaves[:3]) == [0, 1, 2], "all when fewer than eight"

    def rejected(call, message):
        try:
            call()
        except (ValueError, FileExistsError) as error:
            assert message in str(error), (message, str(error))
        else:
            raise AssertionError("accepted invalid input: " + message)

    admit_query(ROOT_CAP, LEAF_CAP, 16)
    for root_bytes, leaf_bytes, count in [(ROOT_CAP + 1, LEAF_CAP, 16),
                                         (ROOT_CAP, LEAF_CAP + 1, 16),
                                         (ROOT_CAP, LEAF_CAP, 17)]:
        rejected(lambda: admit_query(root_bytes, leaf_bytes, count), "cap")

    with tempfile.TemporaryDirectory(prefix="semantic-router-selfcheck-") as tmp:
        root = Path(tmp)
        router = root / "router"
        router.mkdir()
        # Original units are interleaved across leaves. Every physical page has
        # an admitted unit, so unit coverage .6/.4 becomes page coverage 1/1.
        rows, units, dimensions = 40929, 1280, 1
        membership = struct.pack("<1280I", *((u * 3) % 20 for u in range(units)))
        payload = bytearray()
        fixtures = []
        for leaf_id in range(20):
            offset = len(payload)
            ids = [u for u in range(units) if (u * 3) % 20 == leaf_id]
            for unit in ids:
                payload += struct.pack("<Ie", unit, float(leaf_id))
            body = payload[offset:]
            fixtures.append(dict(leaf_id=leaf_id, group_ordinal=leaf_id,
                chunk_ordinal=0, offset=offset, bytes=len(body), unit_count=64,
                source_rows=sum(min(32, rows - 32 * u) for u in ids),
                sha256=sha(body), prototype=[float(leaf_id)]))
        manifest = dict(schema=SCHEMA, input_schema="borsuk-two-bit-generation-v4",
            input_root_sha256="a" * 64,
            input_centroids_sha256=sha(struct.pack("<8sQIIII", b"BORSUCP1", rows, dimensions, 32, 256, 0) +
                b"".join(struct.pack("<e", float((u * 3) % 20)) for u in range(units))),
            input_centroids_bytes=32 + units * dimensions * 2, rows=rows,
            dimensions=dimensions, unit_rows=32, page_rows=256, unit_count=units,
            final_unit_rows=1, algorithm=dict(ALGORITHM, requested_centers=20,
                training_centers=20), modeled_peak_allocation_bytes=1,
            modeled_allocation_limit_bytes=ALLOCATION_CAP,
            allocation_model="conservative allocation capacities; not RSS or an enforced process limit",
            membership=dict(bytes=len(membership), sha256=sha(membership)),
            leaf_payload=dict(bytes=len(payload), sha256=sha(payload)), leaves=fixtures)
        requests = root / "requests.jsonl"
        query_body = b"".join(canonical(dict(query_ordinal=i, query=[1.0]))
                              for i in range(1000))
        requests.write_bytes(query_body)
        truth = root / "truth.u32"
        gold = struct.pack("<100I", *(32 * u for u in range(100))) * 1000
        truth.write_bytes(gold)
        order = root / "order.u64"
        order_body = struct.pack("<" + str(rows) + "Q", *range(rows))
        order.write_bytes(order_body)

        def write_router(m=manifest, members=membership, leaves_body=payload):
            body = canonical(m)
            (router / "manifest.json").write_bytes(body)
            (router / "membership.bin").write_bytes(members)
            (router / "leaves.bin").write_bytes(leaves_body)
            return sha(body)

        manifest_sha = write_router()
        args = (router, manifest_sha, requests, sha(query_body), truth, sha(gold), order, sha(order_body))
        report = evaluate(*args)
        assert len(report["records"]) == 64
        record = report["records"][0]
        assert record["selected_leaf_ids"] == [1, 0, 2, 3, 4, 5, 6, 7]
        assert all(leaf["physical_pages"] == 64 for leaf in report["leaves"])
        assert record["selected_units"] == 512
        assert record["selected_unit_truth10"] == .6
        assert record["selected_unit_truth100"] == .4
        assert record["page_closure_truth10"] == record["page_closure_truth100"] == 1
        assert record["page_closure_rows"] == rows, "partial last page"
        assert record["selected_leaf_bytes"] == 8 * 64 * 6
        assert report["gate"]["passed"] is True
        assert [record["query_ordinal"] for record in report["records"]] == list(range(64))
        output = root / "out.json"
        publish(output, report)
        original_output = output.read_bytes()
        other = root / "repeat.json"
        publish(other, evaluate(*args))
        assert original_output == other.read_bytes(), "nondeterministic output"
        rejected(lambda: publish(output, {}), "exists")
        assert output.read_bytes() == original_output, "overwrote result"

        metadata = b"".join(canonical(dict(query_ordinal=i, query=[1.0],
            nominees=[rows - 1, i], primary_count=1)) for i in range(1000))
        requests.write_bytes(metadata)
        with_metadata = evaluate(router, manifest_sha, requests, sha(metadata), truth, sha(gold),
                                 order, sha(order_body))
        assert with_metadata["records"] == report["records"], "old nominee metadata affected routing"
        for changed, message in [
            (metadata.replace(b'"primary_count":1', b'"primary_count":0', 1), "primary_count"),
            (metadata.replace(b'"nominees":[40928,0]', b'"nominees":[0,0]', 1), "duplicate"),
            (metadata.replace(b'"nominees":[40928,0]', b'"nominees":[40929,0]', 1), "nominee ID"),
            (query_body.replace(b'"query":[1.0]', b'"unknown":1,"query":[1.0]', 1), "fields")]:
            requests.write_bytes(changed)
            rejected(lambda: load_panel(requests, sha(changed), truth, sha(gold), rows, 1), message)
        requests.write_bytes(query_body)

        reversed_order = struct.pack("<" + str(rows) + "Q", *reversed(range(rows)))
        order.write_bytes(reversed_order)
        remapped = struct.pack("<100I", *(rows - 1 - 32 * u for u in range(100))) * 1000
        truth.write_bytes(remapped)
        mapped = evaluate(router, manifest_sha, requests, sha(query_body), truth, sha(remapped),
                          order, sha(reversed_order))
        assert mapped["records"] == report["records"], "logical truth IDs require inverse order"
        truth.write_bytes(gold)
        changed_space = evaluate(router, manifest_sha, requests, sha(query_body), truth, sha(gold),
                                 order, sha(reversed_order))
        assert changed_space["records"][0]["selected_unit_truth10"] == .2
        for changed in [reversed_order[:-1],
                        struct.pack("<Q", rows) + reversed_order[8:],
                        reversed_order[:8] + reversed_order[:8] + reversed_order[16:]]:
            order.write_bytes(changed)
            rejected(lambda: load_order(order, sha(changed), rows),
                     "length" if len(changed) != rows * 8 else "permutation")
        order.write_bytes(order_body)

        # A contiguous partition supplies a known failed ceiling. Preserve its
        # report even though the nomination gate failed.
        contiguous = json.loads(canonical(manifest))
        packed = b"".join(struct.pack("<Ie", u, float(u // 64)) for u in range(units))
        members = struct.pack("<1280I", *(u // 64 for u in range(units)))
        contiguous["membership"]["sha256"] = sha(members)
        contiguous["leaf_payload"]["sha256"] = sha(packed)
        contiguous["input_centroids_sha256"] = sha(
            struct.pack("<8sQIIII", b"BORSUCP1", rows, dimensions, 32, 256, 0) +
            b"".join(struct.pack("<e", float(u // 64)) for u in range(units)))
        for leaf in contiguous["leaves"]:
            start = leaf["offset"]
            leaf["sha256"] = sha(packed[start:start + leaf["bytes"]])
            leaf["source_rows"] = 2017 if leaf["leaf_id"] == 19 else 2048
        failure_sha = write_router(contiguous, members, packed)
        boundary_gold = b"".join(struct.pack("<100I", *(list(range(9)) + [rows - 1] +
            list(range(10, 100)) if i < 32 else list(range(100)))) for i in range(1000))
        truth.write_bytes(boundary_gold)
        boundary_report = evaluate(router, failure_sha, requests, sha(query_body), truth,
                                   sha(boundary_gold), order, sha(order_body))
        assert boundary_report["summary"]["page_closure_truth10"]["mean"] == .95
        assert boundary_report["gate"]["passed"] is True, "exact 95% boundary"
        missed = struct.pack("<100I", *range(rows - 100, rows)) * 1000
        truth.write_bytes(missed)
        failure = evaluate(router, failure_sha, requests, sha(query_body), truth, sha(missed), order, sha(order_body))
        assert failure["gate"]["passed"] is False
        assert failure["summary"]["page_closure_truth10"]["mean"] == 0
        publish(root / "failed.json", failure)
        # Verify the actual CLI preserves a failed gate while returning exit 2.
        import subprocess
        command = ["python3", str(Path(__file__).resolve()), "--router-dir", str(router),
            "--manifest-sha256", failure_sha, "--requests-file", str(requests),
            "--requests-sha256", sha(query_body), "--truth-file", str(truth),
            "--truth-sha256", sha(missed), "--order-file", str(order),
            "--order-sha256", sha(order_body), "--output-file", str(root / "cli-failed.json")]
        child = subprocess.run(command, capture_output=True, text=True, check=False)
        assert child.returncode == 2 and not child.stderr, child
        assert (root / "cli-failed.json").read_bytes() == canonical(failure)
        child = subprocess.run(command, capture_output=True, text=True, check=False)
        assert child.returncode == 1 and "exists" in child.stderr
        truth.write_bytes(gold)
        write_router()

        rejected(lambda: load_router(router, "0" * 64), "SHA256")
        tampered = bytearray(payload)
        tampered[-1] ^= 1
        (router / "leaves.bin").write_bytes(tampered)
        rejected(lambda: load_router(router, manifest_sha), "SHA256")
        write_router()
        for mutation, message in [
            (lambda m: m.update(rows=100001), "rows"),
            (lambda m: m.update(dimensions=769), "dimensions"),
            (lambda m: m.update(unit_rows=31), "geometry"),
            (lambda m: m.update(final_unit_rows=2), "geometry"),
            (lambda m: m["leaves"][0].update(unit_count=65), "unit_count"),
            (lambda m: m["leaves"][1].update(leaf_id=0), "leaf_id"),
            (lambda m: m["leaves"][0].update(offset=1), "offset"),
            (lambda m: m["leaves"][0].update(source_rows=1), "source_rows"),
            (lambda m: m["leaves"][0].update(prototype=[1.0]), "prototype"),
            (lambda m: m.update(extra=True), "fields"),
            (lambda m: m["leaves"][0].update(prototype=[True]), "number"),
            (lambda m: m["leaves"][0].update(prototype=[1e40]), "f32"),
        ]:
            bad = json.loads(canonical(manifest))
            mutation(bad)
            bad_sha = write_router(bad)
            rejected(lambda: load_router(router, bad_sha), message)
        rounded = json.loads(canonical(manifest))
        rounded["leaves"][1]["prototype"] = [1.00000001]
        rounded_router, _, _, _ = load_router(router, write_router(rounded))
        assert rounded_router["leaves"][1]["prototype"] == [1.0]
        # If a serialized f32 prototype is treated as f64, this ninth leaf
        # leaves the zero-distance tie and drops out of the sixteen-leaf set.
        zero_ties = json.loads(canonical(contiguous))
        zero_payload = b"".join(struct.pack("<Ie", u, 1.0) for u in range(units))
        zero_ties["algorithm"]["training_centers"] = 1
        zero_ties["leaf_payload"]["sha256"] = sha(zero_payload)
        zero_ties["input_centroids_sha256"] = sha(
            struct.pack("<8sQIIII", b"BORSUCP1", rows, 1, 32, 256, 0) +
            struct.pack("<e", 1.0) * units)
        for i, leaf in enumerate(zero_ties["leaves"]):
            leaf.update(group_ordinal=0, chunk_ordinal=i,
                prototype=[1.00000001 if i == 8 else 1.0],
                sha256=sha(zero_payload[leaf["offset"]:leaf["offset"] + leaf["bytes"]]))
        rounded_router, _, _, _ = load_router(router, write_router(zero_ties, members, zero_payload))
        assert nominate(cosine_query([1.0]), rounded_router["leaves"]) == list(range(16))
        changed = bytearray(membership)
        struct.pack_into("<I", changed, 0, 1)
        bad = json.loads(canonical(manifest))
        bad["membership"]["sha256"] = sha(changed)
        rejected(lambda: load_router(router, write_router(bad, changed)), "membership")
        changed = bytearray(payload)
        struct.pack_into("<I", changed, 6, 0)
        bad = json.loads(canonical(manifest))
        bad["leaf_payload"]["sha256"] = sha(changed)
        bad["leaves"][0]["sha256"] = sha(changed[:384])
        rejected(lambda: load_router(router, write_router(bad, membership, changed)), "partition")
        for bits in (0x7e00, 0x7c00):
            changed = bytearray(payload)
            struct.pack_into("<H", changed, 4, bits)
            bad = json.loads(canonical(manifest))
            bad["leaf_payload"]["sha256"] = sha(changed)
            bad["leaves"][0]["sha256"] = sha(changed[:384])
            rejected(lambda: load_router(router, write_router(bad, membership, changed)), "nonfinite")
        write_router()
        # Reject duplicate JSON keys even when the body was externally hashed.
        duplicate = canonical(manifest).replace(b'"rows":40929', b'"rows":40929,"rows":40929')
        (router / "manifest.json").write_bytes(duplicate)
        rejected(lambda: load_router(router, sha(duplicate)), "duplicate")
        write_router()
        for field, value, message in [("query_ordinal", 1, "ordinal"),
                                      ("query_ordinal", False, "ordinal"),
                                      ("query", [1e40], "f32"),
                                      ("query", [True], "number"),
                                      ("query", [0.0], "norm")]:
            first = dict(query_ordinal=0, query=[1.0])
            first[field] = value
            changed = canonical(first) + b"\n".join(query_body.splitlines()[1:])
            requests.write_bytes(changed)
            rejected(lambda: load_panel(requests, sha(changed), truth, sha(gold), rows, 1), message)
        for changed in [query_body.replace(b"[1.0]", b"[NaN]", 1),
                        query_body.replace(b"[1.0]", b"[1e999]", 1)]:
            requests.write_bytes(changed)
            rejected(lambda: load_panel(requests, sha(changed), truth, sha(gold), rows, 1), "nonfinite")
        requests.write_bytes(query_body[:-1].split(b"\n", 1)[1])
        rejected(lambda: load_panel(requests, sha(requests.read_bytes()), truth, sha(gold), rows, 1),
                 "1000")
        requests.write_bytes(query_body)
        for changed, message in [(gold[:-1], "length"),
                                 (struct.pack("<I", rows) + gold[4:], "truth ID"),
                                 (gold[:4] + gold[:4] + gold[8:], "duplicate")]:
            truth.write_bytes(changed)
            rejected(lambda: load_panel(requests, sha(query_body), truth, sha(changed), rows, 1), message)
        truth.write_bytes(gold)
        rejected(lambda: load_panel(requests, "0" * 64, truth, sha(gold), rows, 1), "SHA256")
        rejected(lambda: load_panel(requests, sha(query_body), truth, "0" * 64, rows, 1), "SHA256")
        rejected(lambda: load_order(order, "0" * 64, rows), "SHA256")

        # Fewer-than-eight real leaves, zero means, partial final unit/page.
        small = json.loads(canonical(manifest))
        small_rows, small_units = 101, 4
        small_members = struct.pack("<4I", 0, 0, 0, 0)
        small_payload = b"".join(struct.pack("<Ie", u, 0.0) for u in range(small_units))
        small.update(rows=small_rows, unit_count=small_units, final_unit_rows=5,
                     input_centroids_bytes=40,
                     input_centroids_sha256=sha(struct.pack("<8sQIIII", b"BORSUCP1", small_rows,
                         1, 32, 256, 0) + b"\0" * 8),
                     membership=dict(bytes=16, sha256=sha(small_members)),
                     leaf_payload=dict(bytes=24, sha256=sha(small_payload)),
                     leaves=[dict(leaf_id=0, group_ordinal=0, chunk_ordinal=0, offset=0,
                         bytes=24, unit_count=4, source_rows=101, sha256=sha(small_payload),
                         prototype=[0.0])])
        small["algorithm"].update(requested_centers=1, training_centers=1)
        small_sha = write_router(small, small_members, small_payload)
        small_gold = struct.pack("<100I", *range(100)) * 1000
        truth.write_bytes(small_gold)
        small_order = struct.pack("<101Q", *range(101))
        order.write_bytes(small_order)
        small_report = evaluate(router, small_sha, requests, sha(query_body), truth, sha(small_gold),
                                order, sha(small_order))
        assert small_report["records"][0]["selected_leaf_ids"] == [0]
        assert small_report["records"][0]["selected_source_rows"] == 101
        assert small_report["records"][0]["page_closure_rows"] == 101
        assert small_report["summary"]["selected_unit_truth100"]["mean"] == 1

        (router / "manifest.json").write_bytes(b" " * (ROOT_CAP + 1))
        rejected(lambda: load_router(router, sha(b" " * (ROOT_CAP + 1))), "cap")
    print("semantic router synthetic self-check: PASS")


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--self-check", action="store_true")
    for name in ("router-dir", "manifest-sha256", "requests-file", "requests-sha256",
                 "truth-file", "truth-sha256", "order-file", "order-sha256", "output-file"):
        parser.add_argument("--" + name)
    args = parser.parse_args(argv)
    values = [args.router_dir, args.manifest_sha256, args.requests_file, args.requests_sha256,
              args.truth_file, args.truth_sha256, args.order_file, args.order_sha256, args.output_file]
    if args.self_check:
        require(not any(value is not None for value in values), "self-check takes no input/output files")
        self_check()
        return 0
    if any(value is None for value in values):
        parser.error("all router, requests, truth, order, SHA256, and output arguments are required")
    require(not os.path.lexists(args.output_file), "output already exists")
    report = evaluate(*values[:-1])
    publish(args.output_file, report)
    print(json.dumps(dict(output_sha256=sha(canonical(report)), decision=report["gate"]["decision"]),
                     sort_keys=True))
    return 0 if report["gate"]["passed"] else 2


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (ValueError, OSError, RecursionError) as error:
        raise SystemExit("semantic router coverage rejected: " + str(error)) from None
