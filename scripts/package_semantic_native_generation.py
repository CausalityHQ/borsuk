#!/usr/bin/env python3
"""Package the frozen FIRST100k inputs; no fitting, encoding, queries or uploads.

Run --self-check for the small stdlib check. Production invocation:
  package_semantic_native_generation.py DATASET NEW_OUTPUT --repackager BINARY
The binary is the separately built repackage_semantic_generation CLI. All input
paths and body hashes come from the two exact committed authorities below.
"""
import argparse
import contextlib
import hashlib
import json
import os
from pathlib import Path
import shutil
import stat
import struct
import subprocess
import tempfile


REPO = Path(__file__).resolve().parents[1]
EVIDENCE = REPO / "docs/research/performance-architecture-20260930"
AUTHORITY = EVIDENCE / "semantic-router-native-import-authority.json"
AUTHORITY_SHA = "e5b2a50a796e95c9c8ede7c9c9b076c5dab95d4167d26713f3d7cdd103274cc8"
STAGING = EVIDENCE / "semantic-router-native-publication-staging.json"
STAGING_SHA = "d73af65e7ef650433dca4c38d6117df07dc3d5fbb6bda9940c1a0b4608a2d068"
SCRATCH = 65536
GRAPH_FIELDS = (
    "centroids_sha256", "graph_sha256", "graph_resident_bytes",
    "diverse_graph_sha256", "diverse_graph_resident_bytes",
)


def require(ok, message):
    if not ok:
        raise ValueError(message)


def sha(body):
    return hashlib.sha256(body).hexdigest()


def encoded(value):
    return json.dumps(value, separators=(",", ":"), allow_nan=False).encode()


def regular(path):
    # Opening a FIFO must never wait for a writer before the type check.
    fd = os.open(path, os.O_RDONLY | os.O_NONBLOCK | os.O_NOFOLLOW)
    file = os.fdopen(fd, "rb", buffering=0)
    if not stat.S_ISREG(os.fstat(fd).st_mode):
        file.close()
        raise ValueError(f"not a regular file: {path}")
    return file


def metadata(path, digest, cap=SCRATCH, length=None):
    with regular(path) as file:
        size = os.fstat(file.fileno()).st_size
        require(0 < size <= cap and (length is None or size == length), "metadata length/cap")
        body = file.read(size + 1)
    require(len(body) == size and sha(body) == digest, f"metadata identity: {path}")
    return body, json.loads(body)


def stream_copy(path, length, digest, destination=None, *, page_bytes=0,
                page_output=None, page_expected=None):
    """One 64 KiB buffer; optional streamed page SHA table (including the tail)."""
    require(length > 0 and page_bytes >= 0, "stream geometry")
    with contextlib.ExitStack() as stack:
        source = stack.enter_context(regular(path))
        require(os.fstat(source.fileno()).st_size == length, f"input length: {path}")
        target = stack.enter_context(open(destination, "xb", buffering=0)) if destination else None
        pages = stack.enter_context(open(page_output, "xb", buffering=0)) if page_output else None
        expected = stack.enter_context(regular(page_expected)) if page_expected else None
        require(not (pages or expected) or page_bytes > 0, "page geometry")
        buffer = bytearray(SCRATCH)
        whole, unit, table = hashlib.sha256(), hashlib.sha256(), hashlib.sha256()
        total = within = 0
        while total < length:
            count = source.readinto(memoryview(buffer)[:min(SCRATCH, length - total)])
            require(count, f"truncated input: {path}")
            chunk = memoryview(buffer)[:count]
            whole.update(chunk)
            if target:
                require(target.write(chunk) == count, "short copy write")
            total += count
            if page_bytes:
                offset = 0
                while offset < count:
                    end = min(count, offset + page_bytes - within)
                    unit.update(chunk[offset:end])
                    within += end - offset
                    offset = end
                    if within == page_bytes or (total == length and offset == count):
                        value = unit.digest()
                        table.update(value)
                        if pages:
                            require(pages.write(value) == 32, "short digest write")
                        if expected:
                            require(expected.read(32) == value, "page body/digest mismatch")
                        unit, within = hashlib.sha256(), 0
        require(source.read(1) == b"" and whole.hexdigest() == digest, f"input SHA256: {path}")
        if expected:
            require(expected.read(1) == b"", "extra page digests")
        for file in (target, pages):
            if file:
                os.fsync(file.fileno())
    return table.hexdigest() if page_bytes else digest


def write_new(path, body):
    with open(path, "xb") as file:
        file.write(body)
        file.flush()
        os.fsync(file.fileno())
    return {"bytes": len(body), "sha256": sha(body)}


def envelopes(root, plane, digest):
    require(root["schema"] == "borsuk-two-bit-generation-v4", "historical root schema")
    require(plane["schema"] == "borsuk-two-bit-plane-v2", "historical plane schema")
    require(plane["query_or_truth_used"] is False, "source-only provenance")
    current_plane = dict(plane, schema="borsuk-two-bit-plane-v3", page_rows=32,
                         page_digest_sha256=digest)
    plane_body = encoded(current_plane)
    current = dict(root)
    current["discovery"] = {"mode": "graph", **{key: current.pop(key) for key in GRAPH_FIELDS}}
    current["schema"] = "borsuk-two-bit-generation-v7"
    current["plane_manifest_sha256"] = sha(plane_body)
    root_body = encoded(current)
    require(len(root_body) <= SCRATCH and len(plane_body) <= SCRATCH, "envelope cap")
    return root_body, plane_body


def package(dataset, output, binary):
    authority_body, authority = metadata(AUTHORITY, AUTHORITY_SHA)
    staging_body, staging = metadata(STAGING, STAGING_SHA)
    require(staging["import_authority_sha256"] == AUTHORITY_SHA, "staging provenance")
    entry, staged = authority["datasets"][dataset], staging["datasets"][dataset]
    inputs = entry["authenticated_inputs"]

    def load(name, cap=SCRATCH):
        item = inputs[name]
        return metadata(item["path"], item["sha256"], cap, item["bytes"])

    original_root, root = load("root")
    original_plane, plane = load("plane_manifest")
    original_router, router = load("router_manifest", 1024 * 1024)
    rows, dimensions = plane["rows"], plane["dimensions"]
    require((rows, dimensions, plane["record_bytes"]) == (100000, 768, 200), "frozen geometry")
    require(root["canonical"] == entry["canonical"], "canonical authority")
    require(root["canonical"]["rows"] == rows and root["canonical"]["dimensions"] == dimensions,
            "canonical geometry")
    for key, name in (("plane_manifest_sha256", "plane_manifest"),
                      ("centroids_sha256", "centroids"), ("sq8_object_sha256", "sq8")):
        require(root[key] == inputs[name]["sha256"], f"root binding: {key}")
    for key, name in (("records_sha256", "plane_records"), ("mean_sha256", "plane_mean"),
                      ("source_order_sha256", "order"), ("sq8_sha256", "sq8")):
        require(plane[key] == inputs[name]["sha256"], f"plane binding: {key}")
    require(plane["source_sha256"] == entry["plane_source_sha256"], "source provenance")
    require(entry["original_router_source_identity"] == {
        "input_schema": root["schema"], "input_root_sha256": inputs["root"]["sha256"],
        "input_centroids_sha256": inputs["centroids"]["sha256"]}, "original identity")
    for key, value in entry["original_router_source_identity"].items():
        require(router[key] == value, f"router provenance: {key}")
    require((router["rows"], router["dimensions"]) == (rows, dimensions), "router geometry")
    for key, name in (("membership", "router_membership"), ("leaf_payload", "router_leaves")):
        require(router[key]["sha256"] == inputs[name]["sha256"]
                and router[key]["bytes"] == inputs[name]["bytes"], "router payload authority")
    require(sha(encoded({key: root[key] for key in ("low", "step")})) ==
            entry["sq8_codec_tables_sha256"], "codec table authority")
    page_item = staged["page_manifest"]
    page_body, page = metadata(page_item["path"], page_item["sha256"], length=page_item["bytes"])
    require(root["page_manifest_sha256"] == sha(page_body), "page manifest binding")
    require(page["schema"] == "borsuk-v115-sq8-page-authority-v2"
            and (page["rows"], page["dimensions"], page["page_rows"], page["generation"]) ==
            (rows, dimensions, 256, root["generation"]), "page geometry")
    require(page["object_sha256"] == plane["sq8_sha256"]
            and page["page_digest_sha256"] == staged["page_digests"]["sha256"], "page authority")
    require(staged["canonical"]["sha256"] == root["canonical"]["sha256"]
            and staged["canonical"]["bytes"] == rows * (dimensions * 4 + 8)
            and staged["canonical"]["key"] == root["canonical"]["object_key"], "canonical binding")
    for name, length in {"plane_records": rows * 200, "plane_mean": dimensions * 4,
                         "order": rows * 8, "sq8": rows * (dimensions + 12),
                         "centroids": 32 + ((rows + 31) // 32) * dimensions * 2}.items():
        require(inputs[name]["bytes"] == length, f"input geometry: {name}")
    require(staged["page_digests"]["bytes"] == ((rows + 255) // 256) * 32, "page table length")
    binary = Path(binary).resolve(strict=True)
    with regular(binary):
        pass
    output = Path(output).absolute()
    output.mkdir()  # Exclusive creation, including rejection of dangling symlinks.
    try:
        graph, proof = output / "graph", output / "provenance"
        (graph / "plane").mkdir(parents=True)
        (proof / "router").mkdir(parents=True)
        roster = {}

        def copy(item, relative, **kwargs):
            path = output / relative
            stream_copy(item["path"], item["bytes"], item["sha256"], path, **kwargs)
            roster[relative] = {"bytes": item["bytes"], "sha256": item["sha256"]}

        for name, relative in (("plane_mean", "graph/plane/mean.bin"),
                               ("order", "order.u64"), ("centroids", "graph/centroids.bin"),
                               ("router_membership", "provenance/router/membership.bin"),
                               ("router_leaves", "provenance/router/leaves.bin")):
            copy(inputs[name], relative)
        for name in ("canonical", "page_digests"):
            copy(staged[name], f"graph/{name}.bin")
        copy(inputs["sq8"], "sq8.bin", page_bytes=256 * (dimensions + 12),
             page_expected=graph / "page_digests.bin")
        records = inputs["plane_records"]
        unit_sha = stream_copy(records["path"], records["bytes"], records["sha256"],
                               graph / "plane/records.bin", page_bytes=32 * plane["record_bytes"],
                               page_output=graph / "plane/page_digests.bin")
        roster["graph/plane/records.bin"] = {key: records[key] for key in ("bytes", "sha256")}
        roster["graph/plane/page_digests.bin"] = {"bytes": ((rows + 31) // 32) * 32, "sha256": unit_sha}
        old_dir = Path(inputs["root"]["path"]).parent
        for name in ("graph", "diverse_graph"):
            path = old_dir / f"{name}.bin"
            with regular(path) as file:
                length = os.fstat(file.fileno()).st_size
            require(0 < length <= 8 * 1024 * 1024, "graph cap")
            copy({"path": str(path), "bytes": length, "sha256": root[f"{name}_sha256"]},
                 f"graph/{name}.bin")
        root_body, plane_body = envelopes(root, plane, unit_sha)
        for relative, body in (("provenance/original-root.json", original_root),
                               ("provenance/original-plane.json", original_plane),
                               ("provenance/router/manifest.json", original_router),
                               ("provenance/import-authority.json", authority_body),
                               ("provenance/staging-authority.json", staging_body),
                               ("graph/page_manifest.json", page_body),
                               ("graph/plane/manifest.json", plane_body),
                               ("graph/manifest.json", root_body)):
            roster[relative] = write_new(output / relative, body)
        command = [str(binary), str(graph), sha(root_body), str(output / "sq8.bin"),
                   str(proof), inputs["root"]["sha256"], inputs["router_manifest"]["sha256"],
                   str(output / "semantic")]
        # The helper authenticates current graph, original proof, SQ8-derived
        # centroid identity and complete router partition before writing its root.
        with open(output / "repackager.stdout.json", "xb") as stdout, \
                open(output / "repackager.stderr.log", "xb") as stderr:
            completed = subprocess.run(command, stdout=stdout, stderr=stderr, check=False)
        with regular(output / "repackager.stderr.log") as file:
            error_body = file.read(SCRATCH + 1)
        require(completed.returncode == 0,
                f"repackager exit {completed.returncode}: {error_body[:SCRATCH].decode(errors='replace')}")
        require(len(error_body) <= SCRATCH, "repackager stderr cap")
        with regular(output / "repackager.stdout.json") as file:
            result_body = file.read(SCRATCH + 1)
        require(len(result_body) <= SCRATCH, "repackager result cap")
        result = json.loads(result_body)
        semantic_body, semantic = metadata(output / "semantic/manifest.json", result["root_sha256"])
        require(result["root_bytes"] == len(semantic_body), "semantic root size")
        require(semantic["schema"] == "borsuk-two-bit-generation-v7"
                and semantic["discovery"]["mode"] == "semantic", "semantic discovery")
        for key in ("low", "step"):
            require(len(semantic[key]) == dimensions and all(
                struct.pack("<f", a) == struct.pack("<f", b)
                for a, b in zip(root[key], semantic[key])), "codec f32 bits changed")
        # Verify every copied candidate file without hydrating the source bodies.
        for relative, item in list(roster.items()):
            if relative.startswith("graph/") and relative not in (
                    "graph/manifest.json", "graph/graph.bin", "graph/diverse_graph.bin"):
                candidate = relative.replace("graph/", "semantic/", 1)
                stream_copy(output / candidate, item["bytes"], item["sha256"])
                roster[candidate] = dict(item)
        for name in ("manifest.json", "membership.bin", "leaves.bin"):
            item = roster[f"provenance/router/{name}"]
            stream_copy(output / f"semantic/router/{name}", item["bytes"], item["sha256"])
            roster[f"semantic/router/{name}"] = dict(item)
        roster["semantic/manifest.json"] = {"bytes": len(semantic_body), "sha256": sha(semantic_body)}
        receipt = {
            "schema": "borsuk-semantic-native-packaging-v1", "dataset": dataset,
            "import_authority_sha256": AUTHORITY_SHA, "staging_authority_sha256": STAGING_SHA,
            "original_router_source_identity": entry["original_router_source_identity"],
            "graph_root_sha256": sha(root_body), "semantic_root_sha256": sha(semantic_body),
            "changed_envelopes": ["graph/manifest.json", "graph/plane/manifest.json",
                                  "semantic/manifest.json"],
            "new_authorities": ["graph/plane/page_digests.bin"],
            "payloads_transformed": False, "queries_or_truth_opened": False,
            "benchmark_measurement": False, "copy_scratch_bytes": SCRATCH,
            "repackager_argv": command, "repackager_status": completed.returncode,
            "repackager_stdout_sha256": sha(result_body), "repackager_stderr_sha256": sha(error_body),
            "repackager_result": result, "files": roster,
        }
        write_new(output / "receipt.json", encoded(receipt) + b"\n")
        return receipt
    except BaseException:
        shutil.rmtree(output)
        raise


def self_check():
    with tempfile.TemporaryDirectory() as temporary:
        directory = Path(temporary)
        body = bytes(range(251)) * 900 + b"tail"
        source, target, digests = (directory / name for name in ("source", "copy", "digests"))
        source.write_bytes(body)
        digest = stream_copy(source, len(body), sha(body), target, page_bytes=6400, page_output=digests)
        expected = b"".join(hashlib.sha256(body[n:n + 6400]).digest() for n in range(0, len(body), 6400))
        require(target.read_bytes() == body and digests.read_bytes() == expected
                and digest == sha(expected), "stream byte identity/tail digest")
        stream_copy(source, len(body), sha(body), page_bytes=6400, page_expected=digests)
        wide = directory / "wide-page-digests"
        stream_copy(source, len(body), sha(body), page_bytes=199680, page_output=wide)
        require(wide.read_bytes() == hashlib.sha256(body[:199680]).digest()
                + hashlib.sha256(body[199680:]).digest(), "page wider than copy scratch")
        stream_copy(source, len(body), sha(body), page_bytes=199680, page_expected=wide)
        plane = {"schema": "borsuk-two-bit-plane-v2", "seed": 20260923, "query_or_truth_used": False}
        root = {"schema": "borsuk-two-bit-generation-v4", "plane_manifest_sha256": sha(encoded(plane)),
                "low": [-0.0, 0.10000000149011612], "step": [0.0010000000474974513, 1.0],
                **{key: 7 for key in GRAPH_FIELDS}}
        new_root, new_plane = map(json.loads, envelopes(root, plane, digest))
        require(new_root["discovery"] == {"mode": "graph", **{k: root[k] for k in GRAPH_FIELDS}},
                "graph envelope")
        require(new_plane == dict(plane, schema="borsuk-two-bit-plane-v3", page_rows=32,
                                   page_digest_sha256=digest), "plane envelope/seed")
        for key in ("low", "step"):
            require(b"".join(struct.pack("<f", v) for v in root[key]) ==
                    b"".join(struct.pack("<f", v) for v in new_root[key]), "exact codec bits")
        envelope = directory / "envelope"
        envelope.write_bytes(encoded(root))
        metadata(envelope, sha(encoded(root)))
        fifo = directory / "fifo"
        os.mkfifo(fifo)
        for action in (
            lambda: metadata(envelope, "0" * 64),
            lambda: stream_copy(source, len(body) - 1, sha(body)),
            lambda: stream_copy(source, len(body), "0" * 64),
            lambda: stream_copy(source, len(body), sha(body), target),
            lambda: regular(fifo),
            lambda: envelopes(dict(root, schema="wrong"), plane, digest),
        ):
            try:
                action()
            except (ValueError, FileExistsError):
                pass
            else:
                raise AssertionError("tamper/cap/existing-output rejection missing")
        digests.write_bytes(b"\0" * len(expected))
        try:
            stream_copy(source, len(body), sha(body), page_bytes=6400, page_expected=digests)
        except ValueError:
            pass
        else:
            raise AssertionError("page tamper accepted")
    print("self-check: envelope, partial unit digest, tamper, FIFO and byte identity PASS")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("dataset", nargs="?", choices=("relaion", "cohere"))
    parser.add_argument("output", nargs="?", type=Path)
    parser.add_argument("--repackager", type=Path)
    parser.add_argument("--self-check", action="store_true")
    args = parser.parse_args()
    if args.self_check:
        self_check()
    else:
        if not (args.dataset and args.output and args.repackager):
            parser.error("DATASET NEW_OUTPUT --repackager BINARY are required")
        receipt = package(args.dataset, args.output, args.repackager)
        print(json.dumps({key: receipt[key] for key in ("graph_root_sha256", "semantic_root_sha256")}))


if __name__ == "__main__":
    main()
