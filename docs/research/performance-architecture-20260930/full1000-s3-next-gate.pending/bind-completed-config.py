#!/usr/bin/env python3
"""Exact native experiment glue. Runtime/data execution is causality EC2 only.
AUTHORITY AUTHORITY_SHA NEW_COMPLETED_CONFIG. This creates no quality claim.
The root authenticates archive extraction/closure/component provenance separately.
"""
import hashlib
import json
import os
import pathlib
import re
import stat
import sys


def require(ok, reason):
    if not ok:
        raise ValueError(reason)


def pairs(items):
    result = {}
    for key, value in items:
        require(key not in result, "duplicate JSON key")
        result[key] = value
    return result


def decode(raw):
    return json.loads(raw, object_pairs_hook=pairs,
                      parse_constant=lambda _: (_ for _ in ()).throw(ValueError("nonfinite JSON")))


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()


def stamp(s):
    return s.st_dev, s.st_ino, s.st_size, s.st_mtime_ns, s.st_ctime_ns


def read_pin(pin, maximum, capture=False, headers=False):
    require(set(pin) == {"path", "bytes", "sha256"}, "artifact fields")
    require(type(pin["bytes"]) is int and 0 < pin["bytes"] <= maximum, "artifact cap")
    require(isinstance(pin["sha256"], str) and re.fullmatch("[0-9a-f]{64}", pin["sha256"]), "artifact SHA")
    path = pathlib.Path(pin["path"])
    require(path.is_absolute() and str(path.resolve(strict=True)) == str(path), "artifact path")
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    try:
        before = os.fstat(fd)
        require(stat.S_ISREG(before.st_mode) and before.st_nlink == 1 and before.st_size == pin["bytes"], "artifact regular/length/link")
        digest = hashlib.sha256()
        kept = bytearray()
        total = 0
        while True:
            block = os.read(fd, 65536)
            if not block:
                break
            total += len(block)
            require(total <= pin["bytes"], "artifact growth")
            digest.update(block)
            if capture:
                kept.extend(block)
            elif headers and kept.count(b"\n") < 2:
                kept.extend(block)
                require(len(kept) <= 196608, "header cap")
        require(total == pin["bytes"] and digest.hexdigest() == pin["sha256"], "artifact SHA/EOF")
        require(stamp(before) == stamp(os.fstat(fd)) == stamp(os.stat(path, follow_symlinks=False)), "artifact drift")
        if headers:
            lines = kept.split(b"\n")
            require(len(lines) >= 3 and all(0 < len(x) <= 65536 for x in lines[:2]), "two bounded native headers")
            return [decode(x) for x in lines[:2]]
        return decode(kept) if capture else None
    finally:
        os.close(fd)


def main():
    require(len(sys.argv) == 4, "usage: AUTHORITY AUTHORITY_SHA NEW_COMPLETED_CONFIG")
    source, sha, target = sys.argv[1:]
    require(re.fullmatch("[0-9a-f]{64}", sha), "authority SHA")
    size = os.stat(source, follow_symlinks=False).st_size
    a = read_pin({"path": source, "bytes": size, "sha256": sha}, 65536, capture=True)
    require(set(a) == {"schema", "config", "result", "cohort", "binary_sha256", "sources"}, "authority fields")
    require(a["schema"] == "borsuk-completed-header-authority-v1", "authority schema")
    require(a["binary_sha256"] == "59fe47aa1001b3ca24d1f9ff31444f97fcda72e3e297c8d7d846f5c3d811bfc3", "original baseline binary")
    names = {"runner_source_sha256", "generation_source_sha256", "router_source_sha256", "codec_source_sha256",
             "source_plane_source_sha256", "sq8_range_source_sha256", "returned_source_sha256"}
    require(set(a["sources"]) == names and all(isinstance(v, str) and re.fullmatch("[0-9a-f]{64}", v) for v in a["sources"].values()), "source identity fields")
    c = read_pin(a["config"], 65536, capture=True)
    receipt = read_pin(a["cohort"], 65536, capture=True)
    require(c["schema"] == "borsuk-cohere-native-baseline-config-v7", "native config schema")
    require(c["rows"] == 1000000 and c["dimensions"] == 1024 and c["k"] == 10 and c["profile"] == "scale1m", "frozen geometry")
    require(c["count"] in (32, 1000), "historical/full count")
    require({k: c["cohort_receipt"][k] for k in ("bytes", "sha256")} == {k: a["cohort"][k] for k in ("bytes", "sha256")}, "cohort binding")
    require(receipt["status"] == "COMPLETE" and receipt["dataset"] == c["dataset"] and receipt["revision"] == c["revision"], "cohort identity")
    reserved = receipt["reserved_queries_sha256"]
    require(isinstance(reserved, str) and re.fullmatch("[0-9a-f]{64}", reserved), "reserved query seal")
    identity = {"schema": "borsuk-cohere-native-baseline-result-v7", "phase": "identity",
                "config_sha256": a["config"]["sha256"], "fetch_parallelism": c["fetch_parallelism"],
                "serving": c["serving"], "binary_sha256": a["binary_sha256"],
                "scope": "AUTHENTICATED_NATIVE_QUALITY_CORRECTNESS", "physical_s3_measured": False,
                "io_measurement": "logical_GET_charges_separate_from_cumulative_process_native_transport",
                "s3_credential_source": "imds_instance_role_only", "native_transport_includes": "S3_and_IMDS_credential_requests_including_PUT",
                "wire_bytes": None, "unread_bytes": None, "billed_bytes": None, "billed_requests": None,
                "external_gate_required": True, "truth_opened": False, "execution": c["execution"], **a["sources"]}
    fields = ("dataset", "revision", "fetch_parallelism", "serving", "metric", "tie_rule", "rows", "dimensions", "count", "k",
              "corpus_source_first", "query_source_first", "profile", "backend", "generation_prefix", "generation_root_sha256",
              "corpus_intervals", "reserved_query_interval", "producer_authority", "max_memory_bytes", "execution")
    bound = {k: c[k] for k in fields}
    bound.update(phase="bound_inputs", source_cache="off", truth_opened=False,
                 credential_source="imds_instance_role_only" if c["backend"]["kind"] == "s3" else None,
                 reserved_queries_sha256=reserved, cohort_receipt_sha256=c["cohort_receipt"]["sha256"],
                 derivation_receipt_sha256=c["derivation_receipt"]["sha256"],
                 selected_count=c["count"] if c["execution"]["mode"] == "full" else len(c["execution"]["ordinals"]))
    for role in ("requests", "truth"):
        for field in ("bytes", "sha256"):
            bound[role + "_" + field] = c[role][field]
    for dest, src in (("native_source_sha256", "source_sha256"), ("native_sq8_sha256", "sq8_sha256"), ("native_order_sha256", "source_order_sha256")):
        bound[dest] = c["native_source"][src]
    observed = read_pin(a["result"], 33554432, headers=True)
    require(canonical(observed) == canonical([identity, bound]), "native headers differ from independent authority")
    value = {"schema": "borsuk-completed-native-reduction-config-v4", "input": a["result"],
             "expected_identity": identity, "expected_bound_inputs": bound}
    raw = canonical(value) + b"\n"
    require(len(raw) <= 32768, "completed config cap")
    parent = pathlib.Path(target).parent
    require(pathlib.Path(target).is_absolute() and str(parent.resolve(strict=True)) == str(parent), "output parent")
    fd = os.open(target, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o400)
    try:
        with os.fdopen(fd, "wb", closefd=False) as stream:
            stream.write(raw)
            stream.flush()
        os.fsync(fd)
    finally:
        os.close(fd)
    directory = os.open(parent, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        os.fsync(directory)
    finally:
        os.close(directory)
    print(hashlib.sha256(raw).hexdigest())


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        print("INVALID header binding: " + str(error), file=sys.stderr)
        sys.exit(98)
