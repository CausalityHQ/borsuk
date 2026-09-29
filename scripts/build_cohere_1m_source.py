"""Build the current v4 source generation from the sealed CoHere first1M raw rows."""

import hashlib
import json
from pathlib import Path
import struct
import subprocess
import sys


ROWS = 1_000_000
DIMS = 768
SOURCE_SHA = "6c82a340e3e1b4226640e593efa9c4000c6a5962d4b13063093a1dab689a9005"
BUCKET = "borsuk-bench-453182569524-euc1"
PREFIX = "research/native-union/20260929/cohere-source-1m-v4"


def digest(path):
    h = hashlib.sha256()
    with path.open("rb") as f:
        for block in iter(lambda: f.read(8 << 20), b""):
            h.update(block)
    return h.hexdigest()


def run_once(work, name, output, command):
    if output.exists():
        assert (work / (name + ".json")).exists(), name
        return json.loads((work / (name + ".json")).read_text())
    result = subprocess.run([str(x) for x in command], capture_output=True, text=True)
    (work / (name + ".stderr")).write_text(result.stderr)
    result.check_returncode()
    receipt = json.loads(result.stdout)
    assert output.exists() and output.stat().st_size > 0, name
    (work / (name + ".json")).write_text(json.dumps(receipt, indent=2) + "\n")
    return receipt


def main():
    work, binaries = map(Path, sys.argv[1:])
    assert work.is_dir() and binaries.is_dir()
    raw = work / "source.raw"
    assert raw.stat().st_size == ROWS * DIMS * 4 and digest(raw) == SOURCE_SHA
    fit = binaries / "build_sq8_source"
    build = binaries / "build_two_bit_generation"
    assert fit.is_file() and build.is_file()
    normalized = work / "normalized.raw"
    norm = run_once(work, "normalize", normalized,
                    [fit, "normalize", raw, SOURCE_SHA, ROWS, DIMS, 1 << 30, normalized])
    assert normalized.stat().st_size == raw.stat().st_size
    normalized_sha = digest(normalized)
    assert norm["normalized_sha256"] == normalized_sha
    order = work / "order.u64"
    recipe = run_once(work, "hier-fit", order,
                      [fit, "hier-fit", normalized, normalized_sha, ROWS, DIMS, 1 << 30, order])
    assert recipe["query_or_truth_used"] is False
    assert recipe["recipe"] == "borsuk-hierarchical-extents-chacha8-v3"
    assert order.stat().st_size == ROWS * 8 and recipe["order_sha256"] == digest(order)
    seen = bytearray(ROWS)
    with order.open("rb") as f:
        for raw_id in iter(lambda: f.read(8), b""):
            assert len(raw_id) == 8
            row = struct.unpack("<Q", raw_id)[0]
            assert row < ROWS and not seen[row]
            seen[row] = 1
    assert all(seen)
    extents = recipe["extents"]
    assert extents[0][0] == 0 and extents[-1][1] == ROWS
    assert all(0 < b - a <= 1024 for a, b in extents)
    assert all(a[1] == b[0] for a, b in zip(extents, extents[1:]))
    sq8 = work / "sq8.bin"
    sq8_receipt = run_once(work, "sq8", sq8,
                           [fit, normalized, normalized_sha, DIMS, order,
                            recipe["order_sha256"], 1 << 30, sq8])
    sq8_sha = digest(sq8)
    assert sq8.stat().st_size == ROWS * 780 and sq8_receipt["sq8_sha256"] == sq8_sha
    key = PREFIX + "/objects/" + sq8_sha
    aws = ["aws", "--profile", "causality", "--region", "eu-central-1"]
    head = subprocess.run(aws + ["s3api", "head-object", "--bucket", BUCKET, "--key", key],
                          capture_output=True, text=True)
    if head.returncode:
        if "404" not in head.stderr:
            head.check_returncode()
        subprocess.run(aws + ["s3api", "put-object", "--bucket", BUCKET, "--key", key,
                              "--body", str(sq8), "--if-none-match", "*", "--metadata",
                              "sha256=" + sq8_sha], check=True, stdout=subprocess.DEVNULL)
        head = subprocess.run(aws + ["s3api", "head-object", "--bucket", BUCKET, "--key", key],
                              check=True, capture_output=True, text=True)
    object_head = json.loads(head.stdout)
    assert object_head["ContentLength"] == ROWS * 780
    assert object_head["Metadata"]["sha256"] == sq8_sha
    builder = dict(raw=str(raw), raw_sha256=SOURCE_SHA, sq8=str(sq8), sq8_sha256=sq8_sha,
                   rows=ROWS, dimensions=DIMS, generation=1, base_epoch=0,
                   low=sq8_receipt["low"], step=sq8_receipt["step"],
                   sq8_object_key=key, sq8_etag=object_head["ETag"])
    builder_path = work / "builder.json"
    builder_path.write_text(json.dumps(builder, indent=2) + "\n")
    generation = work / "generation"
    assert not generation.exists(), "generation already exists; inspect the original build"
    result = subprocess.run([str(build), str(builder_path), digest(builder_path),
                             str(2 << 30), str(generation)], capture_output=True, text=True)
    (work / "build.stderr").write_text(result.stderr)
    result.check_returncode()
    (work / "build.stdout").write_text(result.stdout)
    manifest = generation / "manifest.json"
    assert result.stdout.strip() == digest(manifest)
    root = json.loads(manifest.read_text())
    assert root["schema"] == "borsuk-two-bit-generation-v4"
    assert root["sq8_object_key"] == key and root["sq8_etag"] == object_head["ETag"]
    assert json.loads((generation / "page_manifest.json").read_text())["rows"] == ROWS
    report = dict(schema="borsuk-cohere-source-1m-v4-build-v1", source_sha256=SOURCE_SHA,
                  normalized_sha256=normalized_sha, order_sha256=digest(order),
                  sq8_sha256=sq8_sha, root_sha256=digest(manifest),
                  builder_sha256=digest(builder_path), sq8_object_key=key,
                  sq8_object_etag=object_head["ETag"], query_or_truth_used=False,
                  quality_measured=False, controller_sha256=digest(Path(__file__)),
                  source_commit=subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip(),
                  binaries={p.name: digest(p) for p in [fit, build]})
    (work / "source-build.json").write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps(report))


if __name__ == "__main__":
    assert len(sys.argv) == 3
    main()
