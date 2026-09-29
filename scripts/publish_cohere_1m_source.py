"""Publish and independently hash the completed source-only CoHere v4 artifacts."""

import hashlib
import json
from pathlib import Path
import subprocess
import sys
import struct

from build_cohere_1m_source import BUCKET, PREFIX, digest


AWS = ["aws", "--profile", "causality", "--region", "eu-central-1"]


def publish(path, key):
    ident = dict(key=key, bytes=path.stat().st_size, sha256=digest(path))
    head = subprocess.run(AWS + ["s3api", "head-object", "--bucket", BUCKET, "--key", key],
                          capture_output=True, text=True)
    if head.returncode:
        if "404" not in head.stderr:
            head.check_returncode()
        subprocess.run(AWS + ["s3api", "put-object", "--bucket", BUCKET, "--key", key,
                              "--body", str(path), "--if-none-match", "*", "--metadata",
                              "sha256=" + ident["sha256"]], check=True,
                       stdout=subprocess.DEVNULL)
    process = subprocess.Popen(AWS + ["s3", "cp", "s3://" + BUCKET + "/" + key,
                                      "-", "--only-show-errors"], stdout=subprocess.PIPE)
    h, size = hashlib.sha256(), 0
    for block in iter(lambda: process.stdout.read(8 << 20), b""):
        h.update(block)
        size += len(block)
    process.stdout.close()
    assert process.wait() == 0
    assert (size, h.hexdigest()) == (ident["bytes"], ident["sha256"]), key
    return ident


def main():
    work = Path(sys.argv[1])
    report = json.loads((work / "source-build.json").read_text())
    controller = Path(__file__).with_name("build_cohere_1m_source.py")
    assert digest(controller) == report["controller_sha256"]
    assert report["query_or_truth_used"] is False and report["quality_measured"] is False
    assert "Exit status: 0" in (work / "process-resources.txt").read_text()
    generation = work / "generation"
    manifest = generation / "manifest.json"
    assert digest(manifest) == report["root_sha256"]
    root = json.loads(manifest.read_text())
    assert root["schema"] == "borsuk-two-bit-generation-v4"
    assert (root["generation"], root["base_epoch"]) == (1, 0)
    for name, field in [("centroids.bin", "centroids_sha256"),
                        ("graph.bin", "graph_sha256"),
                        ("diverse_graph.bin", "diverse_graph_sha256"),
                        ("page_manifest.json", "page_manifest_sha256"),
                        ("plane/manifest.json", "plane_manifest_sha256")]:
        assert digest(generation / name) == root[field], name
    plane = json.loads((generation / "plane/manifest.json").read_text())
    assert (plane["rows"], plane["dimensions"], plane["record_bytes"]) == (1_000_000, 768, 200)
    assert plane["source_sha256"] == report["source_sha256"]
    assert plane["sq8_sha256"] == report["sq8_sha256"]
    assert plane["source_order_sha256"] == report["order_sha256"]
    assert plane["query_or_truth_used"] is False
    assert digest(generation / "plane/mean.bin") == plane["mean_sha256"]
    assert digest(generation / "plane/records.bin") == plane["records_sha256"]
    assert (generation / "plane/records.bin").stat().st_size == 200_000_000
    page = json.loads((generation / "page_manifest.json").read_text())
    assert (page["rows"], page["dimensions"], page["page_rows"]) == (1_000_000, 768, 256)
    assert page["object_sha256"] == root["sq8_object_sha256"] == report["sq8_sha256"]
    assert digest(generation / "page_digests.bin") == page["page_digest_sha256"]
    sq8_receipt = json.loads((work / "sq8.json").read_text())
    for field in ["low", "step"]:
        assert struct.pack("<768f", *root[field]) == struct.pack("<768f", *sq8_receipt[field])
    canonical = generation / "canonical.bin"
    assert (root["canonical"]["rows"], root["canonical"]["dimensions"],
            root["canonical"]["bytes"]) == (1_000_000, 768, 3_080_000_000)
    assert (canonical.stat().st_size, digest(canonical)) == (
        root["canonical"]["bytes"], root["canonical"]["sha256"])
    assert root["sq8_object_key"] == report["sq8_object_key"]
    assert all(root_key.startswith(PREFIX + "/objects/") for root_key in
               [root["sq8_object_key"], root["canonical"]["object_key"]])
    artifacts = {"sq8.bin": publish(work / "sq8.bin", root["sq8_object_key"]),
                 "generation/canonical.bin": publish(canonical, root["canonical"]["object_key"])}
    for path in sorted(generation.rglob("*")):
        if path.is_file() and path != canonical:
            name = "generation/" + path.relative_to(generation).as_posix()
            artifacts[name] = publish(path, PREFIX + "/artifacts/" + name)
    for name in ["order.u64", "normalize.json", "hier-fit.json", "sq8.json", "builder.json",
                 "build.stdout", "source-build.json", "process-resources.txt"]:
        artifacts[name] = publish(work / name, PREFIX + "/artifacts/" + name)
    artifacts["build_cohere_1m_source.py"] = publish(
        controller, PREFIX + "/artifacts/build_cohere_1m_source.py")
    assert artifacts["build_cohere_1m_source.py"]["sha256"] == report["controller_sha256"]
    terminal = dict(schema="borsuk-cohere-source-1m-v4-terminal-v1", status="complete",
                    exit_code=0, source_commit=report["source_commit"],
                    root_sha256=report["root_sha256"], artifacts=artifacts,
                    remote_bodies_independently_hashed=True, query_or_truth_used=False,
                    quality_measured=False, ec2_instance=None)
    path = work / "terminal.json"
    path.write_text(json.dumps(terminal, indent=2) + "\n")
    publish(path, PREFIX + "/terminal.json")
    print(json.dumps(dict(status="complete", root_sha256=report["root_sha256"],
                         terminal_sha256=digest(path), artifact_count=len(artifacts))))


if __name__ == "__main__":
    assert len(sys.argv) == 2
    main()
