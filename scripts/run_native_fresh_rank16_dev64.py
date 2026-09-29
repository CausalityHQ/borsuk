"""One frozen fresh ReLAION dev64 native quality and offered HTTP gate on AWS."""

import atexit
import json
import os
from pathlib import Path
import resource
import struct
import subprocess
import sys

import numpy as np

from scripts.run_native_current_1m_offered_http import native, run, sha, write


def prefix_object(bucket, ident, path, length):
    args = ["aws", "s3api"]
    head = json.loads(subprocess.check_output(args + ["head-object", "--bucket", bucket,
                                                "--key", ident["key"]]))
    if head["ContentLength"] != ident["bytes"] or head["Metadata"]["sha256"] != ident["sha256"]:
        raise ValueError("sealed whole-object identity differs")
    subprocess.run(args + ["get-object", "--bucket", bucket, "--key", ident["key"],
                           "--range", f"bytes=0-{length-1}", "--if-match", head["ETag"],
                           str(path)], check=True, stdout=subprocess.DEVNULL)
    if path.stat().st_size != length:
        raise ValueError("sealed development byte range differs")
    return sha(path)


def main():
    config_path, expected_sha, out_arg, binaries_arg, prefix = sys.argv[1:]
    out, binaries = Path(out_arg), Path(binaries_arg)
    if sha(config_path) != expected_sha:
        raise ValueError("frozen fresh development config differs")
    config = json.loads(Path(config_path).read_text())
    if (config["schema"] != "borsuk-fresh-rank16-1m-dev64-v1"
            or (config["rows"], config["dimensions"], config["count"], config["offered_qps"])
            != (1_000_000, 768, 64, 8)
            or config["setting_order"] != [10, 100, 100, 10]
            or config["gates"] != {"native_mean_recall_at_10_minimum": .95,
                                   "incoming_http_p90_ms_exclusive_maximum": 444,
                                   "successful_qps_minimum": 8,
                                   "all_offered_success": True}
            or config["prospective_ordinals_sealed"] != [64, 999]
            or config["fresh_cohort_used"] is not True
            or np.__version__ != "2.3.3"
            or os.environ["BORSUK_NATIVE_MEMORY_BYTES"] != "1073741824"):
        raise ValueError("frozen fresh development scope differs")
    for name, expected in config["code_sha256"].items():
        if sha(name) != expected:
            raise ValueError(f"source/scorer/protocol differs: {name}")
    for name, ident in config["binaries"].items():
        path = binaries / name
        if path.stat().st_size != ident["bytes"] or sha(path) != ident["sha256"]:
            raise ValueError(f"qualified native binary differs: {name}")
    out.mkdir()
    config["measurement_prefix"] = prefix

    def resources():
        group = Path("/sys/fs/cgroup") / Path("/proc/self/cgroup").read_text().strip().split("0::")[-1].lstrip("/")
        write(out / "cgroup.json", {"cgroup": {key: (group / key).read_text() for key in
                                     ("memory.max", "memory.peak", "memory.swap.max",
                                      "memory.swap.peak", "memory.events", "cpu.stat")
                                     if (group / key).exists()},
                                     "address_space_limit": list(resource.getrlimit(resource.RLIMIT_AS)),
                                     "cpu_affinity": sorted(os.sched_getaffinity(0))})
    atexit.register(resources)
    root = config["root_manifest"]
    generation = out / "generation"
    generation.mkdir()
    subprocess.run(["aws", "s3", "cp", "s3://" + config["bucket"] + "/" + root["key"],
                    str(generation / "manifest.json"), "--only-show-errors"], check=True)
    if (generation / "manifest.json").stat().st_size != root["bytes"] or sha(generation / "manifest.json") != root["sha256"] or root["sha256"] != config["root_sha256"]:
        raise ValueError("current root manifest differs")
    sealed = config["sealed"]
    raw_path = out / "queries64.raw"
    raw_sha = prefix_object(config["bucket"], sealed["queries.raw"], raw_path, 64 * 768 * 4)
    queries = np.frombuffer(raw_path.read_bytes(), dtype="<f4").reshape(64, 768)
    if not np.isfinite(queries).all() or not (queries != 0).any(axis=1).all():
        raise ValueError("fresh development query geometry differs")
    requests_path = out / "requests64.jsonl"
    with requests_path.open("x") as stream:
        for ordinal, vector in enumerate(queries):
            stream.write(json.dumps({"query_ordinal": ordinal, "query": vector.tolist()},
                                    separators=(",", ":"), allow_nan=False) + "\n")
    request_sha = prefix_object(config["bucket"], sealed["requests.jsonl"],
                                out / "sealed-requests64.jsonl", requests_path.stat().st_size)
    if requests_path.read_bytes() != (out / "sealed-requests64.jsonl").read_bytes():
        raise ValueError("development requests differ from sealed original")
    truth_path = out / "truth64.u32"
    truth_sha = prefix_object(config["bucket"], sealed["truth.u32"], truth_path, 64 * 100 * 4)
    truth_body = truth_path.read_bytes()
    truth = [struct.unpack_from("<100I", truth_body, q * 400) for q in range(64)]
    if any(len(set(row)) != 100 or max(row) >= 1_000_000 for row in truth):
        raise ValueError("fresh exact truth geometry differs")
    requests = [json.loads(row) for row in requests_path.read_text().splitlines()]
    assert [r["query_ordinal"] for r in requests] == list(range(64))
    refs = {}
    native_quality = {}
    for k in (10, 100):
        reference = out / f"reference-k{k}.jsonl"
        index = prefix + f"/indexes/relaion/k{k}"
        native(out, f"reference-k{k}", [binaries / "two_bit_plan_demo", generation,
              config["root_sha256"], requests_path, sha(requests_path), reference,
              0, 64, "--live-s3", config["bucket"], config["region"], index,
              "--panel-count", 64, "--top-k", k])
        records = [json.loads(line) for line in reference.read_text().splitlines()]
        if (len(records) != 66 or records[0]["root_sha256"] != config["root_sha256"]
                or records[0]["top_k"] != k or records[0]["declared_panel_count"] != 64
                or records[-1]["count"] != 64):
            raise ValueError("fresh native reference roster differs")
        refs[k] = records[1:-1]
        hits = 0
        for q, row in enumerate(refs[k]):
            if (row["query_ordinal"] != q or len(row["ids"]) != k
                    or len(set(row["ids"])) != k or len(row["ranges"]) != row["submitted_gets"]
                    or row["submitted_gets"] > 32 or row["planned_bytes"] != row["verified_bytes"]
                    or row["verified_bytes"] > 16_773_120 or row["failed_gets"]):
                raise ValueError("fresh native ID or physical bounds differ")
            hits += len(set(row["ids"]) & set(truth[q][:k]))
        native_quality[str(k)] = {"hits": hits, "denominator": 64 * k,
                                  "recall": hits / (64 * k)}
    quality = {"dataset": "ReLAION FIRST1M D768 cosine", "split": "fresh rank16 development0-63",
               "root_sha256": config["root_sha256"], "query_prefix_sha256": raw_sha,
               "requests_prefix_sha256": request_sha, "truth_prefix_sha256": truth_sha,
               "only_dev64_ranges_read": True, "native_quality": native_quality,
               "qualification": False}
    write(out / "native-quality.json", quality)
    if native_quality["10"]["recall"] < .95:
        write(out / "decision.json", {"decision": "FAIL fresh ReLAION1M native R@10 development",
                                      "native_quality": native_quality, "fresh_cohort_used": True,
                                      "http_measured": False, "qualification": False})
        return
    runs = []
    for rep, k in enumerate(config["setting_order"]):
        result = run(out, rep, k, config, binaries / "two_bit_http",
                     prefix + f"/indexes/relaion/k{k}", requests, refs[k], truth)
        runs.append(result)
        if k == 10 and not result["development_gate_passed"]:
            break
    k10 = [r for r in runs if r["k"] == 10]
    passed = len(k10) == 2 and all(r["development_gate_passed"] for r in k10)
    write(out / "decision.json", {"decision": "GO fresh ReLAION1M development only" if passed
                                  else "FAIL fresh ReLAION1M offered HTTP development",
                                  "native_quality": native_quality, "runs": runs,
                                  "fresh_cohort_used": True, "http_measured": True,
                                  "qualification": False, "matched_control_http_measured": False,
                                  "matched_vendor_measured": False})


if __name__ == "__main__":
    main()
