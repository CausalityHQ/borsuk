"""Historical reproduction gate and coverage diagnostic; no new serving arm."""
import hashlib
import json
from pathlib import Path
import subprocess
import sys


def coverage(truth_pages, candidates, nominated, fetched):
    candidate_set, nominated_set, fetched_set = map(set, (candidates, nominated, fetched))
    assert len(candidates) == len(candidate_set)
    assert nominated_set <= candidate_set and nominated_set <= fetched_set
    return tuple(sum(page in pages for page in truth_pages) for pages in (candidate_set, nominated_set, fetched_set))


def self_check():
    truth = [0, 1, 1, 2]
    assert coverage(truth, [0, 1, 2], [0, 2], [0, 1, 2]) == (4, 2, 4)
    assert coverage(truth, [0, 1, 2], [1], [1]) == (4, 2, 2)
    for candidates, nominated, fetched in [([0, 1], [2], [2]), ([0, 1], [1], [0]), ([0, 0], [0], [0])]:
        try:
            coverage(truth, candidates, nominated, fetched)
        except AssertionError:
            continue
        raise AssertionError("invalid page authority accepted")


if sys.argv[1:] == ["--self-check"]:
    self_check()
    print("coverage self-check passed")
    sys.exit(0)

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq


def sha(path):
    digest = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for block in iter(lambda: stream.read(4 << 20), b""):
            digest.update(block)
    return digest.hexdigest()


def checked(path, expected):
    actual = sha(path)
    if actual != expected:
        raise ValueError(f"reproduction identity mismatch: {path.name}, expected {expected}, actual {actual}")
    return actual


def write_json(path, value):
    with path.open("x") as stream:
        json.dump(value, stream, indent=2)
        stream.write("\n")


def run(directory, phase, command):
    print(phase, flush=True)
    with (directory / (phase + ".stdout")).open("x") as stream:
        subprocess.run(["/usr/bin/time", "-v", "-o", str(directory / (phase + ".time")),
                        "timeout", "--kill-after=10s", "300", *map(str, command)], stdout=stream, check=True)
    return (directory / (phase + ".stdout")).read_text().strip()


def main():
    config_path, historical_repo, directory, current_repo, binaries = map(Path, sys.argv[1:])
    config = json.loads(config_path.read_text())
    directory.mkdir()
    def download(key, path):
        subprocess.run(["aws", "s3", "cp", "s3://" + config["bucket"] + "/" + key, str(path), "--only-show-errors"], check=True)

    original = directory / "input.parquet"
    download(config["input_parquet_key"], original)
    checked(original, config["input_parquet_sha256"])
    raw, normalized, order, sq8 = (directory / name for name in ["vectors.raw", "normalized.f32", "order.u64", "sq8.bin"])
    remaining = 100000
    with raw.open("xb") as output:
        for batch in pq.ParquetFile(original).iter_batches(batch_size=8192, columns=["embedding"]):
            column = batch.column(0)
            assert pa.types.is_fixed_size_list(column.type) and column.type.list_size == 768 and column.type.value_type == pa.float32()
            assert column.null_count == column.values.null_count == 0
            rows = min(remaining, len(column))
            vectors = np.asarray(column.values.to_numpy(zero_copy_only=False), dtype="<f4").reshape(len(column), 768)[:rows]
            assert np.isfinite(vectors).all()
            output.write(vectors.tobytes())
            remaining -= rows
            if not remaining:
                break
    assert remaining == 0 and raw.stat().st_size == 307200000
    checked(raw, config["raw_sha256"])
    tool = binaries / "examples/build_sq8_source"
    run(directory, "normalize", [tool, "normalize", raw, config["raw_sha256"], 100000, 768, 268435456, normalized])
    checked(normalized, config["normalized_sha256"])
    run(directory, "fit", [tool, "fit", normalized, config["normalized_sha256"], 100000, 768, 268435456, order])
    checked(order, config["order_sha256"])
    encoding = json.loads(run(directory, "encode", [tool, normalized, config["normalized_sha256"], 768, order, config["order_sha256"], 268435456, sq8]))
    checked(sq8, config["sq8_sha256"])
    builder = dict(raw=str(raw), raw_sha256=config["raw_sha256"], sq8=str(sq8), sq8_sha256=config["sq8_sha256"],
                   rows=100000, dimensions=768, generation=1, low=encoding["low"], step=encoding["step"],
                   sq8_object_key="native-candidate/objects/" + config["sq8_sha256"], sq8_etag="offline-not-published")
    builder_path = directory / "builder.json"
    write_json(builder_path, builder)
    generation = directory / "generation"
    root_sha = run(directory, "build", [binaries / "build_two_bit_generation", builder_path, sha(builder_path), 268435456, generation])
    assert root_sha == config["root_sha256"]
    checked(generation / "manifest.json", config["root_sha256"])
    write_json(directory / "source-admitted.json", {key: config[key] for key in ["raw_sha256", "normalized_sha256", "order_sha256", "sq8_sha256", "root_sha256"]})

    # Query and truth access begins only after exact historical source/root admission.
    requests = directory / "requests.jsonl"
    download(config["requests_key"], requests)
    checked(requests, config["requests_sha256"])
    prepare = historical_repo / "scripts/v282_prepare_pair.py"
    truth_source = directory / "truth-source"
    run(directory, "truth-source", [sys.executable, prepare, "source", "--input", original, "--input-sha", config["input_parquet_sha256"], "--kind", "parquet", "--output", truth_source])
    checked(truth_source / "source.parquet", config["v282_source_parquet_sha256"])
    truth_path = directory / "truth.u32"
    run(directory, "truth", [sys.executable, prepare, "truth", "--source", truth_source / "source.parquet", "--source-sha", config["v282_source_parquet_sha256"],
                             "--requests", requests, "--requests-sha", config["requests_sha256"], "--output", truth_path])
    assert truth_path.stat().st_size == 400000
    checked(truth_path, config["truth_sha256"])
    truth = np.fromfile(truth_path, dtype="<u4").reshape(1000, 100)
    data = np.memmap(sq8, mode="r", dtype=np.dtype([("id", "<i8"), ("norm", "<f4"), ("code", "u1", (768,))]), shape=(100000,))
    permutation = np.fromfile(order, dtype="<u8")
    assert np.array_equal(np.sort(permutation), np.arange(100000)) and np.array_equal(data["id"], permutation)
    page_of = np.empty(100000, dtype=np.int64)
    page_of[data["id"]] = np.arange(100000) // 256
    receipts = current_repo / "docs/research/native-pipeline-quality-20260928/relaion"
    results = []
    for split in config["splits"]:
        first, count = split["first"], split["count"]
        label = "development" if first == 0 else "validation"
        checked(receipts / split["score_file"], split["score_sha256"])
        closed = json.loads((receipts / split["score_file"]).read_text())
        assert closed["root_sha256"] == root_sha and closed["first"] == first and closed["queries"] == count and len(closed["samples"]) == count
        trace_path = directory / (label + "-traces.jsonl")
        run(directory, label + "-plan", [binaries / "two_bit_plan_demo", generation, root_sha, requests, config["requests_sha256"], trace_path, first, count, "--trace"])
        traces = [json.loads(line) for line in trace_path.read_text().splitlines()]
        assert [t["query_ordinal"] for t in traces] == list(range(first, first + count))
        canonical = "".join(json.dumps({k: t[k] for k in ["query_ordinal", "ranges", "planned_bytes"]}, sort_keys=True, separators=(",", ":")) + "\n" for t in traces).encode()
        assert hashlib.sha256(canonical).hexdigest() == closed["plans_sha256"]
        if first == 0:
            old_trace = current_repo / "docs/research/centroid-discovery-diagnostic-20260928/traces.jsonl"
            assert sha(trace_path) == sha(old_trace)
        samples = []
        for trace, scored in zip(traces, closed["samples"]):
            ordinal = trace["query_ordinal"]
            assert scored["query_ordinal"] == ordinal and (truth[ordinal] < 100000).all() and len(np.unique(truth[ordinal])) == 100
            ranges = trace["ranges"]
            assert 0 < len(ranges) <= 32 and all(0 <= start < end <= 78000000 and start % 199680 == 0 and (end % 199680 == 0 or end == 78000000) for start, end in ranges)
            assert all(ranges[j][1] < ranges[j + 1][0] for j in range(len(ranges) - 1))
            assert sum(end - start for start, end in ranges) == trace["planned_bytes"] <= 16773120
            candidates, nominated = trace["ranked_candidate_pages"], trace["selected_pages"]
            assert len(candidates) == 159 and all(0 <= page < 391 for page in candidates)
            assert len(nominated) == len(set(nominated))
            fetched = set(page for start, end in ranges for page in range(start // 199680, (end - 1) // 199680 + 1))
            candidate_hits, nominated_hits, fetched_hits = coverage(page_of[truth[ordinal]], candidates, nominated, fetched)
            assert fetched_hits == scored["fetched_hits"] and len(ranges) == scored["gets"] and trace["planned_bytes"] == scored["bytes"]
            discovery_loss, nomination_loss, gap_bonus = 100 - candidate_hits, candidate_hits - nominated_hits, fetched_hits - nominated_hits
            gap_pages = fetched - set(nominated)
            candidate_gap_bonus = sum(page in gap_pages and page in set(candidates) for page in page_of[truth[ordinal]])
            outside_candidate_gap_bonus = gap_bonus - candidate_gap_bonus
            assert 100 - discovery_loss - nomination_loss + gap_bonus == fetched_hits
            samples.append(dict(query_ordinal=ordinal, candidate_hits=candidate_hits, nominated_hits=nominated_hits, fetched_hits=fetched_hits,
                                returned_hits=scored["returned_hits"], flat_hits=scored["flat_hits"], discovery_loss=discovery_loss, nomination_loss=nomination_loss,
                                gap_bonus=gap_bonus, candidate_gap_bonus=candidate_gap_bonus, outside_candidate_gap_bonus=outside_candidate_gap_bonus, fetched_not_returned=fetched_hits - scored["returned_hits"], planned_gets=len(ranges), planned_bytes=trace["planned_bytes"]))
        results.append(dict(split=label, first=first, queries=count, traces_sha256=sha(trace_path),
                            means={key: sum(s[key] for s in samples) / count for key in samples[0] if key != "query_ordinal"}, samples=samples))
    write_json(directory / "result.json", dict(dataset=config["dataset"], qualification=False, scope=config["scope"],
                                               historical_code_commit=config["historical_code_commit"], root_sha256=root_sha, results=results))


if __name__ == "__main__":
    main()
