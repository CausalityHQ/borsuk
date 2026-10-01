"""Prepare the preregistered 64-query ReLAION continuation (construction only).

Run with ``python -m scripts.prepare_semantic_1m_fresh_panel`` followed by:
  CONFIG CONFIG_SHA256 REPO NEW_OUTPUT PREFIX   (remote construction + seal)
  --replay CONFIG CONFIG_SHA256 REPO OUTPUT   (local receipt verification)
  --self-check                              (bounded synthetic checks only)

Config schema: borsuk-semantic-1m-fresh-panel-v1. Required fields:
  bucket, rows=1000000, dimensions=768, k=100, queries=64,
  reservoir_ordinals=[1000,1063], quality_peek_allowed=false,
  complete_historical_coverage=false, normalization=NORMALIZATION,
  versions={numpy:2.3.3, pyarrow:24.0.0}, code_sha256={repo-relative path:SHA},
  refs={panel,old_panel,old_config,old_verification,registry,population,overlap},
  source_parquet={key,bytes,sha256}, source_raw_sha256,
  consumed_queries={key,bytes,sha256}. Each ref has {path,bytes,sha256}.
All imported scripts must be in code_sha256; extra refs/code pins are allowed.
Optional shard_cache points to read-only registered parquet files. Missing
shards download into fresh owned scratch, never into this cache.

Outputs: source.parquet/source.raw/consumed-queries.raw (authenticated inputs),
queries.raw, requests.jsonl, truth.u32, truth.i64, panel.json,
duplicate-audit.json, oracle.json, resources.json, decision.json. Truth IDs
are SOURCE ORDINALS, widened unchanged to i64. The production scorer must
authenticate the separate source-order/application-ID mapping. The eight
OUTPUTS plus decision.json are conditionally PUT to PREFIX/sealed/NAME and
authenticated by HEAD + GET;
seal-readback.json records those identities locally. No ANN work occurs here.
"""

import hashlib
import json
import resource
import subprocess
import tempfile
import time
from pathlib import Path
import sys

import numpy as np
import pyarrow as pa

from scripts import select_v36_rank16_fresh_ids as selector
from scripts import seal_v36_rank16_fresh_1m as seal
from scripts.run_native_source_frontier_1m import oracle, self_check as oracle_self_check


NORMALIZATION = "original-le-f32-to-le-f64-divide-sqrt-sum-squares-axis1-zero-to-positive-v1"
SCHEMA = "borsuk-semantic-1m-fresh-panel-v1"
OUTPUTS = ("queries.raw", "requests.jsonl", "truth.u32", "truth.i64", "panel.json",
           "duplicate-audit.json", "oracle.json", "resources.json")


def write_json(path, value):
    with path.open("x") as stream:
        json.dump(value, stream, sort_keys=True, allow_nan=False)
        stream.write("\n")


def authenticate(path, ident):
    if seal.digest(path) != (ident["bytes"], ident["sha256"]):
        raise ValueError(f"authenticated length/hash differs: {path.name}")


def vector_hashes(values):
    raw = np.asarray(values, dtype="<f4")
    if (raw.ndim != 2 or raw.shape[1] != 768 or not np.isfinite(raw).all()
            or not (raw != 0).any(axis=1).all()):
        raise ValueError("query/source geometry is not finite nonzero f32 D768")
    unit = raw.astype("<f8")
    unit /= np.sqrt(np.sum(unit * unit, axis=1))[:, None]
    unit[unit == 0] = 0.0
    return ([hashlib.sha256(row.tobytes()).hexdigest() for row in raw],
            [hashlib.sha256(row.tobytes()).hexdigest() for row in unit])


def blocks(path, rows):
    with path.open("rb") as stream:
        for start in range(0, rows, 1024):
            count = min(1024, rows - start)
            body = stream.read(count * 768 * 4)
            if len(body) != count * 768 * 4:
                raise ValueError("raw vector stream truncated")
            yield start, np.frombuffer(body, dtype="<f4").reshape(count, 768)
        if stream.read(1):
            raise ValueError("raw vector stream has trailing bytes")


def audit_duplicates(queries, raw, consumed, rows=1_000_000, consumed_rows=1000):
    raw_hashes, unit_hashes = vector_hashes(queries)
    raw_set, unit_set = set(raw_hashes), set(unit_hashes)
    if len(raw_set) != len(queries) or len(unit_set) != len(queries):
        raise ValueError("new panel repeats raw/unit vectors; no replacement allowed")
    for scope, path, count in (("indexed FIRST1M", raw, rows),
                               ("consumed rank16", consumed, consumed_rows)):
        for start, chunk in blocks(path, count):
            raw_part, unit_part = vector_hashes(chunk)
            for offset, (raw_hash, unit_hash) in enumerate(zip(raw_part, unit_part)):
                if raw_hash in raw_set or unit_hash in unit_set:
                    raise ValueError(f"new panel duplicates {scope} row {start + offset}; no replacement allowed")
    return dict(schema="borsuk-semantic-1m-vector-audit-v1", passed=True,
                normalization=NORMALIZATION, raw_hash_dtype="little-endian f32 bits",
                unit_hash_dtype="little-endian f64; normalized signed zero canonicalized",
                new_rows_audited=len(queries), indexed_rows_audited=rows,
                consumed_rows_audited=consumed_rows, complete_historical_coverage=False,
                raw_sha256=raw_hashes, unit_sha256=unit_hashes,
                replacement_allowed=False)


def write_requests_truth(out, queries, truth):
    if (queries.shape != (64, 768) or queries.dtype != np.dtype("<f4")
            or truth.shape != (64, 100) or truth.dtype != np.dtype("<u4")):
        raise ValueError("query/truth output geometry differs")
    vector_hashes(queries)
    for name, values in (("queries.raw", queries), ("truth.u32", truth),
                         ("truth.i64", truth.astype("<i8"))):
        with (out / name).open("xb") as stream:
            stream.write(values.tobytes())
    with (out / "requests.jsonl").open("x") as stream:
        for ordinal, vector in enumerate(queries):
            stream.write(json.dumps(dict(ordinal=ordinal, query=vector.tolist()),
                                    separators=(",", ":"), allow_nan=False) + "\n")
    check_outputs(out)


def check_outputs(out):
    for name, size in (("queries.raw", 196608), ("truth.u32", 25600), ("truth.i64", 51200)):
        if (out / name).stat().st_size != size:
            raise ValueError(f"output width/length differs: {name}")
    queries = np.fromfile(out / "queries.raw", dtype="<f4").reshape(64, 768)
    vector_hashes(queries)
    truth = np.fromfile(out / "truth.u32", dtype="<u4").reshape(64, 100)
    wide = np.fromfile(out / "truth.i64", dtype="<i8").reshape(64, 100)
    if (not np.array_equal(truth.astype("<i8"), wide) or (truth >= 1_000_000).any()
            or any(len(set(map(int, row))) != 100 for row in truth)):
        raise ValueError("truth source-ordinal widening/roster differs")
    with (out / "requests.jsonl").open() as stream:
        for ordinal, vector in enumerate(queries):
            request = json.loads(stream.readline(2_000_000))
            values = np.asarray(request["query"], dtype="<f4")
            if (set(request) != {"ordinal", "query"} or type(request["ordinal"]) is not int
                    or request["ordinal"] != ordinal or values.shape != (768,)
                    or values.tobytes() != vector.tobytes()):
                raise ValueError("request ordinal/raw f32 parity differs")
        if stream.read(1):
            raise ValueError("request roster has trailing rows")


def read_config(path, expected_sha):
    if seal.digest(path)[1] != expected_sha:
        raise ValueError("configuration identity differs")
    config = json.loads(path.read_text())
    expected = dict(schema=SCHEMA, rows=1_000_000, dimensions=768, k=100, queries=64,
                    reservoir_ordinals=[1000, 1063], quality_peek_allowed=False,
                    complete_historical_coverage=False, normalization=NORMALIZATION,
                    versions=dict(numpy="2.3.3", pyarrow="24.0.0"))
    if (any(config.get(key) != value for key, value in expected.items())
            or type(config.get("quality_peek_allowed")) is not bool
            or type(config.get("complete_historical_coverage")) is not bool
            or (np.__version__, pa.__version__) != ("2.3.3", "24.0.0")):
        raise ValueError("frozen construction configuration differs")
    return config


def repo_path(repo, name):
    path = (repo / name).resolve()
    if Path(name).is_absolute() or not path.is_relative_to(repo.resolve()):
        raise ValueError("frozen reference escapes repository")
    return path


def imported_code(repo):
    paths = {Path(__file__).resolve()}
    paths.update(Path(module.__file__).resolve() for name, module in list(sys.modules.items())
                 if name.startswith("scripts.") and getattr(module, "__file__", None))
    if any(not path.is_relative_to(repo.resolve() / "scripts") for path in paths):
        raise ValueError("imported script origin differs from frozen repository")
    return sorted(str(path.relative_to(repo.resolve())) for path in paths)


def load_inputs(config, repo):
    code = config["code_sha256"]
    if not set(imported_code(repo)) <= set(code):
        raise ValueError("frozen code closure omits an imported script")
    for name, expected in code.items():
        if seal.digest(repo_path(repo, name))[1] != expected:
            raise ValueError(f"frozen code identity differs: {name}")
    refs = config["refs"]
    required = {"panel", "old_panel", "old_config", "old_verification", "registry", "population", "overlap"}
    if not required <= set(refs):
        raise ValueError("frozen input references incomplete")
    data = {}
    for name, ident in refs.items():
        path = repo_path(repo, ident["path"])
        authenticate(path, ident)
        if name in required:
            data[name] = json.loads(path.read_text())
    for name, sha in (("old_panel", selector.PREVIOUS_SHA), ("registry", selector.REGISTRY_SHA),
                      ("population", selector.POPULATION_SHA), ("overlap", selector.REPORT_SHA)):
        if refs[name]["sha256"] != sha:
            raise ValueError(f"original authority identity differs: {name}")
    old, verification, panel = data["old_config"], data["old_verification"], data["panel"]
    if (old["schema"] != "borsuk-v36-rank16-fresh-1m-seal-v1"
            or old["quality_peek_allowed"] is not False
            or verification["valid_construction"] is not True
            or verification["ann_quality_measured"] is not False
            or verification["config_sha256"] != refs["old_config"]["sha256"]
            or verification["panel_sha256"] != refs["old_panel"]["sha256"]
            or old["panel_sha256"] != refs["old_panel"]["sha256"]
            or old["registry_sha256"] != refs["registry"]["sha256"]
            or (old["rows"], old["dimensions"], old["k"]) != (1_000_000, 768, 100)
            or config["bucket"] != old["bucket"]
            or config["source_parquet"] != old["source_parquet"]
            or config["source_raw_sha256"] != old["source_raw_sha256"]
            or config["consumed_queries"] != verification["sealed_artifacts"]["queries.raw"]
            or config["consumed_queries"]["bytes"] != 3_072_000):
        raise ValueError("source/consumed-query authority cross-binding differs")
    validate_panel(panel, data["old_panel"], refs["old_panel"]["sha256"])
    for field, name in (("source_registry_sha256", "registry"),
                        ("population_authority_sha256", "population"),
                        ("physical_id_report_sha256", "overlap")):
        if panel[field] != refs[name]["sha256"]:
            raise ValueError("selected panel authority cross-binding differs")
    return data


def validate_panel(panel, previous, previous_sha):
    expected = dict(schema="borsuk-semantic-1m-fresh-panel-ids-v1", metadata_only=True,
                    query_embeddings_or_gt_opened=False, prior_query_audit_pass=False,
                    qualification=False, complete_historical_coverage=False,
                    selector_seed=selector.SEED.decode(), reservoir_ordinals=[1000, 1063],
                    development_ordinals=[0, 63], sealed_ordinals=[],
                    consumed_panel_sha256=previous_sha)
    if any(panel.get(key) != value for key, value in expected.items()):
        raise ValueError("selected panel scope differs")
    selected, old = panel["selected"], previous["selected"]
    if len(selected) != 64 or len(old) != 1000:
        raise ValueError("selected/consumed panel roster differs")
    combined = [dict(row) for row in old]
    for ordinal, row in enumerate(selected):
        if (type(row["query_ordinal"]) is not int or row["query_ordinal"] != ordinal
                or type(row["reservoir_ordinal"]) is not int or row["reservoir_ordinal"] != 1000 + ordinal
                or type(row["feature_row_id"]) is not int or not 0 <= row["feature_row_id"] < 2**64
                or type(row["source_rank"]) is not int or row["source_rank"] not in range(16, 32)
                or type(row["source_row_offset"]) is not int or row["source_row_offset"] < 0
                or row["selector_sha256"] != hashlib.sha256(
                    selector.SEED + row["feature_row_id"].to_bytes(8, "little")).hexdigest()):
            raise ValueError("selected panel ID/ordinal/locator/hash differs")
        combined.append({key: (1000 + ordinal if key == "query_ordinal" else row[key])
                         for key in selector.FIELDS})
    continued = selector.continuation(combined, old)
    if (any(any(row[key] != expected_row[key] for key in (*selector.FIELDS, "reservoir_ordinal"))
            for row, expected_row in zip(selected, continued))
            or panel["consumed_prefix_tuple_sha256"] != selector.panel_digest(old)
            or panel["reservoir_tuple_sha256"] != selector.panel_digest(combined)
            or len({(row["source_rank"], row["source_row_offset"]) for row in combined}) != 1064):
        raise ValueError("selected panel reservoir identity differs")


def identity(path):
    size, sha = seal.digest(path)
    return dict(path=path.name, bytes=size, sha256=sha)


def resources(started):
    group = Path("/sys/fs/cgroup") / Path("/proc/self/cgroup").read_text().strip().split("0::")[-1].lstrip("/")
    return dict(wall_seconds=time.monotonic() - started,
                process_max_rss_kib=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,
                cgroup_path=str(group), cgroup={name: (group / name).read_text() for name in
                ("memory.max", "memory.peak", "memory.swap.max", "memory.swap.peak", "memory.events", "cpu.stat")
                if (group / name).exists()}, ann_quality_measured=False)


def seal_readback(bucket, prefix, out, expected):
    artifacts = {}
    with tempfile.TemporaryDirectory(dir=out, prefix="readback-") as directory:
        for name, local in expected.items():
            authenticate(out / name, local)
            ident = seal.seal(bucket, prefix, out / name)
            if (ident["bytes"], ident["sha256"]) != (local["bytes"], local["sha256"]):
                raise ValueError("output mutated during sealing")
            head = json.loads(subprocess.check_output([
                "aws", "s3api", "head-object", "--bucket", bucket, "--key", ident["key"]]))
            if head["ContentLength"] != ident["bytes"] or head.get("Metadata", {}).get("sha256") != ident["sha256"]:
                raise ValueError("sealed object HEAD identity differs")
            readback = Path(directory) / "readback"
            seal.checked_s3(bucket, ident, readback)
            readback.unlink()
            authenticate(out / name, local)
            artifacts[name] = dict(ident, etag=head["ETag"], authenticated_readback=True)
    return dict(schema="borsuk-semantic-1m-seal-readback-v1", artifacts=artifacts,
                config_identity_in_decision=True)


def replay(config_path, expected_sha, repo, out):
    config = read_config(config_path, expected_sha)
    load_inputs(config, repo)
    decision = json.loads((out / "decision.json").read_text())
    if (decision["schema"] != "borsuk-semantic-1m-construction-v1"
            or decision["config_sha256"] != expected_sha
            or decision["refs"] != config["refs"] or decision["code_sha256"] != config["code_sha256"]
            or decision["complete_historical_coverage"] is not False
            or decision["ann_quality_measured"] is not False or decision["qualification"] is not False
            or set(decision["artifacts"]) != set(OUTPUTS)):
        raise ValueError("construction receipt binding differs")
    for name, ident in decision["artifacts"].items():
        if ident["path"] != name:
            raise ValueError("construction artifact path differs")
        authenticate(out / name, ident)
    readback = json.loads((out / "seal-readback.json").read_text())
    expected = dict(decision["artifacts"], **{"decision.json": identity(out / "decision.json")})
    if (readback["schema"] != "borsuk-semantic-1m-seal-readback-v1"
            or set(readback["artifacts"]) != set(expected)):
        raise ValueError("seal completion roster differs")
    for name, local in expected.items():
        sealed = readback["artifacts"][name]
        if (sealed["authenticated_readback"] is not True
                or sealed["key"] != f"{decision['prefix']}/sealed/{name}"
                or (sealed["bytes"], sealed["sha256"]) != (local["bytes"], local["sha256"])):
            raise ValueError("seal completion identity differs")
    authenticate(out / "panel.json", config["refs"]["panel"])
    authenticate(out / "source.parquet", config["source_parquet"])
    authenticate(out / "source.raw", dict(bytes=3_072_000_000, sha256=config["source_raw_sha256"]))
    authenticate(out / "consumed-queries.raw", config["consumed_queries"])
    check_outputs(out)
    queries = np.fromfile(out / "queries.raw", dtype="<f4").reshape(64, 768)
    audit = json.loads((out / "duplicate-audit.json").read_text())
    checked = audit_duplicates(queries, out / "source.raw", out / "consumed-queries.raw")
    if any(audit.get(key) != value for key, value in checked.items()):
        raise ValueError("duplicate-audit receipt differs on replay")
    oracle_self_check()
    truth = oracle(out / "source.raw", queries, 1_000_000)
    if truth.tobytes() != (out / "truth.u32").read_bytes():
        raise ValueError("exhaustive oracle differs on replay")
    read_config(config_path, expected_sha)
    load_inputs(config, repo)
    return dict(passed=True, remote_objects_reopened=False, ann_quality_measured=False)


def prepare(config_path, expected_sha, repo, out, prefix):
    config = read_config(config_path, expected_sha)
    data = load_inputs(config, repo)
    if not prefix or prefix != prefix.strip("/") or ".." in prefix.split("/"):
        raise ValueError("new sealed prefix is invalid")
    out.mkdir()
    started = time.monotonic()
    try:
        pa.set_cpu_count(2); pa.set_io_thread_count(1)
        seal.checked_s3(config["bucket"], config["source_parquet"], out / "source.parquet")
        seal.source_raw(out / "source.parquet", out / "source.raw", config["source_raw_sha256"])
        seal.checked_s3(config["bucket"], config["consumed_queries"], out / "consumed-queries.raw")
        cache = Path(config["shard_cache"]) if config.get("shard_cache") else None
        queries, sources = seal.selected_vectors(data["panel"], data["registry"], out,
                                                  count=64, shard_cache=cache)
        audit = audit_duplicates(queries, out / "source.raw", out / "consumed-queries.raw")
        audit.update(config_sha256=expected_sha, panel_sha256=config["refs"]["panel"]["sha256"],
                     source_raw_sha256=config["source_raw_sha256"],
                     consumed_queries=config["consumed_queries"], authenticated_query_sources=sources)
        write_json(out / "duplicate-audit.json", audit)
        oracle_self_check()
        oracle_started = time.monotonic()
        truth = oracle(out / "source.raw", queries, 1_000_000)
        write_requests_truth(out, queries, truth)
        with (out / "panel.json").open("xb") as stream:
            stream.write(repo_path(repo, config["refs"]["panel"]["path"]).read_bytes())
        write_json(out / "oracle.json", dict(schema="borsuk-semantic-1m-oracle-v1", passed=True,
                   oracle_self_check=True, rows=1_000_000, queries=64, k=100,
                   distance="1-dot of f64-normalized original f32", tie="signed source ordinal ascending",
                   exhaustive_block_sort_top100_merge=True, truth_id_space="source ordinal",
                   truth_u32=identity(out / "truth.u32"), truth_i64=identity(out / "truth.i64"),
                   widening="little-endian u32 to little-endian i64, values unchanged",
                   application_id_mapping="production scorer must authenticate separate source-order mapping",
                   source_raw_sha256=config["source_raw_sha256"],
                   wall_seconds=time.monotonic() - oracle_started))
        read_config(config_path, expected_sha)
        load_inputs(config, repo)
        authenticate(out / "source.parquet", config["source_parquet"])
        authenticate(out / "source.raw", dict(bytes=3_072_000_000, sha256=config["source_raw_sha256"]))
        authenticate(out / "consumed-queries.raw", config["consumed_queries"])
        write_json(out / "resources.json", resources(started))
        decision = dict(schema="borsuk-semantic-1m-construction-v1",
                        decision="PASS fresh development panel construction only", qualification=False,
                        ann_quality_measured=False, complete_historical_coverage=False,
                        config_sha256=expected_sha, refs=config["refs"], code_sha256=config["code_sha256"],
                        source_parquet=config["source_parquet"], source_raw_sha256=config["source_raw_sha256"],
                        consumed_queries=config["consumed_queries"], reservoir_ordinals=[1000, 1063],
                        query_ordinals=[0, 63], queries=64, gt_k=100, truth_id_space="source ordinal",
                        prefix=prefix, artifacts={name: identity(out / name) for name in OUTPUTS},
                        resource_pointer="resources.json", oracle_pointer="oracle.json",
                        duplicate_audit_pointer="duplicate-audit.json",
                        seal_completion_pointer="seal-readback.json")
        write_json(out / "decision.json", decision)
        expected = dict(decision["artifacts"], **{"decision.json": identity(out / "decision.json")})
        readback = seal_readback(config["bucket"], prefix, out, expected)
        read_config(config_path, expected_sha)
        load_inputs(config, repo)
        write_json(out / "seal-readback.json", readback)
        return decision
    except BaseException:
        if not (out / "resources.json").exists():
            write_json(out / "resources.json", resources(started))
        raise


def self_check():
    pa.set_cpu_count(1); pa.set_io_thread_count(1)
    rows = [{"query_ordinal": i, "feature_row_id": i + 10,
             "source_rank": 16, "source_row_offset": i,
             "selector_sha256": f"{i:064x}"} for i in range(5)]
    selected = selector.continuation(rows, rows[:3], first=3, count=2)
    assert [(r["query_ordinal"], r["reservoir_ordinal"], r["feature_row_id"])
            for r in selected] == [(0, 3, 13), (1, 4, 14)]
    heap = []
    for score, feature_id in ((5, 7), (1, 4), (1, 2), (3, 8)):
        selector.keep_entry(heap, (-score, -feature_id, feature_id, 16, 0, ""), 3)
    assert [r[2] for r in sorted(heap, key=lambda r: (-r[0], -r[1]))] == [2, 4, 8]
    changed = [dict(r) for r in rows]
    changed[1]["source_row_offset"] += 1
    try:
        selector.continuation(changed, rows[:3], first=3, count=2)
    except ValueError:
        pass
    else:
        raise AssertionError("selection mutation accepted")
    import pyarrow.parquet as pq
    with tempfile.TemporaryDirectory() as directory:
        root = Path(directory)
        cache, scratch = root / "cache", root / "scratch"
        cache.mkdir(); scratch.mkdir()
        vector = [1.] + [0.] * 767
        other = [0., 1.] + [0.] * 766
        body = pa.table({"feature_row_id": pa.array([42, 43], type=pa.int64()),
                         "embedding": pa.array([vector, other],
                                               type=pa.list_(pa.float32(), 768))})
        source = cache / "fixture.parquet"
        pq.write_table(body, source)
        size, sha = seal.digest(source)
        shard = dict(path=source.name, encoded_bytes=size, sha256=sha,
                     uri="file://" + str(source))
        acquired = seal.acquire_owned(shard, scratch, cache)
        assert acquired.is_symlink() and seal.digest(acquired) == (size, sha)
        acquired.unlink()
        assert source.is_file() and seal.digest(source) == (size, sha)
        new = np.zeros((2, 768), dtype="<f4"); new[0, 0] = 1; new[1, 1] = 1
        raw = root / "raw"; np.array([[0.] * 767 + [1.]], dtype="<f4").tofile(raw)
        consumed = root / "consumed"; np.array([[0., 0., 1.] + [0.] * 765], dtype="<f4").tofile(consumed)
        result = audit_duplicates(new, raw, consumed, rows=1, consumed_rows=1)
        assert result["indexed_rows_audited"] == 1 and result["consumed_rows_audited"] == 1
        def rejects(action):
            try:
                action()
            except (ValueError, AssertionError):
                return
            raise AssertionError("invalid input accepted")
        duplicate_ids = [dict(r) for r in rows]
        duplicate_ids[-1]["feature_row_id"] = duplicate_ids[0]["feature_row_id"]
        rejects(lambda: selector.continuation(duplicate_ids, rows[:3], 3, 2))
        reversed_rows = [dict(r) for r in rows]
        reversed_rows[-1]["selector_sha256"] = "0" * 64
        rejects(lambda: selector.continuation(reversed_rows, rows[:3], 3, 2))
        registry = [dict(shard, path=f"part-{i:04d}.parquet") for i in range(2298)]
        ranked = sorted(registry, key=lambda s: (
            hashlib.sha256(b"borsuk-v36-screen-object-v1" + s["path"].encode()
                           + size.to_bytes(8, "little")).digest(), s["path"].encode()))
        cached = cache / ranked[16]["path"]; cached.write_bytes(source.read_bytes())
        fixture = {"selected": [dict(query_ordinal=i, feature_row_id=42+i,
                                   source_rank=16, source_row_offset=i) for i in range(2)]}
        decoded, authenticated = seal.selected_vectors(fixture, registry, scratch, count=2, shard_cache=cache)
        assert decoded.tolist() == [vector, other] and authenticated[0]["sha256"] == sha
        assert cached.exists() and list(scratch.iterdir()) == []
        fixture["selected"][1]["source_row_offset"] = 0
        rejects(lambda: seal.selected_vectors(fixture, registry, scratch, count=2, shard_cache=cache))
        fixture["selected"][1]["source_row_offset"] = 1
        fixture["selected"][1]["feature_row_id"] = 99
        rejects(lambda: seal.selected_vectors(fixture, registry, scratch, count=2, shard_cache=cache))
        assert cached.exists() and list(scratch.iterdir()) == []
        rejects(lambda: seal.acquire_owned(dict(shard, sha256="0"*64), scratch, cache))
        assert source.exists() and list(scratch.iterdir()) == []
        source_bytes = np.asarray([vector, other], dtype="<f4").tobytes()
        # The V36 freezer materializes uint64 IDs; registered candidate shards use int64.
        indexed = root / "indexed.parquet"
        pq.write_table(body.set_column(0, "feature_row_id",
                                      pa.array([43, 42], type=pa.uint64())), indexed)
        raw_copy = root / "source-copy.raw"
        seal.source_raw(indexed, raw_copy, hashlib.sha256(source_bytes).hexdigest(), rows=2)
        assert raw_copy.read_bytes() == source_bytes
        rejects(lambda: seal.source_raw(indexed, root / "bad-copy.raw", "0"*64, rows=2))
        for ordinal, invalid_ids in enumerate((pa.array([42, 42], type=pa.uint64()),
                                              pa.array([42, None], type=pa.uint64()),
                                              pa.array([42, 43], type=pa.int64()),
                                              pa.array([42, -1], type=pa.int64()),
                                              pa.array([42, 43], type=pa.uint32()),
                                              pa.array([42., 43.], type=pa.float64()),
                                              pa.array(["42", "43"]), pa.array([True, False]))):
            invalid_source = root / f"invalid-ids-{ordinal}.parquet"
            pq.write_table(body.set_column(0, "feature_row_id", invalid_ids), invalid_source)
            rejects(lambda: seal.source_raw(invalid_source, root / f"invalid-ids-{ordinal}.raw",
                                             hashlib.sha256(source_bytes).hexdigest(), rows=2))
        for duplicate in (new.copy(), new * 2):
            duplicate[1] = duplicate[0]
            rejects(lambda: audit_duplicates(duplicate, raw, consumed, 1, 1))
        signed = new[:1].copy(); signed[0, 1] = -0.0
        assert vector_hashes(signed)[0] != vector_hashes(new[:1])[0]
        rejects(lambda: audit_duplicates(np.concatenate((new[:1], signed)), raw, consumed, 1, 1))
        for invalid in (np.zeros((1, 768), dtype="<f4"), np.full((1, 768), np.nan, dtype="<f4"),
                        np.full((1, 768), np.inf, dtype="<f4"), np.ones((1, 767), dtype="<f4")):
            rejects(lambda: vector_hashes(invalid))
        (2 * new[:1]).tofile(consumed)
        rejects(lambda: audit_duplicates(new, raw, consumed, 1, 1))
        new[:1].tofile(raw)
        rejects(lambda: audit_duplicates(new, raw, root / "consumed", 1, 1))
        truth = np.tile(np.arange(100, dtype="<u4"), (64, 1))
        queries = np.tile(new[:1], (64, 1))
        write_requests_truth(root, queries, truth)
        assert (root / "queries.raw").stat().st_size == 196608
        assert (root / "truth.u32").stat().st_size == 25600
        assert (root / "truth.i64").stat().st_size == 51200
        check_outputs(root)
        original = (root / "truth.i64").read_bytes()
        (root / "truth.i64").write_bytes(b"x" + original[1:])
        rejects(lambda: check_outputs(root))
        (root / "truth.i64").write_bytes(original[:-1])
        rejects(lambda: check_outputs(root))
        (root / "truth.i64").write_bytes(original)
        request_body = (root / "requests.jsonl").read_text()
        (root / "requests.jsonl").write_text(request_body.replace('"ordinal":0,', '"ordinal":1,', 1))
        rejects(lambda: check_outputs(root))
        (root / "requests.jsonl").write_text(request_body + "{}\n")
        rejects(lambda: check_outputs(root))
        (root / "requests.jsonl").write_text(request_body)
        ident = identity(root / "queries.raw")
        authenticate(root / "queries.raw", ident)
        queries_body = (root / "queries.raw").read_bytes()
        (root / "queries.raw").write_bytes(queries_body + b"x")
        rejects(lambda: authenticate(root / "queries.raw", ident))
        (root / "queries.raw").write_bytes(queries_body)
        oracle_self_check()
        scalar = np.zeros((3, 768), dtype="<f4")
        scalar[0, 0] = scalar[1, 0] = 1; scalar[2, 1] = 1
        tiny = root / "tiny.raw"; scalar.tofile(tiny)
        assert oracle(tiny, new[:1], 3).tolist() == [[0, 1, 2]]
        tiny.write_bytes(tiny.read_bytes() + b"x")
        rejects(lambda: oracle(tiny, new[:1], 3))
        rejects(lambda: list(blocks(tiny, 3)))
        tiny.write_bytes(b"x")
        rejects(lambda: list(blocks(tiny, 1)))
        config = dict(schema=SCHEMA, bucket="fixture", rows=1_000_000, dimensions=768,
                      k=100, queries=64, reservoir_ordinals=[1000, 1063],
                      quality_peek_allowed=False, complete_historical_coverage=False,
                      normalization=NORMALIZATION, versions=dict(numpy="2.3.3", pyarrow="24.0.0"))
        config_path = root / "config.json"; write_json(config_path, config)
        config_sha = seal.digest(config_path)[1]
        assert read_config(config_path, config_sha) == config
        rejects(lambda: read_config(config_path, "0" * 64))
        config["queries"] = 65; config_path.write_text(json.dumps(config))
        rejects(lambda: read_config(config_path, seal.digest(config_path)[1]))
        synthetic = [{"query_ordinal": i, "feature_row_id": feature_id,
                      "source_rank": 16, "source_row_offset": feature_id,
                      "selector_sha256": hashlib.sha256(selector.SEED + feature_id.to_bytes(8, "little")).hexdigest()}
                     for i, feature_id in enumerate(range(1064))]
        synthetic.sort(key=lambda r: (r["selector_sha256"], r["feature_row_id"]))
        for i, row in enumerate(synthetic): row["query_ordinal"] = i
        previous = dict(selected=synthetic[:1000])
        panel = dict(schema="borsuk-semantic-1m-fresh-panel-ids-v1", metadata_only=True,
                     query_embeddings_or_gt_opened=False, prior_query_audit_pass=False,
                     qualification=False, complete_historical_coverage=False,
                     selector_seed=selector.SEED.decode(), reservoir_ordinals=[1000, 1063],
                     development_ordinals=[0, 63], sealed_ordinals=[], consumed_panel_sha256="fixture",
                     consumed_prefix_tuple_sha256=selector.panel_digest(synthetic[:1000]),
                     reservoir_tuple_sha256=selector.panel_digest(synthetic),
                     selected=selector.continuation(synthetic, previous["selected"]))
        validate_panel(panel, previous, "fixture")
        panel["selected"][0]["source_row_offset"] += 1
        rejects(lambda: validate_panel(panel, previous, "fixture"))
        rejects(lambda: repo_path(root, "../escape"))
        rejects(lambda: selector.main(scratch, source, next64=True))
        assert seal.digest(source) == (size, sha)


if __name__ == "__main__":
    if sys.argv[1:] == ["--self-check"]:
        self_check()
        print("self-check PASS")
    elif len(sys.argv) == 6 and sys.argv[1] == "--replay":
        print(json.dumps(replay(Path(sys.argv[2]), sys.argv[3], Path(sys.argv[4]), Path(sys.argv[5]))))
    elif len(sys.argv) == 6:
        prepare(Path(sys.argv[1]), sys.argv[2], Path(sys.argv[3]), Path(sys.argv[4]), sys.argv[5])
    else:
        raise SystemExit(__doc__)
