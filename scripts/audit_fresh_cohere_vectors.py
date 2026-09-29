"""Check fixed CoHere query identity, then seal exact GT without evaluating ANN."""

import hashlib
import json
from pathlib import Path
import subprocess
import sys
import time

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

from build_cohere_1m_source import BUCKET, SOURCE_SHA, digest
from v271_fresh_frontier import unit
from publish_cohere_1m_source import publish
from scripts.run_native_source_frontier_1m import oracle, self_check


def acquire(item, out):
    path = out / item["uri"].rsplit("/", 1)[1]
    assert item["uri"].startswith("s3://" + BUCKET + "/")
    if not path.exists():
        subprocess.run(["aws", "--profile", "causality", "s3", "cp", item["uri"],
                        str(path), "--only-show-errors"], check=True)
    assert (path.stat().st_size, digest(path)) == (item["bytes"], item["sha256"])
    table = pq.read_table(path, columns=["emb"])
    col = table.column("emb").combine_chunks()
    assert table.num_rows == item["rows"] and pa.types.is_fixed_size_list(col.type)
    assert col.type.list_size == 768 and col.type.value_type == pa.float32()
    assert col.null_count == col.values.null_count == 0
    values = np.asarray(col.values.slice(col.offset * 768, len(col) * 768).to_numpy(),
                        dtype="<f4").reshape(item["rows"], 768)
    assert np.isfinite(values).all() and (values != 0).any(axis=1).all()
    return values


def hashes(values):
    return [hashlib.sha256(row.tobytes()).digest() for row in values]


def main():
    work = Path(sys.argv[1])
    terminal = json.loads((work / "terminal.json").read_text())
    assert terminal["status"] == "complete" and terminal["remote_bodies_independently_hashed"]
    assert np.__version__ == "2.3.3" and pa.__version__ == "24.0.0"
    root = Path("docs/research/native-union-20260928")
    candidate = json.loads((root / "fresh-cohere-source-candidate.json").read_text())
    for name, path in {
        "registered_test": "fresh-cohere-10m-test-family.json",
        "stress_train": "fresh-cohere-stress-query-families.json",
        "v271_train": "fresh-cohere-prior-identity/verification.json",
        "v277_train": "fresh-cohere-prior-identity/v277-verification.json",
        "v278_train": "fresh-history-source-inventory/v278-exclusion-proof.json",
    }.items():
        assert digest(root / path) == candidate["known_query_proof_sha256"][name]
    assert candidate["candidate_query_rows_inclusive"] == [1005000, 1005999]
    assert candidate["indexed_source_rows"] == [0, 999999]
    receipt_path = work / "publication-receipt.json"
    assert digest(receipt_path) == candidate["source_receipt"]["sha256"]
    receipt = json.loads(receipt_path.read_text())
    shards = sorted((o for o in receipt["objects"] if o["role"] == "train"),
                    key=lambda o: o["uri"])
    assert len(shards) == 458 and sum(o["rows"] for o in shards) == 10_000_000
    out = work / "fresh-identity"
    out.mkdir(exist_ok=True)
    start = sum(o["rows"] for o in shards[:46])
    assert start == 1004870 and shards[46]["sha256"] == candidate["source_shard"]["sha256"]
    selected = acquire(shards[46], out)[1005000 - start:1006000 - start].copy()
    assert selected.shape == (1000, 768)
    selected_hashes = hashes(selected)
    selected_units = hashes(unit(selected))
    raw_selected, unit_selected = set(selected_hashes), set(selected_units)
    duplicate_counts = {"within_panel_raw": 1000 - len(raw_selected),
                        "within_panel_unit": 1000 - len(unit_selected)}
    raw = work / "source.raw"
    assert digest(raw) == SOURCE_SHA and raw.stat().st_size == 3_072_000_000
    indexed_raw, indexed_unit = set(), set()
    with raw.open("rb") as stream:
        for body in iter(lambda: stream.read(8192 * 768 * 4), b""):
            rows = np.frombuffer(body, dtype="<f4").reshape(-1, 768)
            indexed_raw.update(raw_selected.intersection(hashes(rows)))
            indexed_unit.update(unit_selected.intersection(hashes(unit(rows))))
    duplicate_counts.update(indexed_raw=len(indexed_raw), indexed_unit=len(indexed_unit))
    prior_shard = acquire(shards[45], out)
    first = sum(o["rows"] for o in shards[:45])
    assert first == 983025
    prior, prior_raw = [], []
    prior_identities = {}
    for row, expected in [(1000000, "24ff130daf48ef8cd644c9994cd92fd7ad8305d3a5a38127e9ff239fa8a41a22"),
                          (1001000, "6e3505fdfc9d6a6c101cba2b7d4c16d5c07eee6ec704fa836385a810e8ca17d4")]:
        original = prior_shard[row - first:row + 1000 - first]
        prior_raw.extend(hashes(original))
        values = unit(original)
        h = hashlib.sha256(values.tobytes()).hexdigest()
        assert h == expected, "authenticated prior query normalization differs"
        prior.extend(hashes(values))
        prior_identities[str(row)] = h
    test = next(o for o in receipt["objects"] if o["role"] == "query")
    original = acquire(test, out)
    prior_raw.extend(hashes(original))
    values = unit(original)
    assert hashlib.sha256(values.tobytes()).hexdigest() == "4394cb0f28238fe713182094dbb90e2dbc52db634f48c1419997e86ad085ddd4"
    prior.extend(hashes(values))
    duplicate_counts["known_prior_query_raw"] = len(raw_selected.intersection(prior_raw))
    duplicate_counts["known_prior_query_units"] = len(unit_selected.intersection(prior))
    rejected = any(duplicate_counts.values())
    selected.tofile(out / "queries.raw")
    report = dict(schema="borsuk-fresh-cohere-vector-identity-v1",
                  decision="REJECT whole fixed panel" if rejected else "GO scoped fixed-panel construction",
                  candidate_rows=[1005000, 1005999], query_raw_sha256=digest(out / "queries.raw"),
                  query_count=1000, duplicate_counts=duplicate_counts,
                  prior_unit_query_identities=prior_identities,
                  source_candidate_sha256=digest(root / "fresh-cohere-source-candidate.json"),
                  source_build_terminal_sha256=digest(work / "terminal.json"),
                  complete_prior_query_audit=False, gt_or_ann_quality_opened=False,
                  scope="Exact raw/unit duplicate exclusion against indexed first1M (including consumed train100000-104999), authenticated V271/V278 and registered test; existing source-selector metadata proof retained.")
    (out / "identity.json").write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps(report))
    if rejected:
        return
    truth_path = out / "truth.u32"
    assert not truth_path.exists(), "prior GT construction exists; observe it instead of rerunning"
    requests = out / "requests.jsonl"
    with requests.open("x") as stream:
        for q, vector in enumerate(selected):
            stream.write(json.dumps(dict(query_ordinal=q, query=vector.tolist()),
                                    separators=(",", ":"), allow_nan=False) + "\n")
    self_check()
    oracle_sha = digest(Path("scripts/run_native_source_frontier_1m.py"))
    started = time.monotonic()
    truth = oracle(raw, selected, 1_000_000)
    oracle_wall_seconds = time.monotonic() - started
    assert digest(Path("scripts/run_native_source_frontier_1m.py")) == oracle_sha
    assert truth.shape == (1000, 100) and truth.dtype == np.dtype("<u4")
    assert all(len(set(map(int, row))) == 100 for row in truth)
    truth.tofile(truth_path)
    prefix = "research/native-union/20260929/fresh-cohere-seal-a0001"
    artifacts = {name: publish(out / name, prefix + "/sealed/" + name)
                 for name in ["queries.raw", "requests.jsonl", "truth.u32", "identity.json"]}
    sealed = dict(schema="borsuk-fresh-cohere-1m-seal-v1", status="complete", exit_code=0,
                  root_sha256=terminal["root_sha256"], source_raw_sha256=SOURCE_SHA,
                  query_rows=[1005000, 1005999], development_ordinals=[0, 63],
                  prospective_ordinals=[64, 999], artifacts=artifacts,
                  oracle="exhaustive f64 cosine block-sort merge; signed source ordinal ties",
                  oracle_source_sha256=oracle_sha,
                  oracle_wall_seconds=oracle_wall_seconds,
                  controller_sha256=digest(Path(__file__)),
                  preregister_sha256=digest(root / "fresh-cohere-1m-preregister.md"),
                  complete_prior_query_audit=False, ann_quality_measured=False)
    path = out / "seal.json"
    path.write_text(json.dumps(sealed, indent=2) + "\n")
    publish(path, prefix + "/terminal.json")
    print(json.dumps(dict(sealed=True, terminal_sha256=digest(path), ann_quality_measured=False)))


if __name__ == "__main__":
    assert len(sys.argv) == 2
    main()
