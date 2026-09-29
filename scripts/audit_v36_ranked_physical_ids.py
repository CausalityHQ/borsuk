"""Check physical feature IDs in frozen V36 shards without reading embeddings.

Metadata-only audit. This does not select queries or establish full prior-query
novelty. Source objects are downloaded whole so their registered SHA256 is checked.
"""

import argparse
import hashlib
import json
import os
from pathlib import Path
import urllib.request

import pyarrow as pa
import pyarrow.parquet as pq


ROOT = Path(__file__).resolve().parent.parent
REGISTRY = ROOT / "docs/research/v36-prefix-source-registry.json"
POPULATION = ROOT / "docs/research/native-union-20260928/fresh-identity-authorities/population-authority.json"
REGISTRY_SHA = "b9a19e2f142fd54983ed1db9f09862f2c5623b6b2105d66e538664f8adda9180"
POPULATION_SHA = "be6abb86e2930b38da572cb7daec3fc0e1a7adb93a53ce974ab038f1efa42fbe"


def digest(path):
    h = hashlib.sha256()
    size = 0
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1 << 20), b""):
            size += len(chunk)
            h.update(chunk)
    return size, h.hexdigest()


def acquire(shard, scratch):
    path = scratch / Path(shard["path"]).name
    if path.exists() and digest(path) == (shard["encoded_bytes"], shard["sha256"]):
        return path
    path.unlink(missing_ok=True)
    temporary = path.with_suffix(".partial")
    temporary.unlink(missing_ok=True)
    h = hashlib.sha256()
    size = 0
    try:
        with urllib.request.urlopen(shard["uri"], timeout=120) as response, temporary.open("wb") as out:
            for chunk in iter(lambda: response.read(1 << 20), b""):
                size += len(chunk)
                if size > shard["encoded_bytes"]:
                    raise ValueError("registered shard length exceeded")
                h.update(chunk)
                out.write(chunk)
        if (size, h.hexdigest()) != (shard["encoded_bytes"], shard["sha256"]):
            raise ValueError(f"registered shard identity differs: {shard['path']}")
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)
    return path


def scan_ids(path):
    source = pq.ParquetFile(path)
    field = source.schema_arrow.field("feature_row_id")
    if field.type != pa.int64():
        raise ValueError("registered feature ID type differs")
    ids = set()
    rows = invalid = 0
    for batch in source.iter_batches(columns=["feature_row_id"], batch_size=65_536):
        for value in batch.column(0).to_pylist():
            rows += 1
            if value is None or value < 0:
                invalid += 1
            else:
                ids.add(value)
    return ids, rows, invalid


def main(scratch, output):
    assert digest(REGISTRY)[1] == REGISTRY_SHA
    assert digest(POPULATION)[1] == POPULATION_SHA
    registry = json.loads(REGISTRY.read_text())
    population = json.loads(POPULATION.read_text())
    ranked = sorted(registry, key=lambda shard: (
        hashlib.sha256(b"borsuk-v36-screen-object-v1" + shard["path"].encode()
                       + shard["encoded_bytes"].to_bytes(8, "little")).digest(),
        shard["path"].encode()))
    fields = ("path", "sha256", "encoded_bytes", "uri")
    assert len(ranked) == 2298
    assert all({key: shard[key] for key in fields} == {key: old[key] for key in fields}
               for shard, old in zip(ranked[:16], population["consumed_objects"]))
    assert sum(shard["encoded_bytes"] for shard in ranked[:16]) == 5_485_265_954
    assert sum(shard["encoded_bytes"] for shard in ranked[16:32]) == 5_483_342_562
    scratch.mkdir(parents=True, exist_ok=True)
    original = set()
    candidate = set()
    records = []
    for ordinal, shard in enumerate(ranked[:32]):
        path = acquire(shard, scratch)
        ids, rows, invalid = scan_ids(path)
        target = original if ordinal < 16 else candidate
        target.update(ids)
        records.append({"rank": ordinal, "path": shard["path"],
                        "source_sha256": shard["sha256"], "physical_rows": rows,
                        "valid_unique_ids_in_object": len(ids), "invalid_id_rows": invalid})
        print(f"verified source rank={ordinal} rows={rows} unique_ids={len(ids)}", flush=True)
    report = {"metadata_only": True, "query_gt_or_sealed_bodies_opened": False,
              "complete_prior_query_audit": False, "new_cohort_selected": False,
              "source_registry_sha256": REGISTRY_SHA, "population_authority_sha256": POPULATION_SHA,
              "original_unique_physical_ids": len(original),
              "candidate_unique_physical_ids": len(candidate),
              "shared_physical_ids": len(original & candidate), "objects": records,
              "qualification": False}
    temporary = output.with_suffix(".partial")
    temporary.write_text(json.dumps(report, indent=2) + "\n")
    os.replace(temporary, output)
    print(json.dumps({key: report[key] for key in (
        "original_unique_physical_ids", "candidate_unique_physical_ids", "shared_physical_ids")}), flush=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--scratch", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    arguments = parser.parse_args()
    main(arguments.scratch, arguments.output)
