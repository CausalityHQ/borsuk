"""Select a provisional V36 rank16 query panel using only physical IDs."""

import argparse
import hashlib
import heapq
import json
import os
from pathlib import Path

import pyarrow.parquet as pq

from scripts.audit_v36_ranked_physical_ids import POPULATION, POPULATION_SHA, REGISTRY, REGISTRY_SHA, digest


REPORT = Path(__file__).resolve().parent.parent / "docs/research/native-union-20260928/fresh-relaion-physical-id-overlap.json"
REPORT_SHA = "888748ef1125416bfa47c6dc791280e8be1885eb7ea68f3145872985fe5801c6"
SEED = b"borsuk-v36-rank16-fresh-v1"
PREVIOUS = REPORT.with_name("fresh-rank16-provisional-ids.json")
PREVIOUS_SHA = "a8bd97d6e6468e715c4b26d9df8c400f649e5428b2a85f151307f20cb030230c"
FIELDS = ("query_ordinal", "feature_row_id", "source_rank", "source_row_offset", "selector_sha256")


def keep_entry(selected, entry, count):
    if len(selected) < count:
        heapq.heappush(selected, entry)
    elif entry > selected[0]:
        heapq.heapreplace(selected, entry)


def panel_digest(panel):
    return hashlib.sha256(json.dumps([[r[k] for k in FIELDS] for r in panel],
                                    separators=(",", ":")).encode()).hexdigest()


def continuation(panel, previous, first=1000, count=64):
    if (len(panel) != first + count or len(previous) != first
            or [r["query_ordinal"] for r in panel] != list(range(first + count))
            or len({r["feature_row_id"] for r in panel}) != len(panel)
            or any(tuple(r[k] for k in FIELDS) != tuple(old[k] for k in FIELDS)
                   for r, old in zip(panel[:first], previous))
            or [(r["selector_sha256"], r["feature_row_id"]) for r in panel]
            != sorted((r["selector_sha256"], r["feature_row_id"]) for r in panel)):
        raise ValueError("frozen reservoir/prefix differs or repeats IDs")
    return [dict(row, query_ordinal=i, reservoir_ordinal=first + i)
            for i, row in enumerate(panel[first:])]


def rows(path):
    ordinal = 0
    for batch in pq.ParquetFile(path).iter_batches(columns=["feature_row_id"], batch_size=65_536):
        for value in batch.column(0).to_pylist():
            if type(value) is not int or not 0 <= value < 2**64:
                raise ValueError("invalid physical feature ID")
            yield ordinal, value
            ordinal += 1


def main(scratch, output, next64=False, previous=PREVIOUS, previous_sha=PREVIOUS_SHA):
    if next64 and (output.exists() or output.is_symlink()):
        raise ValueError("new panel output must not exist")
    if next64 and (previous_sha != PREVIOUS_SHA or digest(previous)[1] != previous_sha):
        raise ValueError("consumed panel identity differs")
    keep = 1064 if next64 else 1000
    assert digest(REGISTRY)[1] == REGISTRY_SHA
    assert digest(POPULATION)[1] == POPULATION_SHA
    assert digest(REPORT)[1] == REPORT_SHA
    report = json.loads(REPORT.read_text())
    assert len(report["objects"]) == 32 and report["shared_physical_ids"] == 5724
    registry = json.loads(REGISTRY.read_text())
    ranked = sorted(registry, key=lambda shard: (
        hashlib.sha256(b"borsuk-v36-screen-object-v1" + shard["path"].encode()
                       + shard["encoded_bytes"].to_bytes(8, "little")).digest(),
        shard["path"].encode()))[:32]
    original = set()
    candidate_seen = set()
    selected = []
    for rank, shard in enumerate(ranked):
        assert report["objects"][rank]["path"] == shard["path"]
        path = scratch / Path(shard["path"]).name
        assert digest(path) == (shard["encoded_bytes"], shard["sha256"])
        count = 0
        for row_offset, feature_id in rows(path):
            count += 1
            if rank < 16:
                original.add(feature_id)
                continue
            if feature_id in candidate_seen:
                continue
            candidate_seen.add(feature_id)
            if feature_id in original:
                continue
            selector = hashlib.sha256(SEED + feature_id.to_bytes(8, "little")).digest()
            score = int.from_bytes(selector, "big")
            entry = (-score, -feature_id, feature_id, rank, row_offset, selector.hex())
            keep_entry(selected, entry, keep)
        assert count == report["objects"][rank]["physical_rows"]
    assert len(original) == report["original_unique_physical_ids"]
    assert len(candidate_seen) == report["candidate_unique_physical_ids"]
    assert len(original & candidate_seen) == report["shared_physical_ids"]
    assert len(selected) == keep
    ordered = sorted(selected, key=lambda entry: (-entry[0], -entry[1]))
    panel = [{"query_ordinal": ordinal, "feature_row_id": row[2],
              "source_rank": row[3], "source_row_offset": row[4],
              "selector_sha256": row[5]} for ordinal, row in enumerate(ordered)]
    assert len({row["feature_row_id"] for row in panel}) == keep
    result = {"schema": "borsuk-v36-rank16-provisional-ids-v1", "metadata_only": True,
              "query_embeddings_or_gt_opened": False, "prior_query_audit_pass": False,
              "qualification": False, "source_registry_sha256": REGISTRY_SHA,
              "population_authority_sha256": POPULATION_SHA,
              "physical_id_report_sha256": REPORT_SHA, "selector_seed": SEED.decode(),
              "development_ordinals": [0, 63], "sealed_ordinals": [64, 999],
              "selected": panel}
    if next64:
        old = json.loads(previous.read_text())
        result.update(schema="borsuk-semantic-1m-fresh-panel-ids-v1",
                      consumed_panel_sha256=previous_sha,
                      consumed_prefix_tuple_sha256=panel_digest(panel[:1000]),
                      reservoir_tuple_sha256=panel_digest(panel),
                      reservoir_ordinals=[1000, 1063], development_ordinals=[0, 63],
                      sealed_ordinals=[], complete_historical_coverage=False,
                      selected=continuation(panel, old["selected"]))
    if next64:
        with output.open("x") as stream:
            stream.write(json.dumps(result, indent=2) + "\n")
    else:
        temporary = output.with_suffix(".partial")
        temporary.write_text(json.dumps(result, indent=2) + "\n")
        os.replace(temporary, output)
    final = result["selected"]
    print(json.dumps({"selected": len(final), "first_id": final[0]["feature_row_id"],
                      "last_id": final[-1]["feature_row_id"]}))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--scratch", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--next64", action="store_true")
    parser.add_argument("--previous", type=Path, default=PREVIOUS)
    parser.add_argument("--previous-sha256", default=PREVIOUS_SHA)
    args = parser.parse_args()
    main(args.scratch, args.output, args.next64, args.previous, args.previous_sha256)
