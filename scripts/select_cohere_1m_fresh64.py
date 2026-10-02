#!/usr/bin/env python3
"""Select exactly64 CoHere holdout locators from authenticated metadata only."""

import argparse
import copy
import hashlib
import json
import os
from pathlib import Path
import platform
import random
import sys
import tempfile

from build_cohere_1m_source import BUCKET, DIMS, ROWS, SOURCE_SHA, digest


REPO = Path(__file__).resolve().parent.parent
AUTHORITY = REPO / "docs/research/performance-architecture-20260930/semantic-1m/cohere-panel-tools/metadata-authority.json"
SEED = b"borsuk-cohere-first1m-fresh64-v1"
INTERVALS = [[1_002_000, 1_005_000], [1_006_000, 10_000_000]]
POPULATION = 8_997_000
RECEIPT_SHA = "0965aa0241199822dfac3410bba4edad5536ac0eb0aaa8ab83c216e8c5749a87"
CONTENT_SHA = "fa8ccb38e5c761388e0c2ac211cc219438cd79e802e69debd6197d74f83f11ad"
PUBLICATION = "publication/v3/20260812/datasets/cohere-large-10m-768/attempts/0001"
RULE = "random.Random(seed_integer).sample(range(population_size), 64); map ranks through ascending eligible half-open intervals; preserve sampled order"
FAILURE_POLICY = "STOP whole fixed panel on any duplicate or exclusion failure; never replace or resample"


def require(condition, message):
    if not condition:
        raise ValueError(message)


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":")).encode()


def value_sha(value):
    return hashlib.sha256(canonical(value)).hexdigest()


def read_pinned(path, expected_sha, expected_bytes=None):
    require(isinstance(expected_sha, str) and len(expected_sha) == 64
            and all(c in "0123456789abcdef" for c in expected_sha), "missing/invalid SHA256 pin")
    with path.open("rb") as stream:
        body = stream.read((4 << 20) + 1)
    require(len(body) <= 4 << 20, "metadata exceeds 4MiB limit")
    require(expected_bytes is None or len(body) == expected_bytes,
            f"metadata byte identity differs: {path}")
    require(hashlib.sha256(body).hexdigest() == expected_sha, f"metadata SHA256 differs: {path}")
    return json.loads(body)


def read_proof(pin):
    path = (REPO / pin["path"]).resolve()
    require(path.is_relative_to(REPO / "docs/research"), "proof must be repository research metadata")
    return read_pinned(path, pin["sha256"], pin["bytes"])


def receipt_roster(receipt):
    require(receipt["dataset_id"] == "cohere-large-10m-768"
            and receipt["dataset_content_sha256"] == CONTENT_SHA
            and receipt["object_count"] == len(receipt["objects"]) == 461,
            "published receipt geometry/identity differs")
    ordered = sorted((row for row in receipt["objects"] if row["role"] == "train"),
                     key=lambda row: row["uri"])
    require(len(ordered) == 458, "full 458-shard train roster required")
    shards, start = [], 0
    for ordinal, row in enumerate(ordered):
        key = f"{PUBLICATION}/materialized/train-{ordinal:08d}.parquet"
        require(row["uri"] == f"s3://{BUCKET}/{key}" and row["format"] == "parquet"
                and type(row["rows"]) is int and row["rows"] > 0
                and type(row["bytes"]) is int and row["bytes"] > 0,
                "train shard order/identity/geometry differs")
        require(len(row["sha256"]) == 64 and all(c in "0123456789abcdef" for c in row["sha256"]),
                "missing shard hash")
        shards.append(dict(ordinal=ordinal, key=key, bytes=row["bytes"], sha256=row["sha256"],
                           rows=row["rows"], source_start=start,
                           source_end_exclusive=start + row["rows"]))
        start += row["rows"]
    require(start == 10_000_000, "published train population differs")
    return shards


def authenticate_bindings(a, proofs):
    pins = a["proofs"]
    candidate, stress = proofs["candidate"], proofs["stress"]
    build, terminal, source = (proofs[name] for name in
                               ("source_build", "source_terminal", "source_verification"))
    seal, identity, old = (proofs[name] for name in ("old_seal", "old_identity", "old_verification"))
    require(terminal["status"] == "complete" and terminal["exit_code"] == 0
            and terminal["artifacts"]["source-build.json"]["sha256"] == pins["source_build"]["sha256"]
            and terminal["artifacts"]["source-build.json"]["bytes"] == pins["source_build"]["bytes"]
            and source["terminal_sha256"] == pins["source_terminal"]["sha256"]
            and source["valid_source_construction"] is True, "source construction proofs differ")
    corpus = a["corpus"]
    raw = dict(key=stress["source_terminal_key"].removesuffix("/terminal.json") + "/artifacts/vectors.raw",
               **stress["source_raw_identity"],
               producer_terminal=dict(key=stress["source_terminal_key"], sha256=stress["source_terminal_sha256"]))
    require(canonical(corpus["raw"]) == canonical(raw)
            and raw["bytes"] == ROWS * DIMS * 4
            and raw["sha256"] == build["source_sha256"] == source["source_raw_sha256"] == SOURCE_SHA,
            "original FIRST1M raw crossbinding differs")
    require(corpus["normalized_sha256"] == build["normalized_sha256"]
            and canonical(corpus["sq8"]) == canonical(terminal["artifacts"]["sq8.bin"])
            and corpus["sq8"]["sha256"] == build["sq8_sha256"]
            and corpus["sq8"]["key"] == build["sq8_object_key"]
            and corpus["sq8"]["bytes"] == 780_000_000, "SQ8 source crossbinding differs")
    require(canonical(corpus["order"]) == canonical(terminal["artifacts"]["order.u64"])
            and corpus["order"]["sha256"] == build["order_sha256"]
            and corpus["order"]["bytes"] == ROWS * 8
            and corpus["order_format"] == "LEu64 permutation: physical position to original source ordinal",
            "source order crossbinding differs")
    require(canonical(corpus["root_manifest"]) == canonical(source["generation_artifacts"]["generation/manifest.json"])
            and corpus["root_manifest"]["sha256"] == terminal["root_sha256"]
            == build["root_sha256"] == source["root_sha256"], "source root crossbinding differs")
    require(corpus["rows"] == ROWS and corpus["dimensions"] == DIMS and corpus["metric"] == "cosine"
            and corpus["raw_dtype"] == "<f4" and corpus["source_interval"] == [0, ROWS],
            "FIRST1M corpus geometry differs")
    slices = [dict(shard_ordinal=s["ordinal"], local_row_start=0,
                   local_row_end_exclusive=min(s["rows"], ROWS - s["source_start"]))
              for s in a["ordered_train_shards"] if s["source_start"] < ROWS]
    require(canonical(corpus["first1m_shard_slices"]) == canonical(slices)
            and slices[-1] == dict(shard_ordinal=45, local_row_start=0, local_row_end_exclusive=16_975),
            "FIRST1M constituent boundary differs")
    require(candidate["source_receipt"]["sha256"] == a["source_receipt"]["sha256"]
            and candidate["source_receipt"]["key"] == a["source_receipt"]["key"]
            and candidate["indexed_source_rows"] == [0, ROWS - 1], "candidate corpus binding differs")
    shard = a["ordered_train_shards"][46]
    old_shard = candidate["source_shard"]
    require(all(shard[k] == old_shard[k] for k in ("key", "bytes", "sha256"))
            and shard["source_start"] == old_shard["row_start"]
            and shard["source_end_exclusive"] == old_shard["row_end_exclusive"],
            "consumed panel shard binding differs")
    require(old["valid_construction"] is True and seal["status"] == "complete" and seal["exit_code"] == 0
            and old["terminal_sha256"] == pins["old_seal"]["sha256"]
            and identity["source_build_terminal_sha256"] == pins["source_terminal"]["sha256"]
            and identity["source_candidate_sha256"] == pins["candidate"]["sha256"]
            and identity["query_count"] == 1000 and not any(identity["duplicate_counts"].values()),
            "old consumed panel proofs differ")
    for proof in (seal, old):
        require(proof["source_raw_sha256"] == SOURCE_SHA and proof["root_sha256"] == source["root_sha256"],
                "old panel source crossbinding differs")
    require(seal["query_rows"] == identity["candidate_rows"] == candidate["candidate_query_rows_inclusive"]
            == [1_005_000, 1_005_999]
            and seal["development_ordinals"] == [0, 63] and seal["prospective_ordinals"] == [64, 999],
            "old consumed panel ordinals differ")
    require(canonical(seal["artifacts"]) == canonical(old["sealed_artifacts"])
            and identity["query_raw_sha256"] == old["sealed_artifacts"]["queries.raw"]["sha256"]
            and old["sealed_artifacts"]["identity.json"]["sha256"] == pins["old_identity"]["sha256"],
            "old panel artifact crossbinding differs")
    expected_old = dict(source_interval=[1_005_000, 1_006_000], count=1000,
                        consumed_panel_ordinals=[[0, 64], [64, 1000]], artifacts=old["sealed_artifacts"],
                        truth_scope="old consumed panel only; no fresh64 truth authority")
    require(canonical(a["old_consumed_panel"]) == canonical(expected_old), "whole consumed1000 panel required")
    for name, first, count in [("dev64_config", 0, 64), ("confirm936_config", 64, 936)]:
        config = proofs[name]
        require(config["count"] == count and config.get("first", 0) == first
                and config["rows"] == ROWS and config["dimensions"] == DIMS
                and config["source_raw_sha256"] == SOURCE_SHA
                and config["root_sha256"] == source["root_sha256"]
                and canonical(config["sealed"]) == canonical(old["sealed_artifacts"])
                and config["source_construction_verification_sha256"] == pins["source_verification"]["sha256"]
                and config["sealed_construction_verification_sha256"] == pins["old_verification"]["sha256"],
                "development/confirmation consumption config binding differs")
    for name, key in [("registered_test", "registered_test"), ("stress", "stress_train"),
                      ("v271", "v271_train"), ("v277", "v277_train"), ("v278", "v278_train")]:
        require(candidate["known_query_proof_sha256"][key] == pins[name]["sha256"],
                "consumed-query proof crossbinding differs")
    ranges = [stress["excluded_raw_source_rows_inclusive"], proofs["v277"]["prior_query_rows_inclusive"],
              proofs["v271"]["prior_query_rows_inclusive"], proofs["v278"]["prior_query_source_rows"]]
    require(ranges == candidate["known_consumed_train_ranges_inclusive"], "earlier train exclusions differ")
    ledger = [dict(split="train", interval=[low, high + 1], proofs=[name])
              for (low, high), name in zip(ranges, ("stress", "v277", "v271", "v278"))]
    ledger.extend([dict(split="train", interval=[1_005_000, 1_006_000],
                        proofs=["candidate", "old_seal", "old_identity", "old_verification", "dev64_config", "confirm936_config"],
                        consumed_panel_ordinals=[[0, 64], [64, 1000]]),
                   dict(split="test", interval=[0, 1000], proofs=["registered_test"],
                        shard_sha256=proofs["registered_test"]["test_sha256"])])
    require(candidate["registered_test_rows_inclusive"] == [0, 999]
            and proofs["registered_test"]["query_count"] == 1000
            and canonical(a["consumed_query_ledger"]) == canonical(ledger), "consumed-query ledger differs")
    require(all(proofs[name]["complete_prior_query_audit"] is False
                for name in ("candidate", "stress", "registered_test", "v271", "v277", "v278", "old_seal", "old_identity", "old_verification")),
            "historical coverage must remain explicitly incomplete")


def validate_population(a, *, historical_metadata_replay=False):
    spec = a["selection"]
    geometry = dict(candidate_interval=[ROWS, 10_000_000],
                    excluded_consumed_intervals=[[1_000_000, 1_002_000], [1_005_000, 1_006_000]],
                    eligible_intervals=INTERVALS, population_size=POPULATION,
                    roster_sha256=value_sha(a["ordered_train_shards"]))
    require(canonical({key: spec[key] for key in geometry}) == canonical(geometry)
            and spec["population_sha256"] == value_sha(geometry), "eligible population authority differs")
    seed = hashlib.sha256(SEED).digest()
    require(spec["seed_text"] == SEED.decode() and spec["seed_sha256"] == seed.hex()
            and type(spec["seed_integer"]) is int and spec["seed_integer"] == int.from_bytes(seed, "big"),
            "sampling seed authority differs")
    # Read-only replay authenticates the original sampling provenance; only new
    # sampling requires that interpreter on the current host.
    python = (dict(implementation="CPython", version="3.14.4") if historical_metadata_replay else
              dict(implementation=platform.python_implementation(), version=platform.python_version()))
    require(spec["python"] == python,
            "frozen Python implementation/version differs")
    require(type(spec["count"]) is int and spec["count"] == 64 and spec["rule"] == RULE
            and spec["failure_policy"] == FAILURE_POLICY, "fixed sampling rule/policy differs")
    require(sum(high - low for low, high in INTERVALS) == POPULATION, "eligible interval geometry differs")


def load_authority(path, expected_sha):
    a = read_pinned(path, expected_sha)
    require(a["schema"] == "borsuk-cohere-first1m-fresh64-metadata-authority-v1"
            and a["authority_pending"] is False and type(a["root_pending"]) is bool
            and a["metadata_only"] is True and a["vector_or_truth_bodies_opened"] is False
            and a["complete_historical_coverage"] is False and a["qualification"] is False,
            "missing/pending metadata authority or scope differs")
    require(a["bucket"] == BUCKET and a["dataset_id"] == "cohere-large-10m-768"
            and a["dataset_content_sha256"] == CONTENT_SHA and a["train_rows"] == 10_000_000
            and a["embedding_column"] == "emb", "CoHere train authority differs")
    receipt_pin = a["source_receipt"]
    require(receipt_pin["sha256"] == RECEIPT_SHA and receipt_pin["bytes"] == 135_298
            and receipt_pin["key"] == PUBLICATION + "/STAGING_COMPLETE.json", "original receipt pin differs")
    roster = receipt_roster(read_proof(receipt_pin))
    require(canonical(a["ordered_train_shards"]) == canonical(roster), "missing/changed ordered train shard identity")
    names = {"candidate", "stress", "registered_test", "v271", "v277", "v278", "source_build",
             "source_terminal", "source_verification", "old_seal", "old_identity", "old_verification",
             "dev64_config", "confirm936_config"}
    require(set(a["proofs"]) == names, "missing construction/consumption proof")
    authenticate_bindings(a, {name: read_proof(pin) for name, pin in a["proofs"].items()})
    validate_population(a)
    return a


def rank_to_ordinal(rank, intervals):
    require(type(rank) is int and rank >= 0, "invalid eligible rank")
    for low, high in intervals:
        if rank < high - low:
            return low + rank
        rank -= high - low
    raise ValueError("eligible rank outside population")


def locate(ordinal, shards):
    for index, shard in enumerate(shards):
        if shard["source_start"] <= ordinal < shard["source_end_exclusive"]:
            return index, ordinal - shard["source_start"]
    raise ValueError("source ordinal outside shard roster")


def sampled_ranks(a):
    return random.Random(a["selection"]["seed_integer"]).sample(range(POPULATION), 64)


def validate_panel(panel, a, *, historical_metadata_replay=False):
    validate_population(a, historical_metadata_replay=historical_metadata_replay)
    require(len(panel) == 64 and len({r["source_ordinal"] for r in panel}) == 64,
            "STOP whole panel: duplicate or count failure")
    for ordinal, (row, rank) in enumerate(zip(panel, sampled_ranks(a))):
        source = row["source_ordinal"]
        require(type(source) is int and ROWS <= source < 10_000_000
                and not any(entry["split"] == "train" and entry["interval"][0] <= source < entry["interval"][1]
                            for entry in a["consumed_query_ledger"]), "STOP whole panel: consumed/excluded source ordinal")
        index, local_row = locate(source, a["ordered_train_shards"])
        shard = a["ordered_train_shards"][index]
        expected = dict(query_ordinal=ordinal, eligible_rank=rank,
                        source_ordinal=rank_to_ordinal(rank, INTERVALS), shard_ordinal=index,
                        shard_key=shard["key"], shard_sha256=shard["sha256"], shard_bytes=shard["bytes"],
                        local_row=local_row, embedding_column="emb")
        require(canonical(row) == canonical(expected), "STOP whole panel: fixed locator/order differs")


def select_panel(a):
    validate_population(a)
    panel = []
    for ordinal, rank in enumerate(sampled_ranks(a)):
        source = rank_to_ordinal(rank, INTERVALS)
        index, local_row = locate(source, a["ordered_train_shards"])
        shard = a["ordered_train_shards"][index]
        panel.append(dict(query_ordinal=ordinal, eligible_rank=rank, source_ordinal=source,
                          shard_ordinal=index, shard_key=shard["key"], shard_sha256=shard["sha256"],
                          shard_bytes=shard["bytes"], local_row=local_row, embedding_column="emb"))
    validate_panel(panel, a)
    return panel


def write_new(output, result):
    temporary = None
    try:
        with tempfile.NamedTemporaryFile(mode="w", dir=output.parent, prefix=".cohere-fresh64-", delete=False) as stream:
            temporary = Path(stream.name)
            stream.write(json.dumps(result, indent=2) + "\n")
            stream.flush()
            os.fsync(stream.fileno())
        # A same-directory hard link publishes the complete file atomically and never overwrites.
        os.link(temporary, output)
    finally:
        if temporary is not None:
            temporary.unlink()


def main(authority_path, expected_sha, output):
    a = load_authority(authority_path, expected_sha)
    panel = select_panel(a)
    result = dict(schema="borsuk-cohere-first1m-fresh64-locators-v1", metadata_only=True,
                  root_pending=a["root_pending"], qualification=False, complete_historical_coverage=False,
                  vector_or_truth_bodies_opened=False, authority_sha256=expected_sha,
                  source_receipt_sha256=a["source_receipt"]["sha256"],
                  corpus=a["corpus"], selection=a["selection"],
                  consumed_query_ledger_sha256=value_sha(a["consumed_query_ledger"]),
                  selected_sha256=value_sha(panel), selected=panel)
    write_new(output, result)
    print(json.dumps({"selected": len(panel), "selected_sha256": result["selected_sha256"],
                      "authority_sha256": expected_sha, "root_pending": a["root_pending"]}))


def self_check():
    if sys.flags.optimize:
        raise ValueError("self-check requires assertions enabled")
    # Hand-derived boundaries catch holes, inclusive endpoints and rank drift.
    for rank, want in [(0, 1_002_000), (2999, 1_004_999),
                       (3000, 1_006_000), (8_996_999, 9_999_999)]:
        assert rank_to_ordinal(rank, INTERVALS) == want

    def rejects(call):
        try:
            call()
        except (ValueError, KeyError, TypeError, OSError):
            return
        raise AssertionError("invalid metadata was accepted")

    rejects(lambda: rank_to_ordinal(-1, INTERVALS))
    rejects(lambda: rank_to_ordinal(POPULATION, INTERVALS))
    authority_sha = digest(AUTHORITY)
    authority = load_authority(AUTHORITY, authority_sha)
    panel = select_panel(authority)
    assert panel == select_panel(authority)
    ordinals = [row["source_ordinal"] for row in panel]
    assert len(panel) == len(set(ordinals)) == 64
    # Golden value from a separate stdlib application of the preregistered rule.
    assert hashlib.sha256(json.dumps(ordinals, separators=(",", ":")).encode()).hexdigest() == (
        "f58957a9a7cfb445298a6a6a48c139a5dc8f3df1d9f7714602d4a55141ee321c")
    assert ordinals[:3] == [5_562_837, 7_461_580, 3_208_519]
    assert all(1_000_000 <= row < 10_000_000 for row in ordinals)
    for row in panel:
        shard = authority["ordered_train_shards"][row["shard_ordinal"]]
        assert row["source_ordinal"] == shard["source_start"] + row["local_row"]
        assert 0 <= row["local_row"] < shard["rows"]
        assert row["shard_key"] == shard["key"] and row["shard_sha256"] == shard["sha256"]
        assert row["embedding_column"] == "emb"
    for consumed in [100_000, 104_999, 1_000_000, 1_001_999, 1_005_000, 1_005_999]:
        bad = copy.deepcopy(panel)
        bad[0]["source_ordinal"] = consumed
        rejects(lambda: validate_panel(bad, authority))
    bad = copy.deepcopy(panel)
    bad[1] = dict(bad[0], query_ordinal=1)
    rejects(lambda: validate_panel(bad, authority))

    # Synthetic shards exercise exact boundary lookup independently of real rows.
    synthetic = [{"source_start": 0, "source_end_exclusive": 3},
                 {"source_start": 3, "source_end_exclusive": 8}]
    assert locate(0, synthetic) == (0, 0)
    assert locate(2, synthetic) == (0, 2)
    assert locate(3, synthetic) == (1, 0)
    assert locate(7, synthetic) == (1, 4)
    rejects(lambda: locate(8, synthetic))

    with tempfile.TemporaryDirectory(prefix="cohere-fresh64-check-") as temp:
        temp = Path(temp)
        changed = temp / "authority.json"

        def reject_change(edit):
            altered = copy.deepcopy(authority)
            edit(altered)
            changed.write_text(json.dumps(altered))
            rejects(lambda: load_authority(changed, authority_sha))
            # Semantic checks also reject when the caller supplies the changed SHA.
            rejects(lambda: load_authority(changed, digest(changed)))

        reject_change(lambda a: a["ordered_train_shards"][0].update(sha256="0" * 64))
        reject_change(lambda a: a["ordered_train_shards"].pop())
        reject_change(lambda a: a.pop("ordered_train_shards"))
        reject_change(lambda a: a["selection"].update(population_size=POPULATION - 1))
        reject_change(lambda a: a["selection"].pop("population_size"))
        reject_change(lambda a: a["selection"].pop("population_sha256"))
        reject_change(lambda a: a["selection"].update(seed_integer=1))
        reject_change(lambda a: a["selection"].pop("seed_sha256"))
        reject_change(lambda a: a["selection"].pop("seed_integer"))
        reject_change(lambda a: a["selection"].update(rule="replacement sampling"))
        reject_change(lambda a: a["selection"]["python"].update(version="0.0.0"))
        for name in ["raw", "sq8", "order"]:
            reject_change(lambda a, name=name: a["corpus"][name].update(sha256="0" * 64))
        reject_change(lambda a: a["proofs"]["source_build"].update(sha256="0" * 64))
        reject_change(lambda a: a["proofs"].pop("source_build"))
        reject_change(lambda a: a["consumed_query_ledger"].pop())

        target = temp / "selected.json"
        main(AUTHORITY, authority_sha, target)
        result = json.loads(target.read_text())
        assert result["selected"] == panel and result["authority_sha256"] == authority_sha
        assert result["root_pending"] is authority["root_pending"] and result["qualification"] is False
        before = target.read_bytes()
        rejects(lambda: main(AUTHORITY, authority_sha, target))
        assert target.read_bytes() == before
        link = temp / "linked.json"
        link.symlink_to(target)
        rejects(lambda: main(AUTHORITY, authority_sha, link))
        assert link.is_symlink() and target.read_bytes() == before
        absent = temp / "must-not-exist.json"
        rejects(lambda: main(AUTHORITY, "0" * 64, absent))
        assert not absent.exists()
        assert sorted(p.name for p in temp.iterdir()) == ["authority.json", "linked.json", "selected.json"]
    print(json.dumps({"self_check": "passed", "selected": 64,
                      "population_size": POPULATION, "authority_sha256": authority_sha,
                      "source_ordinals_sha256": "f58957a9a7cfb445298a6a6a48c139a5dc8f3df1d9f7714602d4a55141ee321c",
                      "metadata_only": True, "network_requests": 0}))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--self-check", action="store_true")
    parser.add_argument("authority", nargs="?", type=Path)
    parser.add_argument("sha256", nargs="?")
    parser.add_argument("new_output", nargs="?", type=Path)
    args = parser.parse_args()
    if args.self_check:
        if any(value is not None for value in (args.authority, args.sha256, args.new_output)):
            parser.error("--self-check takes no positional arguments")
        self_check()
    else:
        if any(value is None for value in (args.authority, args.sha256, args.new_output)):
            parser.error("required: AUTHORITY SHA256 NEW_OUTPUT")
        try:
            main(args.authority, args.sha256, args.new_output)
        except (ValueError, KeyError, TypeError, OSError) as error:
            parser.exit(1, f"STOP: {error}\n")
