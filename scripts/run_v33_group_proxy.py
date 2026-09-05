#!/usr/bin/env python3
"""Run the bounded metadata-only V33 three-prototype group diagnostic."""

import argparse
import hashlib
import json
import math
import sys
from pathlib import Path

if __package__:
    from scripts.v33_group_proxy import (
        GroupProxy,
        LeafShape,
        ParentSummary,
        materialize_group_proxies,
        rank_groups,
        rank_shape_groups,
        select_group_prefix,
        selected_group_leaves,
    )
else:
    from v33_group_proxy import (  # type: ignore[no-redef]
        GroupProxy,
        LeafShape,
        ParentSummary,
        materialize_group_proxies,
        rank_groups,
        rank_shape_groups,
        select_group_prefix,
        selected_group_leaves,
    )

EXPECTED_DIGESTS = {
    "directory": "1cd77b268304bc4d36acf9f4beb402ccabc3ec0b1ebde316d2dd7f3a2cdcc995",
    "expanded_terminal": "f78754e0453d939a2c44a7dfeb332bf08e274264f12a48c706994171c2d00950",
    "leaves": "acd94415d04602a8149354189b934e90a0340a5381cf892066fdc0798e73819e",
    "prospective_terminal": "c54255e18102a425d740acb7b204bc5215a0325fed5632dae3546571a5cff8cb",
    "query": "296d45828020c1c0b88c6a1d5c822f6283280513b8c58d01cfa961f3a139a5d4",
    "routing_ranges": "29c4c432560e87c5b00b7043426a3aec4886c6838e0e15c1f572771944abf0a6",
    "shape": "6954ddac2e8dfda76338a9b3c3da278faea80326b29c3427c6aa22753d4e6bea",
}


def _sha256(raw):
    return hashlib.sha256(raw).hexdigest()


def _read(path, role):
    raw = Path(path).read_bytes()
    if not raw or _sha256(raw) != EXPECTED_DIGESTS[role]:
        raise ValueError(f"V33 {role} byte authority differs")
    return raw


def _canonical_json(raw, role):
    value = json.loads(raw)
    expected = json.dumps(
        value, sort_keys=True, separators=(",", ":"), allow_nan=False
    ).encode() + b"\n"
    if raw != expected:
        raise ValueError(f"V33 {role} canonical JSON differs")
    return value


def _arrow_table(raw, expected_schema, role):
    import pyarrow as pa

    reader = pa.ipc.open_file(pa.BufferReader(raw))
    if reader.num_record_batches != 1:
        raise ValueError(f"V33 {role} Arrow batch count differs")
    table = reader.read_all()
    if table.schema != expected_schema or any(column.null_count for column in table.columns):
        raise ValueError(f"V33 {role} Arrow schema differs")
    return table


def _load_authority(args):
    import numpy as np
    import pyarrow as pa

    if __package__:
        from scripts.build_v30_reduced_truth import _matrix, _normalize_like_v30
    else:
        from build_v30_reduced_truth import (  # type: ignore[no-redef]
            _matrix,
            _normalize_like_v30,
        )

    vector = pa.list_(pa.field("element", pa.float16(), nullable=False), 96)
    leaves = _arrow_table(
        _read(args.leaves, "leaves"),
        pa.schema(
            [
                pa.field("root_ordinal", pa.uint16(), nullable=False),
                pa.field("centroid", vector, nullable=False),
            ]
        ),
        "leaves",
    )
    routes = _arrow_table(
        _read(args.routing_ranges, "routing_ranges"),
        pa.schema(
            [
                pa.field("routing_leaf_ordinal", pa.uint32(), nullable=False),
                pa.field("code_parent_leaf_ordinal", pa.uint32(), nullable=False),
                pa.field("routing_centroid", vector, nullable=False),
                pa.field("logical_start", pa.uint64(), nullable=False),
                pa.field("row_count", pa.uint64(), nullable=False),
                pa.field("page_start", pa.uint32(), nullable=False),
                pa.field("page_count", pa.uint32(), nullable=False),
            ]
        ),
        "routing_ranges",
    )
    if leaves.num_rows != 4096 or routes.num_rows != 4141:
        raise ValueError("V33 hierarchy row count differs")
    parent_centers = (
        leaves["centroid"]
        .combine_chunks()
        .values.to_numpy()
        .reshape(4096, 96)
        .astype(np.float64)
    )
    if not np.isfinite(parent_centers).all():
        raise ValueError("V33 parent centroid is nonfinite")
    parent_ordinals = routes["code_parent_leaf_ordinal"].combine_chunks().to_numpy()
    row_counts = routes["row_count"].combine_chunks().to_numpy()
    starts = routes["logical_start"].combine_chunks().to_numpy()
    if (
        not np.array_equal(routes["routing_leaf_ordinal"].to_pylist(), np.arange(4141))
        or np.any(parent_ordinals >= 4096)
        or np.any((row_counts == 0) | (row_counts > 1024))
        or int(row_counts.sum()) != 1_000_000
        or not np.array_equal(starts, np.cumsum(row_counts) - row_counts)
    ):
        raise ValueError("V33 routing range authority differs")
    parent_rows = [0] * 4096
    for parent, rows in zip(parent_ordinals, row_counts, strict=True):
        parent_rows[int(parent)] += int(rows)
    parents = tuple(
        ParentSummary(
            ordinal=ordinal,
            rows=parent_rows[ordinal],
            centroid=tuple(float(value) for value in parent_centers[ordinal]),
        )
        for ordinal in range(4096)
        if parent_rows[ordinal] > 0
    )
    populated_parents = {parent.ordinal for parent in parents}

    directory = _canonical_json(_read(args.directory, "directory"), "directory")
    if (
        set(directory)
        != {"cap_rows", "groups", "helper_sha256", "inputs", "schema", "source_commit"}
        or directory["schema"] != "borsuk-bounded-groups-spike-directory-v1"
        or directory["cap_rows"] != 8192
        or len(directory["groups"]) != 178
    ):
        raise ValueError("V33 group directory schema differs")
    group_rows = []
    for ordinal, group in enumerate(directory["groups"]):
        if (
            set(group) != {"centroid", "parents", "root", "rows"}
            or type(group["root"]) is not int
            or type(group["rows"]) is not int
            or not 0 < group["rows"] <= 8192
            or len(group["centroid"]) != 96
            or any(not math.isfinite(value) for value in group["centroid"])
        ):
            raise ValueError("V33 group row differs")
        group_rows.append((ordinal, group["rows"], tuple(group["parents"])))
    proxies = materialize_group_proxies(
        tuple(group_rows), parents, prototype_count=3, iterations=10
    )
    controls = tuple(
        GroupProxy(
            ordinal=ordinal,
            rows=group["rows"],
            prototypes=(tuple(float(value) for value in group["centroid"]),),
        )
        for ordinal, group in enumerate(directory["groups"])
    )
    group_of_parent = {
        parent: ordinal
        for ordinal, group in enumerate(directory["groups"])
        for parent in group["parents"]
    }
    if set(group_of_parent) != populated_parents:
        raise ValueError("V33 group parent coverage differs")

    shape_vector = pa.list_(pa.field("element", pa.float32(), nullable=False), 96)
    shape = _arrow_table(
        _read(args.shape, "shape"),
        pa.schema(
            [
                pa.field("routing_leaf_ordinal", pa.uint32(), nullable=False),
                pa.field("group_ordinal", pa.uint32(), nullable=False),
                pa.field("logical_start", pa.uint64(), nullable=False),
                pa.field("population", pa.uint64(), nullable=False),
                pa.field("mean", shape_vector, nullable=False),
                pa.field("diagonal_variance", shape_vector, nullable=False),
                pa.field("scalar_moment", pa.float32(), nullable=False),
                pa.field("maximum_radius", pa.float32(), nullable=False),
                pa.field("split_dimension", pa.uint8(), nullable=False),
                pa.field("split_center_left", shape_vector, nullable=False),
                pa.field("split_center_right", shape_vector, nullable=False),
                pa.field("scalar_split_selected", pa.bool_(), nullable=False),
            ]
        ),
        "shape",
    )
    shape_rows = shape.to_pylist()
    if len(shape_rows) != len(row_counts):
        raise ValueError("V33 leaf shape row count differs")
    leaf_shapes = []
    leaf_groups = []
    for ordinal, record in enumerate(shape_rows):
        expected_group = group_of_parent[int(parent_ordinals[ordinal])]
        if (
            record["routing_leaf_ordinal"] != ordinal
            or record["group_ordinal"] != expected_group
            or record["logical_start"] != int(starts[ordinal])
            or record["population"] != int(row_counts[ordinal])
            or not 0 <= record["split_dimension"] < 96
            or record["maximum_radius"] < 0.0
        ):
            raise ValueError("V33 leaf shape binding differs")
        leaf_shapes.append(
            LeafShape(
                ordinal=ordinal,
                group_ordinal=expected_group,
                population=record["population"],
                mean=tuple(record["mean"]),
                diagonal_variance=tuple(record["diagonal_variance"]),
                scalar_moment=record["scalar_moment"],
                split_centers=(
                    tuple(record["split_center_left"]),
                    tuple(record["split_center_right"]),
                ),
                scalar_split_selected=record["scalar_split_selected"],
            )
        )
        leaf_groups.append(expected_group)

    query_raw = _read(args.query, "query")
    queries = _normalize_like_v30(
        _normalize_like_v30(_matrix(query_raw, role="query", physical_rows=10000))
    )
    expanded = _canonical_json(
        _read(args.expanded_terminal, "expanded_terminal"), "expanded_terminal"
    )
    prospective = _canonical_json(
        _read(args.prospective_terminal, "prospective_terminal"),
        "prospective_terminal",
    )
    if expanded.get("status") != "complete" or prospective.get("status") != "complete":
        raise ValueError("V33 terminal status differs")
    evidence = []
    for window in expanded.get("windows", ()):
        evidence.extend(window["diagnostic"]["queries"])
    evidence.extend(prospective["result"]["diagnostic"]["queries"])
    if len(evidence) != 128:
        raise ValueError("V33 query evidence count differs")
    query_authority = []
    for record in evidence:
        ordinal = record["query_ordinal"]
        if type(ordinal) is not int or not 0 <= ordinal < len(queries):
            raise ValueError("V33 query ordinal differs")
        targets = record["current"]["diagnostics"]
        if len(targets) != 10:
            raise ValueError("V33 truth target count differs")
        owners = []
        logicals = []
        for target in targets:
            leaf = target["leaf_ordinal"]
            logical = target["logical"]
            if (
                type(leaf) is not int
                or not 0 <= leaf < len(row_counts)
                or type(logical) is not int
                or not int(starts[leaf]) <= logical < int(starts[leaf] + row_counts[leaf])
            ):
                raise ValueError("V33 truth leaf binding differs")
            owners.append(group_of_parent[int(parent_ordinals[leaf])])
            logicals.append(logical)
        query_authority.append(
            (
                ordinal,
                tuple(float(value) for value in queries[ordinal]),
                tuple(owners),
                tuple(logicals),
            )
        )
    return controls, proxies, tuple(leaf_shapes), tuple(query_authority), tuple(leaf_groups)


def _evaluate(name, groups, queries, row_limit, leaf_groups):
    records = []
    included = 0
    perfect = 0
    for ordinal, query, owners, logicals in queries:
        ranked = rank_groups(groups, query)
        selected = select_group_prefix(
            groups, ranked, row_limit=row_limit, group_limit=64
        )
        chosen = set(selected)
        hits = sum(owner in chosen for owner in owners)
        rows = sum(groups[group].rows for group in selected)
        included += hits
        perfect += hits == len(owners)
        records.append(
            {
                "hits": hits,
                "query": list(query),
                "query_ordinal": ordinal,
                "selected_groups": list(selected),
                "selected_routing_leaves": list(
                    selected_group_leaves(selected, leaf_groups)
                ),
                "selected_rows": rows,
                "truth_logicals": list(logicals),
                "truth_owner_ranks": [ranked.index(owner) + 1 for owner in owners],
            }
        )
    return {
        "arm": name,
        "included_owners": included,
        "maximum_selected_rows": max(record["selected_rows"] for record in records),
        "minimum_selected_rows": min(record["selected_rows"] for record in records),
        "passed": included == 1280 and perfect == 128,
        "perfect_queries": perfect,
        "query_count": len(records),
        "records": records,
        "total_owners": 1280,
    }


def _evaluate_shape(name, groups, shapes, queries, arm, row_limit, leaf_groups):
    records = []
    included = 0
    perfect = 0
    for ordinal, query, owners, logicals in queries:
        ranked = rank_shape_groups(shapes, query, arm)
        selected = select_group_prefix(
            groups, ranked, row_limit=row_limit, group_limit=64
        )
        chosen = set(selected)
        hits = sum(owner in chosen for owner in owners)
        rows = sum(groups[group].rows for group in selected)
        included += hits
        perfect += hits == len(owners)
        records.append(
            {
                "hits": hits,
                "query": list(query),
                "query_ordinal": ordinal,
                "selected_groups": list(selected),
                "selected_routing_leaves": list(
                    selected_group_leaves(selected, leaf_groups)
                ),
                "selected_rows": rows,
                "truth_logicals": list(logicals),
                "truth_owner_ranks": [ranked.index(owner) + 1 for owner in owners],
            }
        )
    return {
        "arm": name,
        "included_owners": included,
        "maximum_selected_rows": max(record["selected_rows"] for record in records),
        "minimum_selected_rows": min(record["selected_rows"] for record in records),
        "passed": included == 1280 and perfect == 128,
        "perfect_queries": perfect,
        "query_count": len(records),
        "records": records,
        "total_owners": 1280,
    }


def run(args):
    controls, proxies, shapes, queries, leaf_groups = _load_authority(args)
    arms = (
        _evaluate("weighted-mean", controls, queries, args.row_limit, leaf_groups),
        _evaluate(
            "three-parent-prototype", proxies, queries, args.row_limit, leaf_groups
        ),
        _evaluate_shape(
            "fine-leaf-centroid",
            controls,
            shapes,
            queries,
            "centroid",
            args.row_limit,
            leaf_groups,
        ),
        _evaluate_shape(
            "scalar-moment",
            controls,
            shapes,
            queries,
            "scalar-moment",
            args.row_limit,
            leaf_groups,
        ),
        _evaluate_shape(
            "diagonal-ellipsoid",
            controls,
            shapes,
            queries,
            "diagonal-moment",
            args.row_limit,
            leaf_groups,
        ),
        _evaluate_shape(
            "matched-byte-split-centroid",
            controls,
            shapes,
            queries,
            "split-centroid",
            args.row_limit,
            leaf_groups,
        ),
    )
    result = {
        "arms": arms,
        "claim_eligible": False,
        "code_reads": 5,
        "corpus_reads": 0,
        "input_sha256": EXPECTED_DIGESTS,
        "page_reads": 0,
        "passed": any(arm["passed"] for arm in arms),
        "row_limit": args.row_limit,
        "schema": "borsuk-v33-group-proxy-result-v1",
    }
    raw = json.dumps(result, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()
    raw += b"\n"
    output = Path(args.output)
    with output.open("xb") as handle:
        handle.write(raw)
    return raw


def parse_args(argv=None):
    values = list(sys.argv[1:] if argv is None else argv)
    parser = argparse.ArgumentParser()
    for role in EXPECTED_DIGESTS:
        parser.add_argument("--" + role.replace("_", "-"), required=True)
    parser.add_argument(
        "--row-limit", required=True, type=int, choices=(65_536, 131_072, 262_144)
    )
    parser.add_argument("--output", required=True)
    parser.add_argument("--execute-group-proxy", action="store_true", required=True)
    singleton_flags = [
        *("--" + role.replace("_", "-") for role in EXPECTED_DIGESTS),
        "--row-limit",
        "--output",
        "--execute-group-proxy",
    ]
    if any(values.count(flag) != 1 for flag in singleton_flags):
        parser.error("each required flag must appear exactly once")
    args = parser.parse_args(values)
    if Path(args.output).exists():
        parser.error("output already exists")
    return args


def main():
    raw = run(parse_args())
    print(
        json.dumps(
            {
                "encoded_bytes": len(raw),
                "sha256": _sha256(raw),
                "status": "complete",
            },
            sort_keys=True,
            separators=(",", ":"),
        )
    )


if __name__ == "__main__":
    main()
