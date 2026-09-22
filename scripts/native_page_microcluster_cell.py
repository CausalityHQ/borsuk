#!/usr/bin/env python3
"""Phase-separated producer and validator for the fixed 100k microcluster cell."""

from __future__ import annotations

import argparse
import dataclasses
import hashlib
import json
from pathlib import Path

from scripts.launch_native_geometric_layout_spot import (
    FROZEN_QUERIES,
    FROZEN_SOURCE,
    FROZEN_TRUTH,
    FrozenInput,
)
from scripts.launch_native_page_microcluster_spot import PRIOR_MEMBERSHIP
from scripts.native_geometric_layout_screen import (
    ArtifactIdentity,
    EvaluationLimits,
    LayoutAuthority,
    LayoutMethod,
    _ground_truth,
    _source_arrays,
    read_membership_parquet,
)
from scripts.native_page_microcluster_router import (
    construct_representatives,
    evaluate_representatives,
    read_representatives,
    write_evidence,
    write_representatives,
)
from scripts.validate_native_geometric_layout_result import _read_geometric_queries
from scripts.validate_native_page_microcluster_result import validate_evidence


@dataclasses.dataclass(frozen=True, slots=True)
class CellInputs:
    layout: LayoutAuthority
    membership: ArtifactIdentity
    queries: ArtifactIdentity
    truth: ArtifactIdentity
    limits: EvaluationLimits


def _frozen_identity(value: FrozenInput) -> ArtifactIdentity:
    return ArtifactIdentity(value.role, value.uri, value.sha256, value.encoded_bytes)


FROZEN_INPUTS = CellInputs(
    layout=LayoutAuthority(
        schema="borsuk-native-geometric-layout-authority-v1",
        source=_frozen_identity(FROZEN_SOURCE),
        rows=100_000,
        dimensions=768,
        metric="l2",
        seed=20260921,
        method=LayoutMethod.TWO_MEANS_480K,
        maximum_page_rows=65_535,
        maximum_page_bytes=491_520,
    ),
    membership=_frozen_identity(PRIOR_MEMBERSHIP),
    queries=_frozen_identity(FROZEN_QUERIES),
    truth=_frozen_identity(FROZEN_TRUTH),
    limits=EvaluationLimits(32, 16_777_216),
)


def _canonical_bytes(value: dict[str, object]) -> bytes:
    return (json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n").encode()


def _read_canonical(path: Path) -> dict[str, object]:
    payload = path.read_bytes()
    try:
        value = json.loads(payload)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ValueError(f"{path.name} JSON differs") from error
    if type(value) is not dict or payload != _canonical_bytes(value):
        raise ValueError(f"{path.name} canonical bytes differ")
    return value


def _authenticate(path: Path, identity: ArtifactIdentity) -> None:
    payload = path.read_bytes()
    if (
        len(payload) != identity.encoded_bytes
        or hashlib.sha256(payload).hexdigest() != identity.sha256
    ):
        raise ValueError(f"{identity.role} identity differs")


def _remote(identity: ArtifactIdentity, prefix: str, name: str) -> ArtifactIdentity:
    return dataclasses.replace(identity, uri=prefix.rstrip("/") + "/artifacts/" + name)


def _read_inputs(root: Path, inputs: CellInputs):
    ids, vectors = _source_arrays(root / "source.parquet", inputs.layout)
    _authenticate(root / "membership.parquet", inputs.membership)
    membership = read_membership_parquet(
        root / "membership.parquet", inputs.layout, ids
    )
    return ids, vectors, membership


def _read_sealed(root: Path, prefix: str, inputs: CellInputs) -> ArtifactIdentity:
    seal = _read_canonical(root / "sealed.json")
    if (
        set(seal) != {"schema", "source", "membership", "representatives"}
        or seal["schema"] != "borsuk-page-microcluster-seal-v1"
        or seal["source"] != dataclasses.asdict(inputs.layout.source)
        or seal["membership"] != dataclasses.asdict(inputs.membership)
    ):
        raise ValueError("microcluster seal authority differs")
    try:
        representative = ArtifactIdentity(**seal["representatives"])
    except (TypeError, ValueError) as error:
        raise ValueError("microcluster seal representative differs") from error
    if (
        representative.role != "page-microcluster-representatives"
        or representative.uri
        != prefix.rstrip("/") + "/artifacts/representatives.parquet"
    ):
        raise ValueError("microcluster seal URI differs")
    return representative


def construct_cell(root: Path, prefix: str, inputs: CellInputs = FROZEN_INPUTS) -> None:
    ids, vectors, membership = _read_inputs(root, inputs)
    pages = construct_representatives(membership, ids, vectors)
    identity = write_representatives(
        root / "representatives.parquet",
        pages,
        bytes.fromhex(inputs.layout.source.sha256),
        bytes.fromhex(inputs.membership.sha256),
    )
    seal = {
        "schema": "borsuk-page-microcluster-seal-v1",
        "source": dataclasses.asdict(inputs.layout.source),
        "membership": dataclasses.asdict(inputs.membership),
        "representatives": dataclasses.asdict(
            _remote(identity, prefix, "representatives.parquet")
        ),
    }
    (root / "sealed.json").write_bytes(_canonical_bytes(seal))


def _read_queries_truth(root: Path, inputs: CellInputs):
    queries = _read_geometric_queries(
        root / "queries.parquet", inputs.queries, inputs.layout.dimensions
    )
    _authenticate(root / "truth.parquet", inputs.truth)
    truth = _ground_truth(root / "truth.parquet")
    if queries.shape[0] != len(truth):
        raise ValueError("microcluster query/truth count differs")
    return queries, truth


def evaluate_cell(
    root: Path,
    out: Path,
    prefix: str,
    inputs: CellInputs = FROZEN_INPUTS,
) -> None:
    ids, _, membership = _read_inputs(root, inputs)
    representative = _read_sealed(root, prefix, inputs)
    pages = read_representatives(
        root / "representatives.parquet",
        representative,
        bytes.fromhex(inputs.layout.source.sha256),
        bytes.fromhex(inputs.membership.sha256),
    )
    if len(ids) != len(membership):
        raise ValueError("microcluster source/membership count differs")
    queries, truth = _read_queries_truth(root, inputs)
    evaluation = evaluate_representatives(
        pages, membership, queries, truth, inputs.limits
    )
    evidence = _remote(
        write_evidence(out / "evidence.parquet", evaluation), prefix, "evidence.parquet"
    )
    metrics = {
        field.name: getattr(evaluation, field.name)
        for field in dataclasses.fields(evaluation)
        if field.name != "samples"
    }
    result = {
        "schema": "borsuk-page-microcluster-result-v1",
        "claim_eligible": False,
        "source": dataclasses.asdict(inputs.layout.source),
        "membership": dataclasses.asdict(inputs.membership),
        "representatives": dataclasses.asdict(representative),
        "queries": dataclasses.asdict(inputs.queries),
        "truth": dataclasses.asdict(inputs.truth),
        "evidence": dataclasses.asdict(evidence),
        "limits": dataclasses.asdict(inputs.limits),
        "metrics": metrics,
        "number_queries": len(truth),
        "output_prefix": prefix.rstrip("/"),
    }
    (out / "result.json").write_bytes(_canonical_bytes(result))


def validate_cell(
    root: Path,
    prefix: str,
    source_commit: str,
    inputs: CellInputs = FROZEN_INPUTS,
) -> dict[str, int | str]:
    result = _read_canonical(root / "result.json")
    representative = _read_sealed(root, prefix, inputs)
    if (
        set(result)
        != {
            "schema",
            "claim_eligible",
            "source",
            "membership",
            "representatives",
            "queries",
            "truth",
            "evidence",
            "limits",
            "metrics",
            "number_queries",
            "output_prefix",
        }
        or result["schema"] != "borsuk-page-microcluster-result-v1"
        or result["claim_eligible"] is not False
        or result["source"] != dataclasses.asdict(inputs.layout.source)
        or result["membership"] != dataclasses.asdict(inputs.membership)
        or result["representatives"] != dataclasses.asdict(representative)
        or result["queries"] != dataclasses.asdict(inputs.queries)
        or result["truth"] != dataclasses.asdict(inputs.truth)
        or result["limits"] != dataclasses.asdict(inputs.limits)
        or result["output_prefix"] != prefix.rstrip("/")
    ):
        raise ValueError("microcluster result authority differs")
    ids, vectors, membership = _read_inputs(root, inputs)
    pages = read_representatives(
        root / "representatives.parquet",
        representative,
        bytes.fromhex(inputs.layout.source.sha256),
        bytes.fromhex(inputs.membership.sha256),
    )
    if pages != construct_representatives(membership, ids, vectors):
        raise ValueError("microcluster sealed construction differs")
    queries, truth = _read_queries_truth(root, inputs)
    if result["number_queries"] != len(truth):
        raise ValueError("microcluster result query count differs")
    try:
        evidence = ArtifactIdentity(**result["evidence"])
    except (TypeError, ValueError) as error:
        raise ValueError("microcluster result evidence differs") from error
    if evidence.uri != prefix.rstrip("/") + "/artifacts/evidence.parquet":
        raise ValueError("microcluster result evidence URI differs")
    replay = validate_evidence(
        root / "evidence.parquet",
        evidence,
        pages,
        membership,
        queries,
        truth,
        inputs.limits,
    )
    if result["metrics"] != replay:
        raise ValueError("microcluster result aggregates differ")
    validation = {
        "schema": "borsuk-page-microcluster-validation-v1",
        "claim_eligible": False,
        "decision": replay["decision"],
        "source_commit": source_commit,
        **replay,
    }
    (root / "validation.json").write_bytes(_canonical_bytes(validation))
    return replay


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("phase", choices=("construct", "evaluate", "validate"))
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--out", type=Path)
    parser.add_argument("--output-prefix", required=True)
    parser.add_argument("--source-commit")
    arguments = parser.parse_args()
    if arguments.phase == "construct":
        construct_cell(arguments.root, arguments.output_prefix)
    elif arguments.phase == "evaluate":
        if arguments.out is None:
            parser.error("--out is required for evaluate")
        evaluate_cell(arguments.root, arguments.out, arguments.output_prefix)
    else:
        if arguments.source_commit is None:
            parser.error("--source-commit is required for validate")
        validate_cell(arguments.root, arguments.output_prefix, arguments.source_commit)


if __name__ == "__main__":
    main()
