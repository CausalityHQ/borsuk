#!/usr/bin/env python3
"""Stream a fixed OPQ8 model into the original 1M physical page order."""

from __future__ import annotations

import dataclasses
import hashlib
import json
import struct
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

from scripts.native_geometric_layout_screen import ArtifactIdentity
from scripts.native_hundred_thousand_opq8_router import (
    DIMENSIONS,
    SUBSPACES,
    Opq8Model,
    encode_opq8_rows,
    read_opq8_model,
)
from scripts.native_one_million_group_selector import (
    MEMBERSHIP_DTYPE,
    SOURCE_ROLES,
    Group,
    _page_map,
    _source_schema,
)
from scripts.v97_row_width_screen import (
    ObjectIdentity,
    PageKey,
    _authenticate_object,
    _canonical_json_bytes,
    _read_page_run,
    _sha256_file,
)

SCHEMA = "borsuk-one-million-opq8-physical-codes-v1"


@dataclass(frozen=True, slots=True)
class PhysicalOpq8:
    groups: tuple[Group, ...]
    membership_ids: np.ndarray
    membership_groups: np.ndarray
    codes: np.ndarray
    model: Opq8Model
    seal: dict[str, object]


def _file_identity(path: Path) -> dict[str, object]:
    return {"bytes": path.stat().st_size, "sha256": _sha256_file(path)}


def _physical_ordinals(
    root: Path, identities: Mapping[str, ObjectIdentity],
) -> tuple[dict[int, int], str]:
    generation = json.loads((root / "generation.json").read_bytes())
    physical: dict[int, int] = {}
    ordered = hashlib.sha256()
    for role_index, role in enumerate(("base", "delta")):
        run = generation["runs"][role_index]
        if run["kind"] != role:
            raise ValueError("OPQ8 physical run order differs")
        _, pages, row_order = _read_page_run(
            root / f"{role}.arrow", run, role=role,
            dimensions=DIMENSIONS, identity=identities[role],
        )
        for page_ordinal in range(len(pages)):
            for row_id in row_order[PageKey(role, page_ordinal)]:
                if row_id in physical:
                    raise ValueError("OPQ8 physical rows overlap")
                physical[row_id] = len(physical)
                ordered.update(struct.pack("<BIQ", role_index, page_ordinal, row_id))
    return physical, ordered.hexdigest()


def build_1m_opq8(
    root: Path, out: Path, identities: Mapping[str, ObjectIdentity],
    model_identity: ArtifactIdentity, *, batch_rows: int = 4096,
) -> PhysicalOpq8:
    if set(identities) != SOURCE_ROLES or not 1 <= batch_rows <= 4096:
        raise ValueError("OPQ8 source contract differs")
    if (root / "queries.parquet").exists() or (root / "truth.parquet").exists():
        raise ValueError("OPQ8 source phase differs")
    for role, name in (
        ("source", "source.parquet"), ("generation", "generation.json"),
        ("base", "base.arrow"), ("delta", "delta.arrow"), ("router", "router.arrow"),
    ):
        _authenticate_object(role, root / name, identities[role])
    model_body = (root / "model.bin").read_bytes()
    if len(model_body) != model_identity.encoded_bytes or hashlib.sha256(model_body).hexdigest() != model_identity.sha256:
        raise ValueError("OPQ8 model identity differs")
    model = read_opq8_model(root / "model.bin")
    groups, id_to_group, page_order_sha = _page_map(root, identities)
    physical, independent_order_sha = _physical_ordinals(root, identities)
    if page_order_sha != independent_order_sha or set(physical) != set(id_to_group):
        raise ValueError("OPQ8 physical source order differs")
    source = pq.ParquetFile(root / "source.parquet")
    if source.schema_arrow != _source_schema():
        raise ValueError("OPQ8 source schema differs")
    out.mkdir(parents=True, exist_ok=True)
    if (out / "model.bin") != (root / "model.bin"):
        (out / "model.bin").write_bytes(model_body)
    count = len(physical)
    codes = np.memmap(out / "codes.bin", mode="w+", dtype=np.uint8, shape=(count, SUBSPACES))
    seen = np.zeros(count, dtype=np.bool_)
    for batch in source.iter_batches(batch_size=batch_rows):
        table = pa.Table.from_batches([batch])
        ids = table.column("feature_row_id").combine_chunks().to_numpy()
        values = table.column("embedding").combine_chunks()
        vectors = np.asarray(values.values.to_numpy(), dtype=np.float32).reshape(-1, DIMENSIONS)
        if values.null_count or len(vectors) != len(ids):
            raise ValueError("OPQ8 source rows differ")
        encoded = encode_opq8_rows(vectors, model)
        for index, row_id in enumerate(ids):
            ordinal = physical.get(int(row_id))
            if ordinal is None or seen[ordinal]:
                raise ValueError("OPQ8 source rows differ")
            codes[ordinal] = encoded[index]
            seen[ordinal] = True
    if not bool(np.all(seen)) or count != sum(group.row_count for group in groups):
        raise ValueError("OPQ8 source rows differ")
    codes.flush()
    del codes
    membership = np.empty(count, dtype=MEMBERSHIP_DTYPE)
    for index, row_id in enumerate(sorted(id_to_group)):
        membership[index] = row_id, id_to_group[row_id]
    (out / "membership.bin").write_bytes(membership.tobytes(order="C"))
    seal: dict[str, object] = {
        "schema": SCHEMA,
        "source_identities": {role: dataclasses.asdict(identities[role]) for role in sorted(SOURCE_ROLES)},
        "model_identity": dataclasses.asdict(model_identity),
        "page_order_sha256": page_order_sha,
        "groups": [dataclasses.asdict(group) for group in groups],
        "codes": _file_identity(out / "codes.bin"),
        "membership": _file_identity(out / "membership.bin"),
    }
    (out / "seal.json").write_bytes(_canonical_json_bytes(seal))
    return read_1m_opq8(out, identities, model_identity)


def read_1m_opq8(
    out: Path, identities: Mapping[str, ObjectIdentity], model_identity: ArtifactIdentity,
) -> PhysicalOpq8:
    body = (out / "seal.json").read_bytes()
    seal = json.loads(body)
    if (
        body != _canonical_json_bytes(seal)
        or set(seal) != {"schema", "source_identities", "model_identity", "page_order_sha256", "groups", "codes", "membership"}
        or seal["schema"] != SCHEMA
        or seal["source_identities"] != {role: dataclasses.asdict(identities[role]) for role in sorted(SOURCE_ROLES)}
        or seal["model_identity"] != dataclasses.asdict(model_identity)
    ):
        raise ValueError("OPQ8 source seal differs")
    model_path = out / "model.bin"
    model_body = model_path.read_bytes()
    if len(model_body) != model_identity.encoded_bytes or hashlib.sha256(model_body).hexdigest() != model_identity.sha256:
        raise ValueError("OPQ8 model identity differs")
    for name in ("codes", "membership"):
        if _file_identity(out / f"{name}.bin") != seal[name]:
            raise ValueError(f"OPQ8 {name} identity differs")
    groups = tuple(Group(**item) for item in seal["groups"])
    count = sum(group.row_count for group in groups)
    codes = np.frombuffer((out / "codes.bin").read_bytes(), dtype=np.uint8)
    membership = np.frombuffer((out / "membership.bin").read_bytes(), dtype=MEMBERSHIP_DTYPE)
    if (
        not groups or any(group.row_count <= 0 or not 1 <= group.end_page - group.first_page <= 8 for group in groups)
        or codes.size != count * SUBSPACES or membership.size != count
        or not np.all(membership["id"][1:] > membership["id"][:-1])
        or (count and int(np.max(membership["group"])) >= len(groups))
    ):
        raise ValueError("OPQ8 physical codes differ")
    return PhysicalOpq8(
        groups, membership["id"], membership["group"], codes.reshape(count, SUBSPACES),
        read_opq8_model(model_path), seal,
    )
