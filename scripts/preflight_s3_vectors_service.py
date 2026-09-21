#!/usr/bin/env python3
"""Run one tiny authenticated S3 Vectors service lifecycle preflight."""

from __future__ import annotations

import argparse
import json
import math
import pathlib
import sys
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Sequence

if not __package__:
    sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))

from scripts.benchmark_s3_vectors_parquet import (
    _vectors_from_list_array,
    delete_service_resources,
)


@dataclass(frozen=True, slots=True)
class ServicePreflightReceipt:
    schema: str
    claim_eligible: bool
    region: str
    api_version: str
    boto3_version: str
    botocore_version: str
    numpy_version: str
    pyarrow_version: str
    vector_bucket: str
    index_name: str
    returned_keys: tuple[str, ...]
    cleanup_completed: bool


def _dependency_preflight() -> tuple[str, str, str, str]:
    import boto3
    import botocore
    import numpy as np
    import pyarrow as pa

    array = pa.FixedSizeListArray.from_arrays(
        pa.array([1.0, 0.0, 0.0, 1.0], type=pa.float32()),
        type=pa.list_(pa.field("item", pa.float32(), nullable=False), 2),
    )
    if _vectors_from_list_array(array, 2) != [[1.0, 0.0], [0.0, 1.0]]:
        raise ValueError("S3 Vectors conversion preflight differs")
    return boto3.__version__, botocore.__version__, np.__version__, pa.__version__


def _wait_for_index(client: object, bucket: str, index: str) -> None:
    import time

    deadline = time.monotonic() + 300
    while True:
        try:
            client.get_index(vectorBucketName=bucket, indexName=index)
            return
        except Exception as error:
            code = str(getattr(error, "response", {}).get("Error", {}).get("Code", ""))
            if code not in {"404", "NotFound", "NotFoundException"}:
                raise
            if time.monotonic() >= deadline:
                raise TimeoutError("S3 Vectors preflight index was not ready") from error
            time.sleep(2)


def run_service_preflight(
    client: object, bucket: str, index: str
) -> ServicePreflightReceipt:
    versions = _dependency_preflight()
    if (
        not bucket.startswith("borsuk-preflight-")
        or not 3 <= len(index) <= 63
        or any(character not in "abcdefghijklmnopqrstuvwxyz0123456789-" for character in index)
    ):
        raise ValueError("S3 Vectors preflight resource names differ")
    api_version = client.meta.service_model.api_version
    region = client.meta.region_name
    created_bucket = False
    created_index = False
    returned_keys: tuple[str, ...] = ()
    try:
        client.create_vector_bucket(vectorBucketName=bucket)
        created_bucket = True
        client.create_index(
            vectorBucketName=bucket,
            indexName=index,
            dataType="float32",
            dimension=2,
            distanceMetric="euclidean",
        )
        created_index = True
        _wait_for_index(client, bucket, index)
        client.put_vectors(
            vectorBucketName=bucket,
            indexName=index,
            vectors=[
                {"key": "probe-a", "data": {"float32": [1.0, 0.0]}},
                {"key": "probe-b", "data": {"float32": [0.0, 1.0]}},
            ],
        )
        response = client.query_vectors(
            vectorBucketName=bucket,
            indexName=index,
            queryVector={"float32": [1.0, 0.0]},
            topK=2,
            returnDistance=True,
            returnMetadata=False,
        )
        vectors = response.get("vectors") if type(response) is dict else None
        if type(vectors) is not list or len(vectors) != 2:
            raise ValueError("S3 Vectors preflight query response differs")
        returned_keys = tuple(vector.get("key") for vector in vectors)
        distances = tuple(vector.get("distance") for vector in vectors)
        if returned_keys != ("probe-a", "probe-b") or any(
            type(value) not in {int, float} or not math.isfinite(value)
            for value in distances
        ):
            raise ValueError("S3 Vectors preflight query order differs")
    finally:
        if created_bucket or created_index:
            delete_service_resources(client, bucket, index)
    return ServicePreflightReceipt(
        schema="borsuk-s3-vectors-service-preflight-v1",
        claim_eligible=False,
        region=region,
        api_version=api_version,
        boto3_version=versions[0],
        botocore_version=versions[1],
        numpy_version=versions[2],
        pyarrow_version=versions[3],
        vector_bucket=bucket,
        index_name=index,
        returned_keys=returned_keys,
        cleanup_completed=True,
    )


def canonical_receipt_bytes(receipt: ServicePreflightReceipt) -> bytes:
    return (
        json.dumps(asdict(receipt), sort_keys=True, separators=(",", ":")) + "\n"
    ).encode()


def main(argv: Sequence[str] | None = None) -> None:
    import boto3

    parser = argparse.ArgumentParser()
    parser.add_argument("--profile", default="causality")
    parser.add_argument("--region", default="eu-central-1")
    parser.add_argument("--bucket", required=True)
    parser.add_argument("--index", required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args(argv)
    client = boto3.Session(profile_name=args.profile, region_name=args.region).client(
        "s3vectors"
    )
    receipt = run_service_preflight(client, args.bucket, args.index)
    body = canonical_receipt_bytes(receipt)
    args.output.write_bytes(body)
    print(body.decode(), end="")


if __name__ == "__main__":
    main()
