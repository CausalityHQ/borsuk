#!/usr/bin/env python3
"""Recount physical GT100 coverage for the terminal-closed V150 plan."""

import hashlib
import json
import struct

import boto3

BUCKET = "borsuk-bench-453182569524-euc1"
V122 = ("research/v122-deep-image-100k/"
        "afe07cb5a9ba8518263375595f589639fdf3f4f1/"
        "runs/v122-20260924T011355Z/a0001/artifacts/")
V150 = ("research/v150-unit-centroid-graph/"
        "1bb1559a775a241f1049f812f40a93d7f1c22fdc/"
        "runs/v150-20260924T121622Z/a0001/")
SHA = {
    "terminal": "c00e15182688a17b94f2182c30f3144d6cb9c2a5e57ecfa0235eb842790a7e07",
    "science": "d546bc92158ff29717dbeb9d9d2165991c0f18171ce9ec3b1706eb0487b0c293",
    "sq8": "c20dcb8058d2409791c6c584d9f078491d4c239acbe7c19350be7757d533e8df",
    "truth": "9e29a3e07ee2fe199fb17d8ec19b62f158d0cebe86ed23db14edecfa877c1a7f",
}


def main() -> None:
    s3 = boto3.Session(profile_name="causality", region_name="eu-central-1").client("s3")

    def get(key: str, size: int | None, sha: str) -> bytes:
        data = s3.get_object(Bucket=BUCKET, Key=key)["Body"].read()
        if (size is not None and len(data) != size) or hashlib.sha256(data).hexdigest() != sha:
            raise ValueError(f"historical input differs: {key}")
        return data

    terminal = json.loads(get(V150 + "terminal.json", None, SHA["terminal"]))
    if (terminal.get("status") != "complete" or terminal.get("exit_code") != 0
            or terminal.get("interrupted") is not False
            or terminal.get("phase") != "complete"):
        raise ValueError("V150 terminal differs")
    science_meta = terminal["artifacts"]["science.jsonl"]
    if science_meta["sha256"] != SHA["science"]:
        raise ValueError("V150 raw evidence differs")
    records = [json.loads(line) for line in get(
        V150 + "artifacts/science.jsonl", science_meta["bytes"], SHA["science"]
    ).splitlines()]
    sq8 = get(V122 + "built/sq8.bin", 10_800_000, SHA["sq8"])
    truth = get(V122 + "truth.npy", 800_128, SHA["truth"])
    if truth[:8] != b"\x93NUMPY\x01\x00":
        raise ValueError("GT NPY version differs")
    header_len = struct.unpack_from("<H", truth, 8)[0]
    header = truth[10:10 + header_len].decode()
    if "'<i8'" not in header or "(1000, 100)" not in header:
        raise ValueError("GT NPY geometry differs")
    gold = struct.unpack_from("<100000q", truth, 10 + header_len)
    inverse = [-1] * 100_000
    for position in range(100_000):
        source = struct.unpack_from("<q", sq8, position * 108)[0]
        if not 0 <= source < 100_000 or inverse[source] != -1:
            raise ValueError("SQ8 source ID mapping differs")
        inverse[source] = position
    if len(records) != 1000 or any(position < 0 for position in inverse):
        raise ValueError("query count or SQ8 mapping differs")

    selected_hits = 0
    range_hits = []
    for ordinal, record in enumerate(records):
        if (record["query_ordinal"] != ordinal
                or record["source_query_ordinal"] != ordinal + 9000):
            raise ValueError("query identity differs")
        selected = set(record["selected_pages"])
        ranges = record["ranges"]
        query_range_hits = 0
        for source in gold[ordinal * 100:(ordinal + 1) * 100]:
            if not 0 <= source < 100_000:
                raise ValueError("GT ID differs")
            position = inverse[source]
            selected_hits += position // 256 in selected
            offset = position * 108
            query_range_hits += any(first <= offset < last for first, last in ranges)
        range_hits.append(query_range_hits)
    print(json.dumps({
        "schema": "borsuk-v150-postterminal-gt-cover-v1",
        "cohort": "deep-image-96-angular-random100k",
        "split": "publication-test-ordinals-9000-9999-already-used",
        "truth_positions": 100_000,
        "selected_page_hits": selected_hits,
        "fetched_range_hits": sum(range_hits),
        "fetched_range_p05_hits_per_query": sorted(range_hits)[49],
        "queries_below_90_fetched_range_hits": sum(hits < 90 for hits in range_hits),
        "terminal_sha256": SHA["terminal"],
        "science_sha256": SHA["science"],
        "truth_sha256": SHA["truth"],
        "sq8_sha256": SHA["sq8"],
    }, sort_keys=True))


if __name__ == "__main__":
    main()
