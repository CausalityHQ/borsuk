"""Substitute frozen Rust nominee rosters into V114 query requests without GT."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from scripts.v114_exact_local_100k import _canonical, _sha256_file
from scripts.validate_v115_router_parity import FROZEN_REQUESTS_SHA256
from scripts.validate_v115_returned_replay import FROZEN_ROSTERS_SHA256


def compose(requests_path: Path, rosters_path: Path, output_path: Path) -> None:
    if (_sha256_file(requests_path) != FROZEN_REQUESTS_SHA256
            or _sha256_file(rosters_path) != FROZEN_ROSTERS_SHA256):
        raise ValueError("replay request or roster SHA-256 differs")
    requests = [json.loads(line) for line in requests_path.read_text().splitlines()]
    rosters = [json.loads(line) for line in rosters_path.read_text().splitlines()]
    if len(requests) != 1000 or len(rosters) != 1000:
        raise ValueError("replay query count differs")
    with output_path.open("x") as output:
        for ordinal, (request, roster) in enumerate(zip(requests, rosters)):
            if (request.get("query_ordinal") != ordinal
                    or roster.get("query_ordinal") != ordinal
                    or request.get("primary_count") != 100
                    or type(request.get("nominees")) is not list
                    or type(roster.get("nominees")) is not list
                    or len(request["nominees"]) != 512
                    or len(roster["nominees"]) != 512
                    or set(request["nominees"]) != set(roster["nominees"])):
                raise ValueError(f"replay roster differs at {ordinal}")
            output.write(_canonical({
                "query_ordinal": ordinal, "query": request["query"],
                "primary_count": 100, "nominees": roster["nominees"],
            }))


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--requests", required=True, type=Path)
    parser.add_argument("--rosters", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    compose(args.requests, args.rosters, args.output)


if __name__ == "__main__":
    main()
