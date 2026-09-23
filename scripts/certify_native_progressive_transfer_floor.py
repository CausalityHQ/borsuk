"""Bind planned transfer minima to two terminal-closed 1M evidence files.

The closed remote validators establish physical range geometry. This checker
authenticates their terminal-bound plans and checks the arithmetic premises
used by the Lean transfer theorem; it does not repeat the remote validators.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import statistics
from pathlib import Path

MAXIMUM_GETS = 32
MAXIMUM_BYTES = 16_777_216


def _sha(body: bytes) -> str:
    return hashlib.sha256(body).hexdigest()


def _read_canonical(body: bytes) -> dict[str, object]:
    try:
        value = json.loads(body)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ValueError("transfer certificate JSON differs") from error
    if (
        type(value) is not dict
        or body != (json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n").encode()
    ):
        raise ValueError("transfer certificate canonical JSON differs")
    return value


def _receipt(terminal: dict[str, object], role: str, body: bytes) -> None:
    if terminal.get("status") != "complete" or terminal.get("exit_code") != 0:
        raise ValueError("transfer terminal status differs")
    artifacts = terminal.get("artifacts")
    receipt = artifacts.get(role) if type(artifacts) is dict else None
    if (
        type(receipt) is not dict
        or receipt.get("sha256") != _sha(body)
        or receipt.get("encoded_bytes") != len(body)
    ):
        raise ValueError("transfer artifact identity differs")


def _wave(plan: dict[str, object]) -> tuple[int, int]:
    encoded = plan.get("encoded_bytes")
    gets = plan.get("gets")
    if (
        type(encoded) is not int or not 0 < encoded <= MAXIMUM_BYTES
        or type(gets) is not int or not 0 < gets <= MAXIMUM_GETS
    ):
        raise ValueError("transfer wave budget differs")
    return encoded, gets


def certify_plans(
    code_body: bytes, paired_body: bytes, code_terminal_body: bytes,
    paired_terminal_body: bytes, *, expected_queries: int,
    code_terminal_sha256: str, paired_terminal_sha256: str,
) -> dict[str, object]:
    """Return exact plan-byte floors after terminal and query-roster checks."""
    if (
        type(expected_queries) is not int or expected_queries <= 0
        or _sha(code_terminal_body) != code_terminal_sha256
        or _sha(paired_terminal_body) != paired_terminal_sha256
    ):
        raise ValueError("transfer terminal identity differs")
    code_terminal = _read_canonical(code_terminal_body)
    paired_terminal = _read_canonical(paired_terminal_body)
    _receipt(code_terminal, "progressive-code-wave-plans", code_body)
    _receipt(paired_terminal, "progressive-paired-plans", paired_body)
    code = _read_canonical(code_body)
    paired = _read_canonical(paired_body)
    if (
        code.get("schema") != "borsuk-one-million-progressive-mirrored-code-projection-v1-plans"
        or paired.get("schema") != "borsuk-one-million-progressive-paired-score-v1-plans"
        or paired.get("prior_plans_sha256") != _sha(code_body)
        or type(code.get("samples")) is not list
        or type(paired.get("samples")) is not list
        or len(code["samples"]) != expected_queries
        or len(paired["samples"]) != expected_queries
    ):
        raise ValueError("transfer plan authority differs")
    lengths: dict[str, list[int]] = {name: [] for name in ("sign", "magnitude", "data", "total")}
    gets_total: list[int] = []
    for ordinal, (code_case, paired_case) in enumerate(
        zip(code["samples"], paired["samples"], strict=True)
    ):
        if (
            type(code_case) is not dict or type(paired_case) is not dict
            or code_case.get("query_ordinal") != ordinal
            or paired_case.get("query_ordinal") != ordinal
            or type(code_case.get("code")) is not dict
            or type(paired_case.get("two_bit")) is not dict
            or set(code_case["code"]) != {"sign", "magnitude"}
        ):
            raise ValueError("transfer query roster differs")
        sign = code_case["code"]["sign"]
        magnitude = code_case["code"]["magnitude"]
        if (
            type(sign) is not dict or type(magnitude) is not dict
            or sign.get("included_pages") != magnitude.get("included_pages")
            or sign.get("ranges") != magnitude.get("ranges")
            or sign.get("gets") != magnitude.get("gets")
        ):
            raise ValueError("transfer mirrored cover differs")
        sign_bytes, sign_gets = _wave(sign)
        magnitude_bytes, magnitude_gets = _wave(magnitude)
        data_bytes, data_gets = _wave(paired_case["two_bit"])
        lengths["sign"].append(sign_bytes)
        lengths["magnitude"].append(magnitude_bytes)
        lengths["data"].append(data_bytes)
        lengths["total"].append(sign_bytes + magnitude_bytes + data_bytes)
        gets_total.append(sign_gets + magnitude_gets + data_gets)
    minima = {name: min(values) for name, values in lengths.items()}
    return {
        "schema": "borsuk-progressive-transfer-floor-certificate-v1",
        "query_count": expected_queries,
        "code_terminal_sha256": code_terminal_sha256,
        "paired_terminal_sha256": paired_terminal_sha256,
        "code_plans_sha256": _sha(code_body),
        "paired_plans_sha256": _sha(paired_body),
        "minimum_bytes": minima,
        "conservative_floor_bytes": sum(minima[name] for name in ("sign", "magnitude", "data")),
        "maximum_bytes": {name: max(values) for name, values in lengths.items()},
        "median_total_bytes": statistics.median(lengths["total"]),
        "minimum_gets": min(gets_total),
        "maximum_gets": max(gets_total),
        "median_gets": statistics.median(gets_total),
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    for name in ("code-plans", "paired-plans", "code-terminal", "paired-terminal"):
        parser.add_argument(f"--{name}", type=Path, required=True)
    parser.add_argument("--code-terminal-sha256", required=True)
    parser.add_argument("--paired-terminal-sha256", required=True)
    parser.add_argument("--expected-queries", type=int, default=1000)
    args = parser.parse_args()
    result = certify_plans(
        args.code_plans.read_bytes(), args.paired_plans.read_bytes(),
        args.code_terminal.read_bytes(), args.paired_terminal.read_bytes(),
        expected_queries=args.expected_queries,
        code_terminal_sha256=args.code_terminal_sha256,
        paired_terminal_sha256=args.paired_terminal_sha256,
    )
    print(json.dumps(result, sort_keys=True, separators=(",", ":")))


if __name__ == "__main__":
    main()
