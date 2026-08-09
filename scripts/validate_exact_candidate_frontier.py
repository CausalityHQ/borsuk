#!/usr/bin/env python3
"""Evaluate the smallest exact refinement width meeting full-corpus quality."""

import argparse
import json
import traceback
from pathlib import Path

import validate_approximate_first_qualification as paired


def validate(root, manifest_path):
    manifest = json.loads(Path(manifest_path).read_text())
    paired_decision = paired.validate(root, manifest_path)
    points = []
    integrity_failures = {
        "treatment exact vectors",
        "treatment exact rerank time",
        "disk cache reads",
        "decoded cache hits",
    }
    for source in paired_decision["points"]:
        failures = [
            failure for failure in source["failures"] if failure in integrity_failures
        ]
        if source["control_mean_recall_at_10"] < manifest["required_mean_recall_at_10"]:
            failures.append("control mean recall")
        if source["control_p05_recall_at_10"] < manifest["required_p05_query_recall_at_10"]:
            failures.append("control p05 recall")
        if source["control_p95_ms"] > manifest["maximum_control_p95_ms"]:
            failures.append("control p95 ceiling")
        if source["control_average_exact_vectors"] != source["max_candidates"]:
            failures.append("exact candidate count")
        if source["control_average_backing_reads"] <= 0:
            failures.append("control backing reads")
        if source["control_average_backing_bytes"] <= 0:
            failures.append("control backing bytes")
        point = {
            key: value
            for key, value in source.items()
            if key
            in {
                "nprobe",
                "max_candidates",
                "queries",
                "control_mean_recall_at_10",
                "control_p05_recall_at_10",
                "control_p50_ms",
                "control_p95_ms",
                "control_average_backing_reads",
                "control_average_backing_bytes",
                "control_average_exact_vectors",
            }
        }
        point["failures"] = failures
        point["accepted"] = not failures
        points.append(point)
    eligible = [point for point in points if point["accepted"]]
    selected = min(
        eligible,
        key=lambda point: (
            point["max_candidates"],
            point["control_average_backing_reads"],
            point["control_average_backing_bytes"],
            point["control_p95_ms"],
            point["control_p50_ms"],
        ),
        default=None,
    )
    return {
        "protocol": manifest["protocol"],
        "identity": paired_decision["identity"],
        "accepted": selected is not None,
        "selected": selected,
        "points": points,
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("root", type=Path)
    parser.add_argument("manifest", type=Path)
    parser.add_argument("--decision", type=Path)
    args = parser.parse_args()
    try:
        decision = validate(args.root, args.manifest)
    except paired.ValidationError as error:
        print(f"invalid: {error}")
        return 2
    except Exception:  # noqa: BLE001
        traceback.print_exc()
        return 2
    encoded = json.dumps(decision, indent=2, sort_keys=True) + "\n"
    if args.decision:
        args.decision.write_text(encoded)
    print(encoded, end="")
    return 0 if decision["accepted"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
