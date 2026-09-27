"""Frozen V292 centroid candidates through unchanged V291 two-bit/SQ8 scoring."""

import argparse
import json
from pathlib import Path

from scripts.v291_two_stage_development import evaluate


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("raw", "archive", "root-sha", "layout", "sq8", "requests", "truth",
                 "candidates", "output", "plans-output"):
        parser.add_argument("--" + name, required=True, type=Path)
    args = parser.parse_args()
    result, plans = evaluate(args.raw, args.archive, args.root_sha, args.layout,
                             args.sq8, args.requests, args.truth, args.candidates)
    result["schema"] = "borsuk-v293-bounded-two-stage-v1"
    args.output.write_text(json.dumps(result, sort_keys=True, separators=(",", ":")) + "\n")
    args.plans_output.write_text("".join(json.dumps(p, sort_keys=True, separators=(",", ":")) + "\n"
                                        for p in plans))
    print(json.dumps(result, sort_keys=True))


if __name__ == "__main__":
    main()
