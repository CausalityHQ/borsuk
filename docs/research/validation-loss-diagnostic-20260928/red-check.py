"""Historical reproduction gate and coverage diagnostic; no new serving arm."""
import hashlib
import json
from pathlib import Path
import subprocess
import sys


def coverage(truth_pages, candidates, nominated, fetched):
    raise NotImplementedError("coverage not implemented")


def self_check():
    truth = [0, 1, 1, 2]
    assert coverage(truth, [0, 1, 2], [0, 2], [0, 1, 2]) == (4, 2, 4)
    assert coverage(truth, [0, 1, 2], [1], [1]) == (4, 2, 2)
    for candidates, nominated, fetched in [([0, 1], [2], [2]), ([0, 1], [1], [0]), ([0, 0], [0], [0])]:
        try:
            coverage(truth, candidates, nominated, fetched)
        except AssertionError:
            continue
        raise AssertionError("invalid page authority accepted")


if sys.argv[1:] == ["--self-check"]:
    self_check()
    print("coverage self-check passed")
    sys.exit(0)
