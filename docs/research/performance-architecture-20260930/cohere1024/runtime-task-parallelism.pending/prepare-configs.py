#!/usr/bin/env python3
"""Prepare only. Root executes printed commands inside its measured resource controller."""
import hashlib
import json
import pathlib
import shlex
import sys

base_path, runner, destination = map(pathlib.Path, sys.argv[1:])
base = json.loads(base_path.read_bytes())
assert base["count"] == 1000 and base["rows"] == 1000000
assert base["dimensions"] == 1024 and base["k"] == 10
assert base["profile"] == "scale1m" and base["semantic_leaf_limit"] == 40
assert base["execution"] == {"mode": "full"}
assert base["serving"] == {"mode": "baseline"}
assert base["fetch_parallelism"] == 64 and base["source_authentication"] == "offload2"
assert base["max_memory_bytes"] == 2 * 1024**3
assert base.get("shared_byte_window") is None and base["sq8_range_split"] is None
assert base["max_active_queries"] in (1, 2)
assert base["schema"] in (
    "borsuk-cohere-native-baseline-config-v16",
    "borsuk-cohere-native-baseline-config-v17",
)
destination.mkdir()
arms = {}
for name in ("current_thread", "multi_thread2"):
    config = dict(base)
    config.update(schema="borsuk-cohere-native-baseline-config-v17",
                  max_active_queries=2, query_runtime=name)
    path = destination / (name + ".json")
    body = (json.dumps(config, separators=(",", ":")) + "\n").encode()
    path.write_bytes(body)
    pin = hashlib.sha256(body).hexdigest()
    (destination / (name + ".sha256")).write_text(pin + "\n")
    arms[name] = config, path, pin
assert {key for key in arms["current_thread"][0]
        if arms["current_thread"][0][key] != arms["multi_thread2"][0][key]} == {"query_runtime"}
print("# Root only: run each fresh process inside the same external8GiB measured controller.")
for cell, name in (("a1", "current_thread"), ("b1", "multi_thread2"),
                   ("b2", "multi_thread2"), ("a2", "current_thread")):
    _, path, pin = arms[name]
    print(shlex.join(map(str, (runner, path, pin, destination / (cell + ".jsonl")))))
