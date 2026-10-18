#!/usr/bin/env bash
# Prepare a Q1 diagnostic32 AB/BA recipe; does not execute serving or open data payloads.
# Inputs must be current configs for separately published, root-approved flat/frame generations.
# Repack before publication:
# build_two_bit_generation --repackage FLAT_DIR ROOT_SHA NEW_ID MEMORY_BYTES NEW_DIR framed_records_v1
# Root must first pass authenticated real-input admission and a separate disposable canary.
set -euo pipefail
if [[ $# != 6 ]]; then
  echo "usage: $0 FLAT_CONFIG FLAT_SHA FRAME_CONFIG FRAME_SHA RUNNER_SHA256 NEW_OUTPUT_DIR" >&2
  exit 2
fi
python3 -I - "$@" <<'PY'
import hashlib, json, pathlib, re, shlex, sys
flat_path, flat_pin, frame_path, frame_pin, runner_pin, output = sys.argv[1:]
def require(condition, message):
    if not condition:
        raise SystemExit(message)
require(re.fullmatch(r"[0-9a-f]{64}", runner_pin), "runner SHA256")
def read(path, pin):
    path = pathlib.Path(path)
    require(path.stat().st_size <= 65536, "config size")
    with path.open("rb") as file:
        data = file.read(65537)
    require(len(data) <= 65536 and hashlib.sha256(data).hexdigest() == pin, "config pin")
    config = json.loads(data)
    require(config["schema"] in ("borsuk-cohere-native-baseline-config-v19", "borsuk-cohere-native-baseline-config-v21"), "config schema")
    require(config["count"] >= 32, "query cohort size")
    config["schema"] = "borsuk-cohere-native-baseline-config-v19"
    config.pop("max_active_queries", None)
    config.pop("query_runtime", None)
    config["execution"] = {"mode": "diagnostic_panel", "ordinals": list(range(32)), "trace": False}
    return config
flat, frame = read(flat_path, flat_pin), read(frame_path, frame_pin)
require(flat["source_layout"] == {"layout": "flat_records"}, "flat layout")
require(frame["source_layout"]["layout"] == "framed_records_v1", "frame layout")
allowed = {"generation_prefix", "generation_root_sha256", "source_layout"}
require({k:v for k,v in flat.items() if k not in allowed} == {k:v for k,v in frame.items() if k not in allowed}, "paired input/transport/settings drift")
out = pathlib.Path(output).absolute()
out.mkdir(exist_ok=False)
pins = {}
configs = [("flat", flat), ("frame", frame)]
for arm, config in tuple(configs):
    smoke = dict(config)
    smoke["execution"] = dict(config["execution"], trace=True)
    configs.append((arm + "-smoke", smoke))
for arm, config in configs:
    data = (json.dumps(config, sort_keys=True, separators=(",", ":")) + "\n").encode()
    path = out / (arm + ".json")
    path.write_bytes(data)
    pins[arm] = hashlib.sha256(data).hexdigest()
    (out / (arm + ".sha256")).write_text(pins[arm] + "\n")
# Each run has a fresh output. The root's external RSS/cost gate still applies.
commands = ["#!/usr/bin/env bash", "set -euo pipefail",
            ': "${RUNNER:?set RUNNER to the frozen release check_cohere_native_baseline binary}"',
            "readonly RUNNER_SHA256=" + shlex.quote(runner_pin)]
verify_runner = 'printf \'%s  %s\n\' "$RUNNER_SHA256" "$RUNNER" | sha256sum --check --status'
header = commands.copy()
for index, arm in enumerate(("flat", "frame", "frame", "flat")):
    config, result, rss = out/(arm+".json"), out/(str(index)+"-"+arm+".jsonl"), out/(str(index)+"-"+arm+".rss")
    commands.append(verify_runner)
    commands.append('/usr/bin/time -v -o ' + shlex.quote(str(rss)) + ' "$RUNNER" ' +
                    shlex.quote(str(config)) + " " + pins[arm] + " " + shlex.quote(str(result)))
(out / "run-abba.sh").write_text("\n".join(commands) + "\n")
(out / "run-abba.sh").chmod(0o700)
smoke_commands = header.copy()
for arm in ("flat-smoke", "frame-smoke"):
    smoke_commands.append(verify_runner)
    smoke_commands.append('"$RUNNER" ' + shlex.quote(str(out/(arm+".json"))) +
                          " " + pins[arm] + " " + shlex.quote(str(out/(arm+".jsonl"))))
(out / "run-smoke.sh").write_text("\n".join(smoke_commands) + "\n")
(out / "run-smoke.sh").chmod(0o700)
(out / "pairing.txt").write_text("Chronological order: 0 flat, 1 frame, 2 frame, 3 flat.\nValidate each cell, then require exact parity separately for pairs 0/1 and 3/2; report both effects separately.\n")
print(out / "run-abba.sh")
PY
