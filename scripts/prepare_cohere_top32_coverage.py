#!/usr/bin/env python3
"""One fresh CoHere fixed64 top32 coverage falsifier; root owns config/launch.

CONFIG SHA REPO NEW_OUTPUT PREFIX; --replay CONFIG SHA REPO OUTPUT; --self-check.
No reselection, compilation, ANN scorer, returned recall, latency or GET claim.
The exact config/code/ref contract is exported as FIXED, CODE, EXPECTED, ARTIFACTS.
Nomination is read-only and conditionally sealed before the sole exhaustive GT100.
Replay authenticates identities and recomputes coverage only; it never runs GT.
"""
from contextlib import contextmanager
import gzip
import hashlib
import importlib
import json
import math
import os
from pathlib import Path
import platform
import resource
import shutil
import signal
import struct
import subprocess
import sys
import tempfile
import threading
import time

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from scripts import prepare_cohere_semantic_1m_panel as prior

require, canonical = prior.require, prior.canonical
SCHEMA = "borsuk-cohere-top32-preparation-v1"
BASE = prior.BASE + "cohere-top32-coverage/"
LIMITS = dict(memory_bytes=4 << 30, scratch_bytes=8 << 30, cpu=2, threads=2,
              swap_bytes=0, max_requests=256)
POLICIES = ("existing_first8_boundary1.15_max16", "fixed_top32_same_squared_distance_ranking")
BUILDER_SHA = "54071f8e7daf70589a416a2d9b8b4eefe42456a566bad767a77872b55c6e6c3f"
ORIGINAL_SOURCE = "c9ecb3ac7138978b45efde523c8e96a88811d559e50906c178fd4cb602fb9ff5"
QUALITY_SOURCE = "295a79de9a499cc388db14b4b78ac9fcd4f1eb8673ceb5c1222dfc7f119e9ae4"
FIXED = {
    "metadata_authority": {
        "path": "docs/research/performance-architecture-20260930/semantic-1m/cohere-top32-coverage/panel-tools/authority.json",
        "bytes": 202936,
        "sha256": "d07bef1f8552a0884cceb04c7404ad596516666aa1433ba06be7c26182b3027b"
    },
    "panel": {
        "path": "docs/research/performance-architecture-20260930/semantic-1m/cohere-top32-coverage/panel-tools/panel.json",
        "bytes": 58878,
        "sha256": "572e11afce1f778ababcbd17038ba8e5ac3ab2c0744e1075b56d6af2d9d07f49"
    },
    "root_freeze": {
        "path": "docs/research/performance-architecture-20260930/semantic-1m/cohere-top32-coverage/panel-tools/root-freeze.json",
        "bytes": 897,
        "sha256": "a79ebfdf80458c51136ab1b25a4330c7f31be289fbfc9621e74a204be1f30779"
    },
    "verification": {
        "path": "docs/research/performance-architecture-20260930/semantic-1m/cohere-top32-coverage/panel-tools/verification.json",
        "bytes": 7152,
        "sha256": "c8b770f6753df4f39eabe87cac750e49d7491766810ef46277377eab95d7ecdb"
    },
    "root_metadata_freeze": {
        "path": "docs/research/performance-architecture-20260930/semantic-1m/cohere-top32-coverage/root-metadata-freeze.json",
        "bytes": 703,
        "sha256": "85f0d92ba8357b770e95492dcae501e6fe872e47a0720c8cce06cbf944d61df1"
    },
    "preregister": {
        "path": "docs/research/performance-architecture-20260930/semantic-1m/cohere-top32-coverage/preregister.md",
        "bytes": 1833,
        "sha256": "8c134c4ecfc0662d555c6c197419ceb3da4623c23285973d9582005886858e13"
    },
    "prospective_protocol": {
        "path": "docs/research/performance-architecture-20260930/semantic-1m/cohere-top32-coverage/prospective-protocol.json",
        "bytes": 2378,
        "sha256": "b4547ffd131a72e1f5cd2e244a26d47743892c39da1c2cf3a7c48500c9c9048f"
    },
    "source_receipt": {
        "path": "docs/research/performance-architecture-20260930/semantic-1m/cohere-source-receipt.json",
        "key": "publication/v3/20260812/datasets/cohere-large-10m-768/attempts/0001/STAGING_COMPLETE.json",
        "bytes": 135298,
        "sha256": "0965aa0241199822dfac3410bba4edad5536ac0eb0aaa8ab83c216e8c5749a87"
    },
    "quality_config": {
        "path": "docs/research/performance-architecture-20260930/semantic-1m/cohere-quality/config.json",
        "bytes": 14302,
        "sha256": "435bb52600646820b47713ebb6a652c894407016e0a9c86cb15143ba4470814c"
    },
    "quality_source": {
        "path": "docs/research/performance-architecture-20260930/semantic-1m/cohere-quality/a0001/screen/source-qualification.json",
        "bytes": 45094,
        "sha256": "383c7fb5c301161972a06caf1e13ae26a79948dd4d1a7d8947d3421c9e048728"
    },
    "quality_terminal": {
        "path": "docs/research/performance-architecture-20260930/semantic-1m/cohere-quality/a0001/aws-terminal.json",
        "bytes": 3783,
        "sha256": "ab6b66ca67bb4f5f095bd448adf7fa9aa81632ee1530bf8f26947414a63d638d"
    }
}
EXPECTED = dict(schema=SCHEMA, authority_pending=False,
    bucket="borsuk-bench-453182569524-euc1", region="eu-central-1", rows=1_000_000,
    dimensions=768, k=100, queries=64, quality_peek_allowed=False,
    complete_historical_coverage=False, normalization=prior.NORMALIZATION,
    versions=prior.VERSIONS, limits=LIMITS, stage_limit_seconds=1800)
ARTIFACTS = ("queries.raw", "requests.jsonl", "panel.json", "duplicate-audit.json",
    "source-qualification.json", "sq8-ordinal-check.json", "builder-config.json",
    "build.log", "build-resources.txt", "build-resources.json", "generation-manifest.json",
    "nominate-config.json", "nomination.json", "nomination-seal.json", "truth.u32",
    "truth.i64", "oracle.json", "reduce-config.json", "coverage.json", "resources.json")
INPUTS = ("source.raw", "source-order.u64", "source-root.json", "source-sq8.bin",
          "consumed-queries.raw", "prior-queries.raw", "test-queries.raw")


def identity(path):
    path = Path(path)
    require(path.is_file() and not path.is_symlink(), "regular owned file required")
    return prior.local_identity(path)


def pointer(path):
    return dict(identity(path), path=str(Path(path).absolute()))


def authenticate(path, pin):
    got = identity(path)
    require((got["bytes"], got["sha256"]) == (pin["bytes"], pin["sha256"]),
            "authenticated length/hash differs: " + str(path))


def read_ref(repo, pin):
    path = repo / pin.get("archived_path", pin["path"])
    require(not path.is_symlink(), "symlink authority")
    path = prior.repo_path(repo, str(path.relative_to(repo)))
    require(path.is_file() and path.stat().st_size <= 16 << 20, "bounded regular authority required")
    if "archived_path" in pin:
        require(pin["archived_path"] == pin["path"] + ".gz", "archive path differs")
        with gzip.open(path, "rb") as stream:
            body = stream.read((16 << 20) + 1)
    else:
        body = path.read_bytes()
    require(len(body) <= 16 << 20 and len(body) == pin["bytes"]
            and hashlib.sha256(body).hexdigest() == pin["sha256"], "authority body identity differs")
    return body


def publish(path, value):
    """Sync, make read-only, then publish by hard link without overwriting."""
    path = Path(path)
    body = value if isinstance(value, bytes) else canonical(value)
    with tempfile.NamedTemporaryFile(dir=path.parent, prefix=".top32-", delete=False) as stream:
        temporary = Path(stream.name)
        try:
            stream.write(body); stream.flush(); os.fchmod(stream.fileno(), 0o444)
            os.fsync(stream.fileno())
            os.link(temporary, path)
            fd = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
            try:
                os.fsync(fd)
            finally:
                os.close(fd)
        finally:
            temporary.unlink(missing_ok=True)
    return pointer(path)


def code_roster(repo):
    quality = json.loads(read_ref(repo, FIXED["quality_config"]))
    return tuple(sorted(set((*prior.CODE, *quality["code_sha256"],
        "scripts/prepare_cohere_top32_coverage.py", "scripts/check_semantic_binary_coverage.py",
        "scripts/select_cohere_fresh64_coverage.py",
        "scripts/check_semantic_router_coverage.py", "crates/borsuk/src/semantic_unit_router.rs",
        "crates/borsuk/src/sq8_source.rs"))))


CODE = code_roster(Path(__file__).resolve().parents[1])


def read_config(path, sha, repo):
    require(__debug__, "optimized Python disables assurance checks")
    require(identity(path)["bytes"] <= 1 << 20, "config byte cap")
    authenticate(path, dict(identity(path), sha256=sha))
    config = json.loads(Path(path).read_bytes())
    extra = {"code_sha256", "refs", "corpus", "consumed_queries", "registered_test",
             "prior_unit_sha256", "builder"}
    require(set(config) == set(EXPECTED) | extra
            and all(canonical(config.get(k)) == canonical(v) for k, v in EXPECTED.items()),
            "configuration contract differs")
    require(Path(__file__).resolve() == repo / "scripts/prepare_cohere_top32_coverage.py", "adapter origin")
    require(set(config["code_sha256"]) == set(code_roster(repo)), "exact code roster differs")
    for name, digest in config["code_sha256"].items():
        require(identity(prior.repo_path(repo, name))["sha256"] == digest, "code identity differs: " + name)
    return config


def dependencies(repo, config):
    # Authenticate the whole union before executing the original helper closure.
    if not hasattr(prior, "shared"):
        # The legacy audit knows only its original closure. Our adapter has already
        # authenticated itself and is audited with the complete union below.
        own = sys.modules.pop(__name__, None) if __name__.startswith("scripts.") else None
        try:
            prior.dependencies(repo, {n: config["code_sha256"][n] for n in prior.CODE})
        finally:
            if own is not None:
                sys.modules[__name__] = own
    native = importlib.import_module("scripts.run_native_semantic_1m_quality")
    coverage = importlib.import_module("check_semantic_binary_coverage")
    require(Path(coverage.__file__).resolve() == repo / "scripts/check_semantic_binary_coverage.py",
            "coverage import origin differs")
    require(platform.machine() == "x86_64", "qualified builder requires x86_64")
    for name in native.THREAD_ENV:
        os.environ[name] = "2"
    for name, module in list(sys.modules.items()):
        if (name.startswith("scripts.") or name in
                ("build_cohere_1m_source", "check_semantic_binary_coverage", "check_semantic_router_coverage")):
            path = Path(module.__file__).resolve()
            require(path.is_relative_to(repo), "helper import origin differs")
            require(str(path.relative_to(repo)) in config["code_sha256"], "unpinned imported helper")
    return native, coverage


def validate_metadata(data):
    a, p, f, root = (data[n] for n in ("metadata_authority", "panel", "root_freeze", "root_metadata_freeze"))
    require(a["schema"] == "borsuk-cohere-top32-fresh64-metadata-authority-v1"
            and p["schema"] == "borsuk-cohere-top32-fresh64-locators-v1"
            and f["schema"] == "borsuk-cohere-top32-fresh64-root-freeze-v1"
            and root["schema"] == "borsuk-cohere-top32-root-metadata-freeze-v1", "top32 metadata schema")
    require(root["root_pending"] is False and root["input_artifacts_authenticated"] is True
            and root["independent_seed_rank_mapping_verified"] is True
            and root["prior64_excluded"] is True and root["quality_peek_allowed"] is False
            and root["metadata_only"] is True and root["ann_measured"] is False
            and root["ground_truth_constructed"] is False and root["old_fail_preserved"] is True
            and root["panel"] == FIXED["panel"] and root["population"] == 8_996_936
            and root["count"] == 64, "root metadata freeze differs")
    # panel-tools intentionally remain root_pending=true; the separate root receipt clears admission.
    for body in (a, p, f):
        require(body["authority_pending"] is False and body["metadata_only"] is True
                and body["vector_or_truth_bodies_opened"] is False
                and body["ground_truth_constructed"] is False and body["ann_measured"] is False
                and body["qualification"] is False and body["complete_historical_coverage"] is False
                and body["original_quality_status"] == "FAIL"
                and body["original_quality_fail_unchanged"] is True, "metadata scope differs")
    selected, spec = p["selected"], a["selection"]
    require(len(selected) == 64 and len({r["source_ordinal"] for r in selected}) == 64
            and len({r["shard_ordinal"] for r in selected}) == f["selected_shards"] == 62
            and f["selection_count"] == 64
            and prior.value_sha(selected) == p["selected_sha256"] == f["selected_locators_sha256"]
                == "583067ac7d7d88b25400f9b7d80026767589a8506e07dd970bddb003549e2dad"
            and p["authority_sha256"] == f["authority_sha256"] == FIXED["metadata_authority"]["sha256"]
            and f["panel_sha256"] == FIXED["panel"]["sha256"]
            and p["selection"] == spec and p["corpus"] == a["corpus"]
            and p["prior64_vector_hashes"] == a["prior64_vector_hashes"]
            and p["consumed_query_ledger_sha256"] == prior.value_sha(a["consumed_query_ledger"])
            and spec["population_size"] == 8_996_936 and spec["count"] == 64
            and spec["seed_text"] == "borsuk-cohere-first1m-top32-coverage-fresh64-v1", "fixed locator binding")
    previous = a["prior64_vector_hashes"]
    old = data["previous_duplicate_audit"]
    require(previous["query_ordinals"] == list(range(64))
            and previous["source_ordinals"] == a["previous_source_ordinals"]
            and previous["raw_sha256"] == old["raw_sha256"]
            and previous["normalized_sha256"] == old["unit_sha256"]
            and previous["normalization"] == prior.NORMALIZATION
            and previous["must_check_before_ground_truth"] is True, "prior64 duplicate authority differs")
    for ordinal, row in enumerate(selected):
        source = prior.selector.rank_to_ordinal(row["eligible_rank"], spec["eligible_intervals"])
        index, local = prior.selector.locate(source, a["ordered_train_shards"])
        shard = a["ordered_train_shards"][index]
        expected = dict(query_ordinal=ordinal, eligible_rank=row["eligible_rank"], source_ordinal=source,
            shard_ordinal=index, shard_key=shard["key"], shard_sha256=shard["sha256"],
            shard_bytes=shard["bytes"], local_row=local, embedding_column="emb")
        require(row == expected and source not in previous["source_ordinals"]
                and not any(e["split"] == "train" and "interval" in e
                            and e["interval"][0] <= source < e["interval"][1]
                            for e in a["consumed_query_ledger"]), "changed/consumed locator; STOP no replacement")


def load_inputs(config, repo):
    require(all(config["refs"].get(n) == p for n, p in FIXED.items()), "fixed refs differ")
    data = {n: json.loads(read_ref(repo, p)) for n, p in FIXED.items() if n != "preregister"}
    read_ref(repo, FIXED["preregister"])
    a = data["metadata_authority"]
    refs = dict(FIXED)
    for roster in (a["proofs"], a["input_pins"]):
        for n, p in roster.items():
            require(n not in refs or refs[n] == p, "inconsistent repeated proof name")
            refs[n] = p
    require(config["refs"] == refs, "exact source/consumption ref roster differs")
    for n, p in refs.items():
        body = read_ref(repo, p)
        if n != "preregister":
            data[n] = json.loads(body)
    original = data["source_population_authority"]
    old_config = dict(config, refs=dict(prior.FIXED, **original["proofs"]))
    old = prior.load_inputs(old_config, repo)  # Original source/producer/ledger checks, without RNG or GT.
    require(a["corpus"] == original["corpus"] and a["ordered_train_shards"] == original["ordered_train_shards"]
            and a["proofs"] == original["proofs"] and a["consumed_query_ledger"][:-1] == original["consumed_query_ledger"]
            and a["consumed_query_ledger"][-1] == dict(split="train", source_ordinals=a["previous_source_ordinals"],
                count=64, panel_pin=data["metadata_authority"]["input_pins"]["previous_consumed_panel"],
                duplicate_audit_pin=data["metadata_authority"]["input_pins"]["previous_duplicate_audit"])
            and a["old_consumed_panel"] == original["old_consumed_panel"], "original population identity differs")
    require(config["corpus"]["raw"]["bytes"] == 3_072_000_000
            and config["builder"] == data["quality_config"]["binaries"]["builder"], "raw/builder authority differs")
    validate_metadata(data)
    require(identity(repo / "scripts/select_cohere_fresh64_coverage.py")["sha256"]
            == data["root_metadata_freeze"]["selector_sha256"], "frozen selector source identity differs")
    return data, old


def builder_authority(repo, data):
    """Authenticate archived assurance, without comparing it to today's native tree."""
    q, proof = data["quality_config"], data["quality_source"]
    refs = q["refs"]
    names = ("source_manifest", "implementation_verification", "implementation_terminal",
             "implementation_receipt", "full_verification", "full_terminal", "full_receipt")
    values = {n: json.loads(read_ref(repo, refs[n])) for n in names}
    original = values["source_manifest"]["source_sha256"]
    require(len(original) == len(proof["native_source_sha256"]) == proof["source_file_count"] == 399
            and prior.value_sha(original) == ORIGINAL_SOURCE
            and prior.value_sha(proof["native_source_sha256"]) == QUALITY_SOURCE
            and proof["original_source_identity_sha256"] == ORIGINAL_SOURCE
            and proof["source_identity_sha256"] == QUALITY_SOURCE
            and proof["source_commit"] == data["quality_terminal"]["source_commit"]
            and proof["source_archive_sha256"] == data["quality_terminal"]["source_archive_sha256"]
            and proof["native_rebuilt"] is proof["current_whole_tree_full_execution"] is False
            and original["crates/borsuk/src/bin/build_two_bit_generation.rs"]
                == proof["native_source_sha256"]["crates/borsuk/src/bin/build_two_bit_generation.rs"],
            "archived native source map differs")
    for kind in ("implementation", "full"):
        v, t, r = (values[kind + "_" + n] for n in ("verification", "terminal", "receipt"))
        require(v["qualified"] is True and v["exit_status"] == 0
                and t["status"] == t["phase"] == "complete"
                and t["exit_code"] == t["original_exit_code"] == r["exit_status"] == r["gate_status"] == 0
                and r["qualified"] is r["command_completed"] is r["command_started"] is r["source_unchanged"] is True
                and r["source_sha256"] == original
                and v["source_identity_sha256"] == t["source_identity_sha256"]
                    == r["source_identity_sha256"] == ORIGINAL_SOURCE
                and t["artifacts"]["workspace-receipt.json"] == {k: refs[kind + "_receipt"][k] for k in ("bytes", "sha256")}
                and t["artifacts"]["native-source-manifest.json"] == {k: refs["source_manifest"][k] for k in ("bytes", "sha256")}
                and v["oom_kills"] == v["swap_peak_bytes"] == 0, "original build/full assurance differs")
    require(values["full_verification"]["actual_full_workspace_execution"] is True
            and values["full_receipt"]["command"][-5:] == ["test", "--release", "--locked", "--workspace", "--all-targets"],
            "original full assurance scope differs")
    pin = q["binaries"]["builder"]
    require(pin["sha256"] == proof["binary_sha256"]["builder"] == BUILDER_SHA
            and {k: pin[k] for k in ("bytes", "sha256")} == values["implementation_terminal"]["artifacts"]["binaries/build_two_bit_generation"],
            "qualified builder binary differs")
    body = read_ref(repo, pin)
    return body, dict(schema="borsuk-cohere-top32-archived-builder-assurance-v1",
        original_native_source_sha256=original, archived_quality_native_source_sha256=proof["native_source_sha256"],
        original_source_identity_sha256=ORIGINAL_SOURCE, archived_quality_source_identity_sha256=QUALITY_SOURCE,
        source_file_count=399, archived_assurance_refs={n: refs[n] for n in names},
        original_full_workspace_execution_reused=True, current_native_tree_qualified=False,
        current_whole_tree_full_execution=False, native_rebuilt=False, scorer_invocations=0,
        builder_binary=pin, builder_commit=values["implementation_terminal"]["source_commit"],
        builder_source_archive_sha256=values["implementation_terminal"]["source_archive_sha256"],
        builder_native_source_commit=values["source_manifest"]["native_source_commit"],
        source_commit=proof["source_commit"], source_archive_sha256=proof["source_archive_sha256"])


def duplicate_audit(queries, data, out, accounting):
    raw, normalized = prior.shared.vector_hashes(queries)
    previous = data["metadata_authority"]["prior64_vector_hashes"]
    require(not set(raw).intersection(previous["raw_sha256"])
            and not set(normalized).intersection(previous["normalized_sha256"]),
            "new panel duplicates previous64 raw/f64 vector; STOP no replacement")
    report = prior.audit(queries, out, accounting=accounting)
    report.update(schema="borsuk-cohere-top32-duplicate-audit-v1", previous64_rows_audited=64,
        previous64_authority=previous, selected_locators_sha256=data["panel"]["selected_sha256"])
    return report


class Accounting(prior.Accounting):
    def __init__(self, config, out, group):
        super().__init__(config, out, group, limits=config["limits"])
        self.deadline = None
        self.native_active = False
        self.build_invocations = self.oracle_invocations = 0
        self.baseline_events = self.events()
        self.native_run = lambda *a, **kw: subprocess.run(*a, **kw, timeout=self.remaining())

    def events(self):
        return {} if self.group is None else dict(line.split() for line in (self.group / "memory.events").read_text().splitlines())

    def remaining(self):
        require(self.deadline is not None, "operation outside bounded stage")
        left = self.deadline - time.monotonic()
        require(left > 0, "preparation stage deadline exceeded")
        return left

    def checkpoint(self, reserve=0):
        super().checkpoint(reserve)
        if self.deadline is not None:
            self.remaining()
        if self.group is not None:
            require((self.group / "memory.swap.peak").read_text().strip() == "0", "cgroup swap observed")
            require(int((self.group / "memory.peak").read_text()) <= self.limits["memory_bytes"], "aggregate cgroup memory exceeded")
            require(all(int(v) == int(self.baseline_events.get(k, 0)) for k, v in self.events().items()
                        if k in ("oom", "oom_kill", "max")), "cgroup memory admission failure")

    @contextmanager
    def stage(self, name):
        require(self.deadline is None, "overlapping preparation stages")
        self.deadline = time.monotonic() + self.config["stage_limit_seconds"]
        def expired(*unused):
            raise ValueError("preparation deadline/resource monitor stopped stage: " + name)
        previous = signal.signal(signal.SIGALRM, expired)
        stop = threading.Event()
        failures = []
        def monitor():
            while not stop.wait(.25):
                try:
                    self.checkpoint()
                except FileNotFoundError:
                    continue  # One owned temporary file may be removed between stat and open.
                except Exception as error:
                    failures.append(str(error))
                    if not (self.native_active and time.monotonic() >= self.deadline):
                        os.kill(os.getpid(), signal.SIGALRM)
                    return
        observer = threading.Thread(target=monitor, daemon=True)
        observer.start(); signal.setitimer(signal.ITIMER_REAL, self.config["stage_limit_seconds"])
        try:
            with super().stage(name):
                yield
            require(not failures, "resource observer failure: " + str(failures))
        finally:
            signal.setitimer(signal.ITIMER_REAL, 0)
            stop.set(); observer.join(timeout=2)
            signal.signal(signal.SIGALRM, previous); self.deadline = None
            require(not observer.is_alive(), "resource observer still running")

    def report(self, passed, cleanup=None):
        return dict(super().report(passed, cleanup), schema="borsuk-cohere-top32-preparation-resources-v1",
            build_invocations=self.build_invocations, oracle_invocations=self.oracle_invocations,
            scorer_invocations=0, stage_limit_seconds=self.config["stage_limit_seconds"],
            scratch_sample_interval_seconds=.25, memory_events=self.events(),
            aggregate_memory_peak_bytes=None if self.group is None else int((self.group / "memory.peak").read_text()),
            coverage_only=True, returned_recall_measured=False, cold_http_measured=False,
            physical_s3_query_gets_measured=False)


def current_group():
    return Path("/sys/fs/cgroup") / Path("/proc/self/cgroup").read_text().strip().split("0::")[-1].lstrip("/")


def write_queries(out, queries):
    require(queries.shape == (64, 768) and len(queries.tobytes()) == 196608, "fixed64 FP32 query geometry")
    publish(out / "queries.raw", queries.tobytes())
    body = b"".join(canonical(dict(ordinal=i, query=row.tolist())) + b"\n" for i, row in enumerate(queries))
    publish(out / "requests.jsonl", body)


def write_truth(out, truth):
    require(truth.shape == (64, 100) and truth.dtype == prior.np.dtype("<u4"), "oracle GT100 geometry/dtype")
    publish(out / "truth.u32", truth.tobytes())
    publish(out / "truth.i64", truth.astype("<i8").tobytes())
    prior.shared.check_outputs(out)


def generation_inputs(out, config, native):
    generation = out / "generation"
    manifest = json.loads((generation / "manifest.json").read_bytes())
    d = manifest["discovery"]
    require(manifest["schema"] == "borsuk-two-bit-generation-v8" and manifest["generation"] == 1
            and manifest["base_epoch"] == 0 and d["mode"] == "semantic" and d["profile"] == "fresh1m"
            and d["source_sha256"] == config["corpus"]["raw"]["sha256"]
            and d["source_order_sha256"] == config["corpus"]["order"]["sha256"]
            and d["sq8_sha256"] == manifest["sq8_object_sha256"] == config["corpus"]["sq8"]["sha256"],
            "genuine semantic generation source binding differs")
    require((out / "build.log").read_text().splitlines()[-1] == identity(generation / "manifest.json")["sha256"],
            "native builder root digest differs")
    result = {}
    for name, field in (("root", "router_root"), ("membership", "membership"), ("leaves", "leaves")):
        path = generation / "router" / (name + ".bin")
        pin = dict(bytes=d[name + "_bytes"], sha256=d[name + "_sha256"])
        authenticate(path, pin); result[field] = pointer(path)
    publish(out / "generation-manifest.json", (generation / "manifest.json").read_bytes())
    result["source"] = dict(profile=d["profile"], schema=d["input_schema"],
        root_sha256=d["input_root_sha256"], centroids_sha256=d["centroids_sha256"], rows=1_000_000, dimensions=768)
    return result


def check_nomination(report, panel):
    require(report["schema"] == "borsuk-semantic-binary-coverage-nomination-v1"
            and report["phase"] == "nomination_frozen_before_truth" and report["truth_opened"] is False
            and report["count"] == 64 and report["policies"] == list(POLICIES)
            and report["panel_selected_sha256"] == panel["selected_sha256"]
            and len(report["records"]) == 64, "all64 both-policy nomination required before GT")
    for i, record in enumerate(report["records"]):
        require(record["query_ordinal"] == i and record["source_ordinal"] == panel["selected"][i]["source_ordinal"]
                and set(record["policies"]) == set(POLICIES), "nomination query order/policies differ")


def seal(config, out, prefix, files, accounting):
    expected = {n: identity(out / n) for n in files}
    accounting.sizes.update({f"{prefix}/sealed/{n}": p["bytes"] for n, p in expected.items()})
    return prior.shared.seal_readback(config["bucket"], prefix, out, expected)


def coverage_status(report):
    hits = report["summary"][POLICIES[1]]["page_closure_hits10"]
    require(type(hits) is int and 0 <= hits <= 640, "top32 coverage denominator differs")
    status = "GO-for-native-investigation" if hits >= 608 else "FAIL"
    require(report["status"] == status, "coverage threshold/status differs")
    return status


def cleanup_failure(out):
    removed = []
    for name in (*INPUTS, "builder", "generation", "shard-scratch", "aws-debug.tmp", "all-consumed.raw",
                 "seal-readback.json", "decision.json"):
        path = out / name
        if path.is_dir() and not path.is_symlink():
            shutil.rmtree(path); removed.append(name)
        elif path.exists() or path.is_symlink():
            path.unlink(); removed.append(name)
    return dict(removed=removed, owned_heavy_inputs_remaining=False,
                remote_conditional_objects_may_remain=True, replacement_allowed=False)


def run(config_path, sha, repo, out, prefix):
    repo, out, config_path = Path(repo).resolve(), Path(out).absolute(), Path(config_path).absolute()
    config = read_config(config_path, sha, repo)
    native, coverage = dependencies(repo, config)
    data, _ = load_inputs(config, repo)
    binary, proof = builder_authority(repo, data)
    require(prefix and prefix == prefix.strip("/") and ".." not in prefix.split("/")
            and not any(c.isspace() for c in prefix), "invalid fresh output prefix")
    require(not out.exists() and not out.is_symlink() and not out.resolve().is_relative_to(repo), "fresh external output required")
    group = current_group(); prior.cgroup_limits(group, config["limits"])
    out.mkdir()
    accounting = Accounting(config, out, group)
    try:
        accounting.checkpoint(7 << 30)
        with prior.accounted_helpers(accounting):
            with accounting.stage("authenticated_inputs"):
                for name, pin in (("source.raw", config["corpus"]["raw"]),
                    ("source-order.u64", config["corpus"]["order"]),
                    ("source-root.json", config["corpus"]["root_manifest"]),
                    ("source-sq8.bin", config["corpus"]["sq8"]),
                    ("consumed-queries.raw", config["consumed_queries"])):
                    prior.download(config, pin, out / name, accounting)
                prior.verify_order(out, config)
                check = native.ordinal_check(out / "source-sq8.bin", out / "source-order.u64")
                publish(out / "sq8-ordinal-check.json", check)
            with accounting.stage("fixed_locator_extraction"):
                queries, sources = prior.acquire_vectors(config, data, out, accounting)
            with accounting.stage("duplicates_before_truth"):
                audit = duplicate_audit(queries, data, out, accounting)
                audit.update(authenticated_query_sources=sources, config_sha256=sha,
                             inputs={n: identity(out / n) for n in INPUTS})
                publish(out / "duplicate-audit.json", audit)
                write_queries(out, queries)
                publish(out / "panel.json", read_ref(repo, FIXED["panel"]))
                publish(out / "source-qualification.json", proof)
            with accounting.stage("one_genuine_semantic_build"):
                historical = json.loads((out / "source-root.json").read_bytes())
                require(historical["schema"] == "borsuk-two-bit-generation-v4"
                        and historical["canonical"]["rows"] == 1_000_000 and historical["canonical"]["dimensions"] == 768
                        and historical["sq8_object_sha256"] == config["corpus"]["sq8"]["sha256"]
                        and historical["sq8_object_key"] == config["corpus"]["sq8"]["key"]
                        and historical["generation"] == 1 and historical["base_epoch"] == 0
                        and all(len(historical[n]) == 768 and all(type(v) in (int, float) and math.isfinite(v)
                                for v in historical[n]) for n in ("low", "step"))
                        and all(v > 0 for v in historical["step"]), "original root/low/step differs")
                head = native.local_head(out / "source-sq8.bin")
                builder = dict(discovery="semantic", semantic_profile="fresh1m", rows=1_000_000, dimensions=768,
                    generation=1, base_epoch=0, raw=str(out / "source.raw"), raw_sha256=config["corpus"]["raw"]["sha256"],
                    sq8=str(out / "source-sq8.bin"), sq8_sha256=config["corpus"]["sq8"]["sha256"],
                    order=dict(path=str(out / "source-order.u64"), sha256=config["corpus"]["order"]["sha256"]),
                    low=historical["low"], step=historical["step"],
                    sq8_object_key="coverage/objects/" + config["corpus"]["sq8"]["sha256"], sq8_etag=head["etag"])
                publish(out / "builder-config.json", builder)
                publish(out / "builder", binary); (out / "builder").chmod(0o500)
                authenticate(out / "builder", config["builder"])
                accounting.build_invocations += 1
                # run_process owns timeout and process-group reaping. A second
                # deadline signal must not interrupt its timeout cleanup.
                signal.setitimer(signal.ITIMER_REAL, 0)
                accounting.native_active = True
                try:
                    result = native.run_process([str(out / "builder"), str(out / "builder-config.json"),
                        identity(out / "builder-config.json")["sha256"], str(native.PAYLOAD), str(out / "generation")],
                        out / "build.log", accounting.remaining(), out / "build-resources.txt")
                finally:
                    accounting.native_active = False
                signal.setitimer(signal.ITIMER_REAL, accounting.remaining())
                require(result["exit_status"] == 0 and result["process_cleanup"] is True
                        and result["wall_seconds"] <= config["stage_limit_seconds"]
                        and result["process_peak_rss_kib"] * 1024 <= LIMITS["memory_bytes"], "bounded native build failed")
                require(native.local_head(out / "source-sq8.bin") == head, "SQ8 metadata drift")
                accounting.checkpoint()
                publish(out / "build-resources.json", dict(result, preparation_resources=accounting.report(True)))
                generated = generation_inputs(out, config, native)
                (out / "builder").unlink()
            with accounting.stage("both_policy_nomination_and_seal_before_truth"):
                require(not (out / "truth.i64").exists() and not (out / "truth.u32").exists(), "truth before nomination")
                nomination_config = dict(generated, schema="borsuk-semantic-binary-coverage-nominate-config-v1",
                    order=pointer(out / "source-order.u64"), requests=pointer(out / "requests.jsonl"),
                    panel=pointer(out / "panel.json"), protocol=dict(FIXED["prospective_protocol"],
                        path=str(repo / FIXED["prospective_protocol"]["path"])),
                    provenance=dict(source_commit=proof["source_commit"], source_archive_sha256=proof["source_archive_sha256"],
                        builder_commit=proof["builder_commit"], builder_binary_sha256=BUILDER_SHA,
                        source_identity=pointer(out / "source-qualification.json"), resource_metadata=pointer(out / "build-resources.json")))
                nomination_config_pin = publish(out / "nominate-config.json", nomination_config)
                nomination = coverage.nominate(nomination_config, nomination_config_pin)
                check_nomination(nomination, data["panel"])
                nomination_pin = publish(out / "nomination.json", nomination)
                receipt = seal(config, out, prefix + "/nomination", ("nomination.json",), accounting)
                publish(out / "nomination-seal.json", receipt)
            with accounting.stage("one_exhaustive_f64_gt100"):
                read_config(config_path, sha, repo); load_inputs(config, repo)
                for name, pin in audit["inputs"].items():
                    authenticate(out / name, pin)
                for name, pin in (("source.raw", config["corpus"]["raw"]),
                                  ("nomination.json", nomination_pin), ("requests.jsonl", nomination_config["requests"])):
                    authenticate(out / name, pin)
                require((out / "nomination.json").stat().st_mode & 0o222 == 0, "nomination not immutable")
                accounting.oracle_invocations += 1
                truth = prior.shared.oracle(out / "source.raw", queries, 1_000_000)
                write_truth(out, truth)
                publish(out / "oracle.json", dict(schema="borsuk-cohere-top32-oracle-v1", passed=True,
                    rows=1_000_000, queries=64, k=100, ground_truth_constructions=1,
                    distance="1-dot of f64-normalized original f32", tie="signed source ordinal ascending",
                    truth_id_space="source ordinal", widening="LEu32 to LEi64, unchanged values",
                    source_raw=config["corpus"]["raw"], nomination=nomination_pin,
                    truth_u32=pointer(out / "truth.u32"), truth_i64=pointer(out / "truth.i64")))
            with accounting.stage("reduce_pinned_nomination"):
                reduction_config = dict(schema="borsuk-semantic-binary-coverage-reduce-config-v1",
                    nomination=nomination_pin, truth=pointer(out / "truth.i64"), truth_id_space="source_ordinal",
                    **{n: nomination_config[n] for n in ("order", "requests", "panel", "protocol")})
                reduction_pin = publish(out / "reduce-config.json", reduction_config)
                coverage_report = coverage.reduce(reduction_config, reduction_pin)
                coverage_status(coverage_report)
                publish(out / "coverage.json", coverage_report)
                inputs = {n: identity(out / n) for n in INPUTS}
                publish(out / "resources.json", accounting.report(True))
                decision = dict(schema="borsuk-cohere-top32-construction-v1", config_sha256=sha, prefix=prefix,
                    status=coverage_report["status"], coverage_only=True, qualification=False,
                    complete_historical_coverage=False, returned_recall_measured=False, cold_http_measured=False,
                    physical_s3_query_gets_measured=False, refs=config["refs"], code_sha256=config["code_sha256"],
                    corpus=config["corpus"], selected_locators_sha256=data["panel"]["selected_sha256"],
                    build_invocations=1, oracle_invocations=1, scorer_invocations=0, inputs=inputs,
                    artifacts={n: identity(out / n) for n in ARTIFACTS})
                publish(out / "decision.json", decision)
            with accounting.stage("conditional_final_seal"):
                read_config(config_path, sha, repo); load_inputs(config, repo)
                receipt = seal(config, out, prefix, (*ARTIFACTS, "decision.json"), accounting)
            publish(out / "final-resources.json", accounting.report(True))
            receipt["final_resources"] = identity(out / "final-resources.json")
            publish(out / "seal-readback.json", receipt)
            return decision
    except BaseException as error:
        cleanup = cleanup_failure(out)
        publish(out / "failure-resources.json", accounting.report(False, cleanup))
        publish(out / "failure.json", dict(schema="borsuk-cohere-top32-failure-v1", config_sha256=sha,
            error_type=type(error).__name__, error=str(error), replacement_allowed=False, cleanup=cleanup,
            build_invocations=accounting.build_invocations, oracle_invocations=accounting.oracle_invocations))
        raise


def replay(config_path, sha, repo, out):
    repo, out = Path(repo).resolve(), Path(out).absolute()
    config = read_config(Path(config_path), sha, repo)
    _, coverage = dependencies(repo, config)
    data, _ = load_inputs(config, repo); _, proof = builder_authority(repo, data)
    decision = json.loads((out / "decision.json").read_bytes())
    require(decision["schema"] == "borsuk-cohere-top32-construction-v1"
            and decision["config_sha256"] == sha and decision["refs"] == config["refs"]
            and decision["code_sha256"] == config["code_sha256"] and decision["corpus"] == config["corpus"]
            and set(decision["artifacts"]) == set(ARTIFACTS) and set(decision["inputs"]) == set(INPUTS)
            and decision["build_invocations"] == decision["oracle_invocations"] == 1
            and decision["scorer_invocations"] == 0 and decision["coverage_only"] is True
            and decision["qualification"] is decision["returned_recall_measured"]
                is decision["cold_http_measured"] is decision["physical_s3_query_gets_measured"] is False,
            "construction identity/scope differs")
    for name, pin in dict(decision["inputs"], **decision["artifacts"]).items():
        require(pin["path"] == name, "artifact name differs"); authenticate(out / name, pin)
    for name, field in (("source.raw", "raw"), ("source-order.u64", "order"),
                        ("source-root.json", "root_manifest"), ("source-sq8.bin", "sq8")):
        authenticate(out / name, config["corpus"][field])
    authenticate(out / "panel.json", FIXED["panel"])
    require((out / "source-qualification.json").read_bytes() == canonical(proof), "archived builder provenance differs")
    nomination_config = json.loads((out / "nominate-config.json").read_bytes())
    for name, relative in (("router_root", "root"), ("membership", "membership"), ("leaves", "leaves")):
        pin = nomination_config[name]
        require(pin["path"] == str(out / "generation/router" / (relative + ".bin")), "generated router path differs")
        authenticate(Path(pin["path"]), pin)
    nomination_seal = json.loads((out / "nomination-seal.json").read_bytes())
    require(set(nomination_seal["artifacts"]) == {"nomination.json"}, "nomination seal roster differs")
    sealed = nomination_seal["artifacts"]["nomination.json"]
    require(sealed["authenticated_readback"] is True
            and sealed["key"] == f"{decision['prefix']}/nomination/sealed/nomination.json"
            and (sealed["bytes"], sealed["sha256"]) ==
                (identity(out / "nomination.json")["bytes"], identity(out / "nomination.json")["sha256"]),
            "nomination seal differs")
    prior.shared.check_outputs(out)
    reduction = json.loads((out / "reduce-config.json").read_bytes())
    require(reduction["nomination"] == pointer(out / "nomination.json")
            and reduction["truth"] == pointer(out / "truth.i64"), "replay reduction pointers differ")
    repeated = coverage.reduce(reduction, pointer(out / "reduce-config.json"))
    coverage_status(repeated)
    require(canonical(repeated) == (out / "coverage.json").read_bytes()
            and repeated["status"] == decision["status"], "offline coverage reduction differs")
    readback = json.loads((out / "seal-readback.json").read_bytes())
    expected = dict(decision["artifacts"], **{"decision.json": identity(out / "decision.json")})
    require(readback["schema"] == "borsuk-semantic-1m-seal-readback-v1"
            and set(readback["artifacts"]) == set(expected), "final seal roster differs")
    for name, pin in expected.items():
        sealed = readback["artifacts"][name]
        require(sealed["authenticated_readback"] is True
                and sealed["key"] == f"{decision['prefix']}/sealed/{name}"
                and (sealed["bytes"], sealed["sha256"]) == (pin["bytes"], pin["sha256"]), "final full-body seal differs")
    authenticate(out / "final-resources.json", readback["final_resources"])
    return dict(passed=True, coverage_only=True, remote_objects_reopened=False,
                ground_truth_reexecuted=False, builder_reexecuted=False)


def self_check():
    """Full phase-order mock plus actual metadata/assurance/resource falsifiers."""
    import copy
    from types import SimpleNamespace
    from unittest.mock import patch
    started = time.monotonic()
    repo = Path(__file__).resolve().parents[1]
    if str(repo / "scripts") not in sys.path:
        sys.path.insert(0, str(repo / "scripts"))
    selector = importlib.import_module("scripts.select_cohere_1m_fresh64")
    def rejected(action):
        try:
            action()
        except (ValueError, AssertionError, KeyError, OSError, subprocess.TimeoutExpired):
            return
        raise AssertionError("invalid input unexpectedly accepted")

    class Array:
        def __init__(self, values, dtype="<f4"):
            self.values, self.dtype = values, dtype
            self.shape = (len(values), len(values[0]))
        def __iter__(self):
            return iter(SimpleNamespace(tolist=lambda row=row: row) for row in self.values)
        def tobytes(self):
            code = {"<f4": "f", "<u4": "I", "<i8": "q"}[self.dtype]
            return b"".join(struct.pack("<" + code * len(row), *row) for row in self.values)
        def astype(self, dtype):
            return Array(self.values, dtype)

    queries = Array([[1., float(i + 2), *([0.] * 766)] for i in range(64)])
    gold = Array([list(range(100)) for _ in range(64)], "<u4")
    def vector_hashes(values):
        raw, normalized = [], []
        for row in values.values:
            raw.append(hashlib.sha256(struct.pack("<768f", *row)).hexdigest())
            norm = math.sqrt(sum(v * v for v in row))
            normalized.append(hashlib.sha256(struct.pack("<768d", *(v / norm for v in row))).hexdigest())
        return raw, normalized
    shared = SimpleNamespace(authenticate=authenticate, vector_hashes=vector_hashes,
        subprocess=subprocess, seal=SimpleNamespace(subprocess=subprocess))
    with tempfile.TemporaryDirectory() as directory, \
            patch.object(prior, "selector", selector, create=True), \
            patch.object(prior, "shared", shared, create=True), \
            patch.object(prior, "np", SimpleNamespace(dtype=lambda name: name), create=True):
        work = Path(directory)
        # Exercise authenticated real METADATA bodies only, never vector/truth bodies.
        a = json.loads(read_ref(repo, FIXED["metadata_authority"]))
        receipt = json.loads(read_ref(repo, FIXED["source_receipt"]))
        proofs = {n: json.loads(read_ref(repo, p)) for n, p in a["proofs"].items()}
        test = next(o for o in receipt["objects"] if o["role"] == "query")
        refs = dict(FIXED, **a["proofs"], **a["input_pins"])
        test_pin = dict(key=test["uri"].split("/", 3)[3], bytes=test["bytes"], sha256=test["sha256"], rows=1000)
        quality = json.loads(read_ref(repo, FIXED["quality_config"]))
        roster = ("scripts/prepare_cohere_top32_coverage.py", "scripts/prepare_cohere_semantic_1m_panel.py")
        config = dict(EXPECTED, code_sha256={n: identity(repo / n)["sha256"] for n in roster}, refs=refs,
            corpus=a["corpus"], consumed_queries=a["old_consumed_panel"]["artifacts"]["queries.raw"],
            registered_test=test_pin, prior_unit_sha256=prior.historical_pins(proofs), builder=quality["binaries"]["builder"])
        config_path = work / "config.json"; publish(config_path, config)
        config_sha = identity(config_path)["sha256"]
        with patch(__name__ + ".code_roster", return_value=roster):
            assert read_config(config_path, config_sha, repo) == config
            rejected(lambda: read_config(config_path, "0" * 64, repo))
            changed = copy.deepcopy(config); changed["limits"]["memory_bytes"] *= 2
            bad_config = work / "bad-config.json"; publish(bad_config, changed)
            rejected(lambda: read_config(bad_config, identity(bad_config)["sha256"], repo))
        data, _ = load_inputs(config, repo)
        assert len(data["panel"]["selected"]) == 64
        for name, field, value in (("panel", "selected_sha256", "0" * 64),
                ("root_metadata_freeze", "root_pending", True), ("metadata_authority", "ground_truth_constructed", True)):
            changed = copy.deepcopy(data); changed[name][field] = value
            rejected(lambda: validate_metadata(changed))
        changed = copy.deepcopy(data); changed["panel"]["selected"][0]["local_row"] += 1
        rejected(lambda: validate_metadata(changed))
        binary, proof = builder_authority(repo, data)
        assert hashlib.sha256(binary).hexdigest() == BUILDER_SHA
        for field, value in (("source_identity_sha256", "0" * 64), ("source_file_count", 398)):
            changed = copy.deepcopy(data); changed["quality_source"][field] = value
            rejected(lambda: builder_authority(repo, changed))
        changed = copy.deepcopy(data); changed["quality_config"]["binaries"]["builder"]["sha256"] = "0" * 64
        rejected(lambda: builder_authority(repo, changed))
        for hits, status in ((607, "FAIL"), (608, "GO-for-native-investigation")):
            assert coverage_status(dict(status=status, summary={POLICIES[1]: dict(page_closure_hits10=hits)})) == status
        rejected(lambda: coverage_status(dict(status="GO-for-native-investigation", summary={POLICIES[1]: dict(page_closure_hits10=607)})))
        with patch.object(prior, "audit", return_value=dict(passed=True)):
            assert duplicate_audit(queries, data, work, None)["previous64_rows_audited"] == 64
            for field, value in zip(("raw_sha256", "normalized_sha256"), vector_hashes(queries)):
                changed = copy.deepcopy(data)
                changed["metadata_authority"]["prior64_vector_hashes"][field][0] = value[0]
                rejected(lambda: duplicate_audit(queries, changed, work, None))

        group = work / "cgroup"; group.mkdir()
        for n, value in {"memory.max": str(4 << 30), "memory.peak": "1234", "memory.swap.max": "0",
                "memory.swap.peak": "0", "cpu.max": "200000 100000", "cpu.stat": "usage_usec 10",
                "memory.events": "max 0\noom 0\noom_kill 0"}.items():
            (group / n).write_text(value)
        prior.cgroup_limits(group, LIMITS)
        rejected(lambda: prior.cgroup_limits(group))
        (group / "memory.max").write_text(str(2 << 30)); prior.cgroup_limits(group)
        (group / "memory.max").write_text(str(4 << 30))
        old_account = prior.Accounting(config, work, None)
        assert old_account.limits == dict(memory_bytes=2 << 30, scratch_bytes=8 << 30,
            cpu=2, threads=2, swap_bytes=0, max_requests=128)
        account = Accounting(config, work, group)
        account.checkpoint()
        assert account.limits == LIMITS and prior.LIMITS == old_account.limits
        for name, value in (("memory.swap.max", "1"), ("cpu.max", "300000 100000"),
                            ("memory.swap.peak", "1"), ("memory.peak", str(5 << 30)),
                            ("memory.events", "max 0\noom 1\noom_kill 0")):
            previous = (group / name).read_text(); (group / name).write_text(value)
            rejected(lambda: account.checkpoint()); (group / name).write_text(previous)
        rejected(lambda: account.checkpoint(9 << 30))
        with patch.object(prior.resource, "getrusage", return_value=SimpleNamespace(ru_maxrss=(5 << 30) // 1024)):
            rejected(lambda: account.checkpoint())
        fast = Accounting(dict(config, stage_limit_seconds=.01), work, group)
        def timed_out():
            with fast.stage("timeout"):
                time.sleep(.025)
        rejected(timed_out)
        assert fast.deadline is None
        with account.stage("valid"):
            assert account.remaining() > 0
        assert "valid" in account.stages

        # The synthetic runtime owns small bodies; production metadata is never relabelled as synthetic evidence.
        root_body = canonical(dict(schema="borsuk-two-bit-generation-v4", canonical=dict(rows=1_000_000, dimensions=768),
            sq8_object_sha256=hashlib.sha256(b"sq8").hexdigest(), sq8_object_key="synthetic/sq8",
            generation=1, base_epoch=0, low=[0.] * 768, step=[1.] * 768))
        bodies = {"synthetic/raw": b"raw", "synthetic/order": b"order", "synthetic/root_manifest": root_body,
                  "synthetic/sq8": b"sq8", "synthetic/consumed": b"consumed"}
        def small_pin(key):
            body = bodies[key]
            return dict(key=key, bytes=len(body), sha256=hashlib.sha256(body).hexdigest())
        synthetic = dict(config, corpus={n: small_pin("synthetic/" + n) for n in ("raw", "order", "root_manifest", "sq8")},
                         consumed_queries=small_pin("synthetic/consumed"))
        events, mode = [], [None]
        def fetch(bucket, pin, path):
            body = bodies[pin["key"]]
            Path(path).write_bytes(body + b"wrong" if mode[0] == "hash" else body)
        shared.seal.checked_s3 = fetch
        def acquire(config, data, out, accounting):
            events.append("extract")
            publish(out / "prior-queries.raw", b"prior"); publish(out / "test-queries.raw", b"test")
            return queries, []
        def build(args, log, seconds, timing):
            events.append("build")
            assert not (Path(log).parent / "truth.i64").exists() and seconds <= 1800
            out = Path(log).parent; generation = Path(args[-1]); (generation / "router").mkdir(parents=True)
            builder = json.loads((out / "builder-config.json").read_bytes())
            assert builder["discovery"] == "semantic" and builder["semantic_profile"] == "fresh1m"
            d = dict(mode="semantic", profile="fresh1m", source_sha256=synthetic["corpus"]["raw"]["sha256"],
                source_order_sha256=synthetic["corpus"]["order"]["sha256"], sq8_sha256=synthetic["corpus"]["sq8"]["sha256"],
                input_schema="borsuk-two-bit-plane-v3", input_root_sha256="1" * 64, centroids_sha256="2" * 64)
            for name in ("root", "membership", "leaves"):
                p = generation / "router" / (name + ".bin"); publish(p, name.encode())
                d[name + "_bytes"], d[name + "_sha256"] = identity(p)["bytes"], identity(p)["sha256"]
            manifest = dict(schema="borsuk-two-bit-generation-v8", generation=1, base_epoch=0, discovery=d,
                sq8_object_sha256=d["sq8_sha256"])
            pin = publish(generation / "manifest.json", manifest)
            Path(log).write_text(pin["sha256"] + "\n"); Path(timing).write_text("synthetic timing\n")
            if mode[0] == "timeout":
                raise subprocess.TimeoutExpired(args, seconds)
            return dict(exit_status=1 if mode[0] == "build" else 0, process_cleanup=mode[0] != "cleanup",
                        wall_seconds=1, process_peak_rss_kib=(5 << 20) if mode[0] == "rss" else 100)
        native = SimpleNamespace(run_process=build, PAYLOAD=512 << 20,
            ordinal_check=lambda *args: dict(id_matches_order=True), local_head=lambda p: dict(etag='"synthetic"'))
        def nominate(config, pin):
            events.append("nominate")
            assert "truth" not in config and not (Path(config["requests"]["path"]).parent / "truth.i64").exists()
            records = [dict(query_ordinal=i, source_ordinal=r["source_ordinal"], policies={p: {} for p in POLICIES})
                       for i, r in enumerate(data["panel"]["selected"])]
            if mode[0] == "nomination":
                records.pop()
            if mode[0] == "input-drift":
                (Path(config["requests"]["path"]).parent / "source.raw").write_bytes(b"drift")
            return dict(schema="borsuk-semantic-binary-coverage-nomination-v1", phase="nomination_frozen_before_truth",
                truth_opened=False, count=64, policies=list(POLICIES), records=records,
                panel_selected_sha256=data["panel"]["selected_sha256"])
        def oracle(*unused):
            events.append("oracle")
            assert events[-2] == "nomination-seal"
            return gold
        def reduce(config, pin):
            events.append("reduce")
            authenticate(Path(config["nomination"]["path"]), config["nomination"])
            assert Path(config["nomination"]["path"]).stat().st_mode & 0o222 == 0
            return dict(status="GO-for-native-investigation", summary={POLICIES[1]: dict(page_closure_hits10=608)}, coverage_only=True)
        def sealed(bucket, prefix, out, expected):
            events.append("nomination-seal" if prefix.endswith("/nomination") else "final-seal")
            if prefix.endswith("/nomination"):
                assert "oracle" not in events and list(expected) == ["nomination.json"]
                if mode[0] == "seal":
                    raise ValueError("conditional seal refusal")
                if mode[0] == "writable":
                    (out / "nomination.json").chmod(0o644)
            return dict(schema="borsuk-semantic-1m-seal-readback-v1", artifacts={n: dict(p,
                key=f"{prefix}/sealed/{n}", authenticated_readback=True) for n, p in expected.items()})
        shared.seal_readback, shared.oracle = sealed, oracle
        shared.check_outputs = lambda out: require((out / "truth.u32").read_bytes() == gold.tobytes()
            and (out / "truth.i64").read_bytes() == gold.astype("<i8").tobytes(), "truth widening")
        coverage = SimpleNamespace(nominate=nominate, reduce=reduce)
        with patch(__name__ + ".read_config", return_value=synthetic), \
                patch(__name__ + ".dependencies", return_value=(native, coverage)), \
                patch(__name__ + ".load_inputs", return_value=(data, {})), \
                patch(__name__ + ".current_group", return_value=group), \
                patch.object(prior, "acquire_vectors", side_effect=acquire), \
                patch.object(prior, "verify_order"), patch.object(prior, "audit", return_value=dict(passed=True)):
            output = work / "output"
            result = run(config_path, config_sha, repo, output, "synthetic/top32")
            assert events == ["extract", "build", "nominate", "nomination-seal", "oracle", "reduce", "final-seal"]
            assert result["build_invocations"] == result["oracle_invocations"] == 1
            assert (output / "queries.raw").read_bytes() == queries.tobytes()
            assert [json.loads(line)["ordinal"] for line in (output / "requests.jsonl").read_bytes().splitlines()] == list(range(64))
            before = events.count("oracle")
            assert replay(config_path, config_sha, repo, output)["ground_truth_reexecuted"] is False
            assert events.count("oracle") == before and events.count("build") == 1
            rejected(lambda: run(config_path, config_sha, repo, output, "synthetic/top32"))
            rejected(lambda: publish(output / "nomination.json", {}))
            for failure in ("hash", "build", "rss", "timeout", "cleanup", "nomination", "seal", "input-drift", "writable"):
                mode[0] = failure; events.clear(); failed = work / ("fail-" + failure)
                rejected(lambda: run(config_path, config_sha, repo, failed, "synthetic/" + failure))
                assert (failed / "failure.json").exists() and not (failed / "decision.json").exists()
                assert not any((failed / n).exists() for n in (*INPUTS, "generation", "builder", "shard-scratch"))
                assert "oracle" not in events and events.count("build") <= 1
    require(time.monotonic() - started <= 55 and resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * 1024 <= 200 << 20,
            "self-check resource envelope exceeded")
    print("self-check PASS: phase order, one builder/oracle, offline replay, authentication, exclusions, resources, cleanup, no overwrite")


def main():
    args = sys.argv[1:]
    if args == ["--self-check"]:
        self_check()
    elif len(args) == 5 and args[0] == "--replay":
        print(json.dumps(replay(*args[1:])))
    elif len(args) == 5:
        print(json.dumps(run(*args)))
    else:
        raise SystemExit("usage: CONFIG SHA REPO NEW_OUTPUT PREFIX | --replay CONFIG SHA REPO OUTPUT | --self-check")


if __name__ == "__main__":
    main()
